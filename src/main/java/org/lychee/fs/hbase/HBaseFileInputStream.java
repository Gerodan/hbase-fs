/*
 * Copyright 2014 gerodan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.lychee.fs.hbase;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * the InputStream impl of a HBase file. <br/>
 * 
 * Read a hbase file.like a common inputstream.
 * 
 * @author gerodan
 * @see HBaseFile
 * @see HBaseFileOutputStream
 */
public class HBaseFileInputStream extends InputStream {
	private static final Logger log = LoggerFactory.getLogger(HBaseFileInputStream.class);
    
    private final HBaseFile hbFile;
    //缓冲区的已读游标
    private int cacheReadedIndex = 1;
    //文件已读游标
    private int shardsCursor = 0;
    //文件总分块数
    private int fileTotalShardsNum = 1;
    
    private List<byte[]> continuousCache;
    private byte[] cache;
    private int cursor;
    private boolean isFirst;
    
    //预读取数据块数目（可配置）
    private int preReadNum=2;
    //触发下次缓冲区去读取文件的间隔阈值（可配置）
    private int approachingThreshold=2;
    
    public HBaseFileInputStream(HBaseFile hbFile) {
    	this.hbFile = hbFile;
    	this.isFirst=true;
    	this.fileTotalShardsNum=hbFile.getShards();
    }
    
    /**
     * @author gerodan
     * @param hbFile(build via HBaseFile)
     * @param preReadNum(the number of shards to pre read)
     * @param toReadThreshold(the threshold of a interval which is between readed shards and cached shards)
     * @see HBaseFile
     */
    public HBaseFileInputStream(HBaseFile hbFile,int preReadNum,int toReadThreshold) {
        this.hbFile = hbFile;
        this.isFirst=true;
        this.fileTotalShardsNum=hbFile.getShards();
        this.preReadNum=preReadNum;
        this.approachingThreshold=toReadThreshold;
    }

    @Override
    public int read(){
    	//第一次读取初始化变量
    	if(isFirst){
    	   initRead();
    	   isFirst=false;
    	}
    	
    	//缓冲区没有数据
        if (cache == null) {
        	cursor = 0;
        	
        	//每次多线程读取N个数据块
        	try {
        		if(isApproaching()){
        		   toPreReadShard();
        	}
				
			} catch (InterruptedException e) {
				log.error(e.toString());
			} catch (ExecutionException e) {
				log.error(e.toString());
			}
        	if(shardsCursor<this.fileTotalShardsNum){
        	   cache=this.continuousCache.get(shardsCursor++);
        	}

        	//读不到分片了，返回-1,上层while循环判断文件读取完毕-1
            if (cache == null || cache.length == 0) return -1;
       
            //读到文件之后，把已经读取过的文件流置为空，释放内存资源
        	volatileTheReadedCache();
        }
        
        //每次从缓冲区取一个字节
        byte b = -1;
        if (cursor < cache.length) {
            b = cache[cursor++];
        }
        if (cursor >= cache.length) {
            cache = null;
        }
        
        //byte转换int时与0xff进行与运算是为了让int的高24位清0
        return b & 0xff;
    }
    
    /**
     * 已经读取的数据块是否在逼近缓冲区的数据块
     */
    private boolean isApproaching() {
		return cacheReadedIndex-shardsCursor<approachingThreshold;
	}

	/**
     * 将读取过的文件流置为空以释放内存
     */
    private void volatileTheReadedCache() {
    	this.continuousCache.set(shardsCursor-1, null);		
	}

    /**
     * 每次预读取Ｎ个数据流块
     */
    private void toPreReadShard() throws InterruptedException, ExecutionException  {
    	ExecutorService executorService = Executors.newFixedThreadPool(getThreadNum(preReadNum));
		HashMap<Integer,Future<byte[]>> futureMap=new HashMap<Integer,Future<byte[]>>();
		
		//分发任务
		for(int shardEnd=cacheReadedIndex+preReadNum;cacheReadedIndex<=fileTotalShardsNum&&cacheReadedIndex<shardEnd;cacheReadedIndex++){
			futureMap.put(cacheReadedIndex,executorService.submit(new ReadCacheRunnable(hbFile,cacheReadedIndex)));
		}
		
		Iterator<Entry<Integer, Future<byte[]>>> iter = futureMap.entrySet().iterator(); 
		while (iter.hasNext()) { 
		    Map.Entry<Integer, Future<byte[]>> entry = (Map.Entry<Integer, Future<byte[]>>) iter.next(); 
		    int index = entry.getKey(); 
		    if(entry.getValue()!=null){
		       byte[] thisShardContents = entry.getValue().get(); 
		       this.continuousCache.set(index-1, thisShardContents);
		    }
		} 
	}

    /**
     * 根据＂预读取数＂确定线程数
     */
    private int getThreadNum(int preReadNum) {
    	return preReadNum>=5?5:preReadNum;
    }
    
    /**
     * 第一次进入read()初始化参数
     */
    private void initRead() {
    	this.continuousCache=new ArrayList<byte[]>();
    	for(int i=0;i<hbFile.getShards();i++){
    		this.continuousCache.add(i, null);
    	}
	}
	
    /**
     * 新线程读取文件流
     */
	private class ReadCacheRunnable implements Callable<byte[]> {
		private final HBaseFile hbFile;
	    private int shardIndex;
	    
	    private byte[] thisShardByte;

		public ReadCacheRunnable(HBaseFile thisFile,int shardIndex) {
			super();
			this.hbFile = thisFile;
			this.shardIndex = shardIndex;
		}

		public byte[] call() throws Exception{
			thisShardByte=HBaseFileHelper.readShard(hbFile, shardIndex);
			return thisShardByte;
		}
	}

}

