/*
 * Copyright 2014 chunhui.
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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
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
 * @author chunhui
 * @see HBaseFile
 * @see HBaseFileOutputStream
 */
public class HBaseFileInputStream extends InputStream {
    
    private final HBaseFile hbFile;
    private int shard = 1;
    
    private List<byte[]> continuousCache;
    private byte[] cache;
    private int cursor;
    
    public HBaseFileInputStream(HBaseFile hbFile) {
        this.hbFile = hbFile;
        toReadAllCache();
    }

    @Override
    public int read() throws IOException {
    	//缓冲区没有数据了
        if (cache == null) {
        	if(continuousCache!=null&&continuousCache.size()>0){
        		int shardIndex=shard++;
        		if(shardIndex<=continuousCache.size()){
        		   cache=continuousCache.get(shardIndex-1);
        		}
         	}else{
               cache = readCacheFromHBase();
         	}
        	// 读不到分片了，返回-1,上层while循环判断文件读取完毕-1
            if (cache == null || cache.length == 0) return -1;
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
    
    
    /*
     * 第一次读取的时候，另外起一个线程预读取整个文件流
     */
    private void toReadAllCache() {
    	ExecutorService executorService = Executors.newFixedThreadPool(2);
		try{
			this.continuousCache=executorService.submit(new ReadCacheRunnable(hbFile)).get();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

    //每次读完一个缓冲区之后，再读满一个缓冲区
	private byte[] readCacheFromHBase() throws IOException {
    	return HBaseFileHelper.readShard(hbFile, shard++);
    }
    
}

/*
 * 新线程读取文件流
 */
class ReadCacheRunnable implements Callable<List<byte[]>> {
	private static final Logger log = LoggerFactory.getLogger(ReadCacheRunnable.class);

	private final HBaseFile hbFile;
    private int shardIndex = 1;
    
    private List<byte[]> thisShardByteList;
    private byte[] thisShardByte;

	public ReadCacheRunnable(HBaseFile thisFile) {
		super();
		this.hbFile = thisFile;
		thisShardByteList=new ArrayList<byte[]>();
	}

	public List<byte[]> call() {
		try {
			do{
				thisShardByte=HBaseFileHelper.readShard(hbFile, shardIndex++);
				if(thisShardByte!=null&&thisShardByte.length > 0){
				   thisShardByteList.add(thisShardByte);
				}
			}
			while(thisShardByte == null || thisShardByte.length == 0);
			
		} catch (IOException e) {
			log.error(e.toString());
			e.printStackTrace();
		}
		
		return thisShardByteList;
	}
}
