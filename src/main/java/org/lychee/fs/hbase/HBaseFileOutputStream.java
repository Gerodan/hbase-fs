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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * The OutputStream impl of the Hbase file. <br/>
 * 
 * You can use it like common OutputStream, just remeber to **close** it.
 * 
 * @author chunhui
 * @see HBaseFile
 * @see HBaseFileInputStream
 */
public class HBaseFileOutputStream extends OutputStream {
	private static final Logger log = LoggerFactory.getLogger(HBaseFileOutputStream.class);

    private final HBaseFile hbFile;
    
    //定义每个文件分块大小为1MB
    private final static int CACHE_SIZE = 1024 * 1024;
    
    private byte[] cache;
    
    private byte[] needFlushShard;
    
    private List<byte[]> needFlushShardList;
    
    private int cachePollNum = 2;
    
    private int cursor = 0;
    
    private long size = 0;
    
    private int startShardCursor=0;

    
    public HBaseFileOutputStream(HBaseFile hbFile) {
        this.hbFile = hbFile;
        this.needFlushShardList= Collections.synchronizedList(new ArrayList<byte[]>());
    }
    
    @Override
    public void write(int b) throws IOException {
        if (cache == null) {
            cache = new byte[CACHE_SIZE];
            cursor = 0;
        }
        //int转换为byte时，强转即可
        cache[cursor++] = (byte)b;
        size++;
        //每写满一次缓存，入库一次
        if (cursor == CACHE_SIZE) {
            needFlushShard = cache;
            needFlushShardList.add(needFlushShard);
            cache = null;
            if(needFlushShardList.size()%cachePollNum==0){
               writeCacheListToHBase();
            }
        }
    }
    
    /**
     * 多线程分发写任务
     */
    private void writeCacheListToHBase() {
    	ExecutorService executorService = Executors.newFixedThreadPool(getThreadNum(cachePollNum));
		ArrayList<Future<Boolean>> futureList=new ArrayList<Future<Boolean>>();
		
    	//分发任务
		for(int endShard=startShardCursor+cachePollNum;(startShardCursor<endShard&&startShardCursor<needFlushShardList.size());startShardCursor++){
		    byte[] thisShardByte=needFlushShardList.get(startShardCursor);
		    futureList.add(executorService.submit(new WriteCacheRunnable(thisShardByte)));
		    log.info(startShardCursor+"-----RUNNING...");
		   /* try {
				Thread.currentThread().sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}*/
		}
		
		//为保证当前线程池里面所有任务执行完成,调用get()保证完成
		for(Future<Boolean> thisFuture:futureList){
			try {
				log.info("每个任务是否完成?:"+thisFuture.get().toString());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
			
		}
		
	}
    
    /**
     * 单线程写任务
     */
    private synchronized void writeCacheToHBase(byte[] thisNeedFlushShard) throws IOException {
        if (hbFile.isNew()) {
            hbFile.setStatus(HBaseFileConst.TRANSIT);
            HBaseFileHelper.saveOrUpdateMeta(hbFile);
        }
        hbFile.setSize(size);
        //分片计数（下标从１开始算）
        hbFile.setShards(hbFile.getShards() + 1);
        //将文件分片实体入库
        HBaseFileHelper.addShard(hbFile, thisNeedFlushShard);
    }
    
    /**
     * 根据＂预留线程池数＂确定线程数
     */
    private int getThreadNum(int cachePollNum) {
    	return cachePollNum>=5?5:cachePollNum;
    }

	@Override
    public void flush() throws IOException {
        flush0();
    }
    
    @Override
    public void close() throws IOException {
        flush0();
    }
    
    
    /**
     * 最后一次调用（不用多线程），将余下的数据流写入HBase
     */
    private void flush0() throws IOException {
    	//1/2写入List中的剩余部分
    	for(;startShardCursor<needFlushShardList.size();startShardCursor++){
    		needFlushShard=needFlushShardList.get(startShardCursor);
    		writeCacheToHBase(needFlushShard);
    	}
    	//2/2写入cache中的剩余部分
        needFlushShard = Arrays.copyOf(cache, cursor);
        writeCacheToHBase(needFlushShard);
        if (!hbFile.integrity()) {
            hbFile.setStatus(HBaseFileConst.INTEGRITY);
            HBaseFileHelper.saveOrUpdateMeta(hbFile);
        }
    }
    
    
    /**
     * 新线程写出文件流
     */
	private class WriteCacheRunnable implements Callable<Boolean> {
		private byte[] thisShardByte;
	    private Boolean isCompleted;

		public WriteCacheRunnable(byte[] thisShardByte) {
			super();this.
			thisShardByte = thisShardByte;
			isCompleted = false;
		}

		@Override
		public Boolean call() throws Exception {
			try {
				needFlushShard=thisShardByte;
				writeCacheToHBase(thisShardByte);
				isCompleted=true;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return isCompleted;
			
		}

	}
}
