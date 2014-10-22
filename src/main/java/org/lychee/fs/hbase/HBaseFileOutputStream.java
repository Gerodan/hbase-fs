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
import java.util.Arrays;

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

    private final HBaseFile hbFile;
    
    private final static int CACHE_SIZE = 1024 * 1024;
    
    private byte[] cache;
    
    private byte[] needFlushShard;
    
    private int cursor = 0;
    private long size = 0;
    
    public HBaseFileOutputStream(HBaseFile hbFile) {
        this.hbFile = hbFile;
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
            cache = null;
            writeCacheToHBase();
        }
    }
    
    @Override
    public void flush() throws IOException {
        flush0();
    }
    
    @Override
    public void close() throws IOException {
        flush0();
    }
    
    private void writeCacheToHBase() throws IOException {
        if (hbFile.isNew()) {
            hbFile.setStatus(HBaseFileConst.TRANSIT);
            HBaseFileHelper.saveOrUpdateMeta(hbFile);
        }
        hbFile.setSize(size);
        //分片计数
        hbFile.setShards(hbFile.getShards() + 1);
        //将文件分片实体入库
        HBaseFileHelper.addShard(hbFile, needFlushShard);
    }
    
    private void flush0() throws IOException {
        needFlushShard = Arrays.copyOf(cache, cursor);
        writeCacheToHBase();
        if (!hbFile.integrity()) {
            hbFile.setStatus(HBaseFileConst.INTEGRITY);
            HBaseFileHelper.saveOrUpdateMeta(hbFile);
        }
    }
    
}
