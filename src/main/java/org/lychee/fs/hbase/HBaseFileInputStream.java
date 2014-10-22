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
    
    private byte[] cache;
    private int cursor;
    
    public HBaseFileInputStream(HBaseFile hbFile) {
        this.hbFile = hbFile;
    }

    @Override
    public int read() throws IOException {
        if (cache == null) {
            cache = readCacheFromHBase();
            cursor = 0;
            // 读不到分片了，返回-1,上层while循环判断文件读取完毕-1
            if (cache == null || cache.length == 0) return -1;
        }
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
    
    
    private byte[] readCacheFromHBase() throws IOException {
        return HBaseFileHelper.readShard(hbFile, shard++);
    }
    
}
