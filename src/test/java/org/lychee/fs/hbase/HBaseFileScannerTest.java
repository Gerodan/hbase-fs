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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author chunhui
 */
public class HBaseFileScannerTest {
    
    private static final Logger log = LoggerFactory.getLogger(HBaseFileScannerTest.class);
    
    private static HBaseFileSystem hbfs; 
    
    public HBaseFileScannerTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        hbfs = HBaseFileSystem.instance();
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of instance method, of class HBaseFileSystem.
     */
    @Test
    public void testInstance() {
        System.out.println("instance");
        HBaseFileSystem fs1 = HBaseFileSystem.instance();
        HBaseFileSystem fs2 = HBaseFileSystem.instance();
        assertTrue(fs1 == fs2);
    }

    /**
     * Test of next method, of class HBaseFileSystem.
     */
    @Test
    public void getHBaseFileList() {
    	HBaseFileResultScanAdapter scanAdapter = hbfs.scan();
    	HBaseFile hbFile;
    	log.info("已经存储的HBaseFile列表");
    	Integer count=0;
    	while ((hbFile = scanAdapter.nextOne()) != null) {
    		log.info("文件("+(++count)+")MD5："+hbFile.toString());
    	}
    	log.info("共"+count+"个文件");
    	
    }
    
    @Test
    public void getOneHBaseFile() throws IOException {
    	String rowKey="d22616317c72bc47e1d7b14ac6d190f1";
    	//参数rowkey，如果已经上传，读到文件信息，否则返回一个新HBaseFile
    	HBaseFile hbFile =HBaseFile.Factory.buildHBaseFile(rowKey);
    	if(hbFile!=null){
    	   log.info("文件MD5("+rowKey+")信息："+hbFile.toString());
    	}
    	else{
     	   log.info("文件MD5("+rowKey+")找不到");
    	}

    }

    /**
     * Test of count method, of class HBaseFileSystem.
     */
    @Test
    public void testCount() {
        System.out.println("count");
    }
    
}
