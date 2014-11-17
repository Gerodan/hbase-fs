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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
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
public class HBaseFileOperateTest {
    private static final Logger log = LoggerFactory.getLogger(HBaseFileOperateTest.class);
    
    private final String uploadPath = "/home/gerodan/SCWork/HBaseFS/code/hbase-fs/pic";
    private final String outPath = uploadPath + File.separator + "out" + File.separator;
    
    public HBaseFileOperateTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() throws IOException {
        FileUtils.deleteDirectory(new File(outPath));
        FileUtils.forceMkdir(new File(outPath));
    	log.info("清空并重建"+outPath+"");
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void uploadAndDownloadHBaseFiles() throws IOException {
        final File uploadFolder = new File(uploadPath);
        if (!uploadFolder.isDirectory()) {
            fail("请设置待上传路径");
        	log.info("请设置待上传路径");
        }
        //获得待上传路径下所有文件
        Collection<File> uploadFiles = FileUtils.listFiles(uploadFolder, null, false);
        if (uploadFiles == null || uploadFiles.isEmpty()) {
            fail("该路径下没有文件");
        	log.info("该路径下没有文件");
        }
        
        log.info("将要上传"+uploadFiles.size()+"个文件");
        
        for (File uploadFile : uploadFiles) {
            if (uploadFile.isFile())
            	uploadAndDownloadOneHBaseFile(uploadFile);
        }
    }
    
    @Test
    public void downloadOneHBaseFile() throws IOException {
    	String identifier="d22616317c72bc47e1d7b14ac6d190f1";
    	String downloadPath=outPath +"another.jpg";
    	
    	//HBase下载到本地路径
    	File outFile = new File(downloadPath);
    	HBaseFileUtils.download(identifier, outFile);
    	log.info("下载文件到"+downloadPath);
    }
    
    @Test
    //文件夹数个文件测试，验证上传之前和下载之后的MD5是否一致
    public void testAllUpDownload4EachFile() throws IOException {
    	final File uploadFolder = new File(uploadPath);
    	Collection<File> uploadFiles = FileUtils.listFiles(uploadFolder, null, false);
    	for(File thisFile:uploadFiles){
    		//文件原MD5
    		String originalMD5=HBaseFileUtils.md5Hex(thisFile);
    		//一个个上传，一个个下载，一个个验证
    		uploadAndDownloadOneHBaseFile(thisFile);
    		File outFile = new File(outPath + thisFile.getName());
    		//下载文件现在的MD5
    		String downloadedFileMD5=HBaseFileUtils.md5Hex(outFile);
    		assertTrue(originalMD5.equals(downloadedFileMD5));
    	}
    	
    	log.info("文件夹数个文件测试流程结束");
    }
    
    @Test
    //文件夹批量测试，验证上传之前和下载之后的MD5是否一致
    public void testAllUpDownload4BatchFile() throws IOException {
    	final File uploadFolder = new File(uploadPath);
        Collection<File> uploadFiles = FileUtils.listFiles(uploadFolder, null, false);
        //多线程批量上传
        List<Future<String>> md5List=HBaseFileUtils.upload(uploadFiles);
        
        //批量上传，一个个下载，一个个验证
        for(Future<String> thisOriginalMD5:md5List){
        	try {
				HBaseFile thisFile=HBaseFile.Factory.buildHBaseFile(thisOriginalMD5.get());
				String downloadPath=outPath +thisFile.getDesc();
		    	//HBase下载到本地路径
		    	File outFile = new File(downloadPath);
		    	HBaseFileUtils.download(thisFile.getIdentifier(), outFile);	
		    	
	    		//下载文件现在MD5
		    	String downloadedFileMD5=HBaseFileUtils.md5Hex(outFile);
		    	System.out.println(downloadedFileMD5+" VS "+thisOriginalMD5.get());
	    		assertTrue(thisOriginalMD5.get().equals(downloadedFileMD5));
		    	} 
        	catch (Exception e) {
				e.printStackTrace();
			}
        }
       
    	log.info("文件夹批量测试流程结束");
    }
    
    @Test
    //多线程上传FileList测试
    public void uploadHBaseFiles() throws IOException {
    	 final File uploadFolder = new File(uploadPath);
         if (!uploadFolder.isDirectory()) {
             fail("请设置待上传路径");
         	 log.info("请设置待上传路径");
         }
         //获得待上传路径下所有文件
         Collection<File> uploadFiles = FileUtils.listFiles(uploadFolder, null, false);
         if (uploadFiles == null || uploadFiles.isEmpty()) {
             fail("该路径下没有文件");
         	 log.info("该路径下没有文件");
         }
         
         log.info("将要上传"+uploadFiles.size()+"个文件");
         List<Future<String>> md5List=HBaseFileUtils.upload(uploadFiles);
         for(Future<String> thisMD5:md5List){
        	 try {
				log.info("上传的文件"+thisMD5.get());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
         }
    }
    
    private void uploadAndDownloadOneHBaseFile(File localFile) throws IOException {
    	//本地文件上传到HBase
        String identifier = HBaseFileUtils.upload(localFile);
        log.info("文件上传完成，下面开始下载");
    	//HBase下载到本地路径
        File outFile = new File(outPath + localFile.getName());
        HBaseFileUtils.download(identifier, outFile);
    }
    
    private void testDel(HBaseFile hbFile) throws IOException {
        if (hbFile.exists())
            hbFile.delete();
    }
    
}
