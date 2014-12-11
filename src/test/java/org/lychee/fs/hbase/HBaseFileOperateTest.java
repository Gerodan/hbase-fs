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
import static org.lychee.fs.hbase.HBaseFileConst.*;

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
            log.info(thisFile.getName()+"---文件验证OK!!!!!!!!!!!!!!!");
    	}
    	
    	log.info("文件夹数个文件测试流程结束");
    }
    
    //将文件夹下的文件,全部在HBase上删除
    public void hbaseFilesDel() throws IOException {
    	log.info("测试前先在HBase上删除待上传的文件");
        final File uploadFolder = new File(uploadPath);
        Collection<File> uploadFiles = FileUtils.listFiles(uploadFolder, null, false);
      
        for(File thisFile:uploadFiles){
    		//待删除文件的MD5
	    	String needDeleteFileMD5=HBaseFileUtils.md5Hex(thisFile);
	    	HBaseFile needDeleteFile=HBaseFile.Factory.buildHBaseFile(needDeleteFileMD5);
        
	    	if (needDeleteFile.exists()){
	    		needDeleteFile.delete();
	        	log.info(thisFile.getName()+"在HBase上删除成功");
	    	}
        }
    	log.info("HBase上删除上传路径下的所有文件成功");
    }
    
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
    
    private void uploadAndDownloadOneHBaseFile(File localFile) throws IOException {
    	//本地文件上传到HBase
    	String identifier="";
        Long startUploadTime=System.currentTimeMillis();
        log.info(localFile.getName()+"---文件开始上传");
        String uploadInfo = HBaseFileUtils.upload(localFile);
        Long endUploadTime=System.currentTimeMillis();
        log.info(localFile.getName()+"---上传耗时:"+(endUploadTime-startUploadTime)+"毫秒");
        //文件上传前检测已经存在HBase
        if(uploadInfo.startsWith(file_exists_prefix)){
        	log.info(uploadInfo);
        	identifier=uploadInfo.split(division_symbol)[1];
        }
        else{
        	identifier=uploadInfo;
        }
        log.info(localFile.getName()+"---文件已经存在HBase，下面开始下载");
        log.info(localFile.getName()+"---下载计时开始...");
        Long startTime=System.currentTimeMillis();
    	//HBase下载到本地路径
        File outFile = new File(outPath + localFile.getName());
        HBaseFileUtils.download(identifier, outFile);
        log.info(localFile.getName()+"---文件下载完成");
        Long endTime=System.currentTimeMillis();
        log.info(localFile.getName()+"---下载耗时:"+(endTime-startTime)+"毫秒");
    }
    
    @Test
    //测试先删除Hbase上文件,再上传,再下载
    public void testAllManyTimes() throws Exception {
    	//测试次数
    	int testTimes=3;
    	
    	for(int i=0;i<testTimes;i++){
    		//先全部在Hbase上删除
    		hbaseFilesDel();
    		testAllUpDownload4EachFile();;
        	//testAllUpDownload4BatchFile();
    	}
    
    }
    
}
