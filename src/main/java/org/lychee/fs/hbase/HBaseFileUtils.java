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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * A utils for user to use the Hbase file system.
 * 
 * @author chunhui
 */
public class HBaseFileUtils {

	private static final Logger log = LoggerFactory
			.getLogger(HBaseFileUtils.class);

	/**
	 * 
	 * upload local file to the hbase file system.
	 * 
	 * @param localFile
	 * @return the identifier of the file in the hbase file system.
	 * @throws IOException
	 */
	public static String upload(File localFile) throws IOException {
		String md5 = md5Hex(localFile);
		try (InputStream is = new FileInputStream(localFile)) {
			HBaseFile hbFile = HBaseFile.Factory.buildHBaseFile(md5,
					localFile.getName());
			// 如果文件已经完整存在HBase中，是否完整作为标记符保持在HBase的列簇中一列
			if (hbFile.integrity()) {
				// do nothing. the file is already store in hbase cluster.
				log.debug("文件MD5是 " + md5 + ", 已经存在，无需上传");
			} else {
				// 如果不完整，先删除
				hbFile.delete();
				// 再上传
				hbFile = HBaseFile.Factory.buildHBaseFile(md5,
						localFile.getName());
				// 上传实体文件流
				try (OutputStream ops = new HBaseFileOutputStream(hbFile)) {
					IOUtils.copy(is, ops);
				}
			}
		}
		return md5;
	}

	/**
	 * 
	 * upload local files to the hbase file system.
	 * MultiThread Batch Upload
	 * 
	 * @param localFiles
	 * @return the identifier of the file in the hbase file system.
	 * @throws IOException
	 */
	public static List<Future<String>> upload(Collection<File> localFiles) {
		//开启过多线程可能引起性能问题，这里先设置最多5个线程
		ExecutorService executorService = Executors.newFixedThreadPool(5);
		List<Future<String>> md5List=new ArrayList<Future<String>>();

		for (File thisFile : localFiles) {
		     md5List.add(executorService.submit(new UploadRunnable(thisFile)));
		}

		return md5List;
	}

	/**
	 * 
	 * download the file in the hbase file system to the local file. the file
	 * must dose exist and integrity.
	 * 
	 * @param identifier
	 * @param localFile
	 * @throws IOException
	 */
	public static void download(String identifier, File localFile)
			throws IOException {
		HBaseFile hbFile = HBaseFile.Factory.buildHBaseFile(identifier);
		if (!hbFile.integrity()) {
			throw new IOException(
					"Fail to read the file in the hbase file system.");
		}
		try (InputStream is = new HBaseFileInputStream(hbFile)) {
			FileUtils.deleteQuietly(localFile);
			try (OutputStream os = new FileOutputStream(localFile)) {
				// 将读入内存的is写到os，实现文件流从内存写入磁盘
				IOUtils.copy(is, os);
			}
		}
	}

	public static String md5Hex(File file) throws IOException {
		String md5;
		try (InputStream is = new FileInputStream(file)) {
			md5 = DigestUtils.md5Hex(is);
		}
		return md5;
	}
}

/*
 * 多线程上传FileList
 */
class UploadRunnable implements Callable<String> {
	private static final Logger log = LoggerFactory.getLogger(UploadRunnable.class);

	private File thisUploadFile;
	private String thisUploadFileMD5;

	public UploadRunnable(File thisUploadFile) {
		super();
		this.thisUploadFile = thisUploadFile;
	}

	public String call() {
		try {
			thisUploadFileMD5 = HBaseFileUtils.upload(thisUploadFile);
		} catch (IOException e) {
			log.error("多线程上传出错:" + thisUploadFile.getAbsolutePath());
			e.printStackTrace();
		}
		return thisUploadFileMD5;
	}
}
