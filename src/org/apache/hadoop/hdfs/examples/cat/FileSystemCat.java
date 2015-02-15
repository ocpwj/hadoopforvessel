package org.apache.hadoop.hdfs.examples.cat;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemCat {
	public static void main(String[] args) throws Exception {
		//String uri = args[0];
		String uri = "hdfs://bjzd-labvm148:9000/user/jing.wang/dailyoutput/20140211-r-00001";
		Configuration conf = new Configuration();
		//getLocal可以返回本地文件系统
		//public static LocalFileSystem getLocal(Configuration conf) throws IOException

		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = null;
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
}