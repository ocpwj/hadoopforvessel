package org.apache.hadoop.hdfs.examples.sync;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MakeFileVisiableBySync {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String uri="hdfs://10.52.137.201:9000/user/jing.wang/test.txt";
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path p = new Path(uri);
			FSDataOutputStream out = fs.create(p);
			out.write("content".getBytes("UTF-8"));
			out.flush();
			System.out.println(fs.getFileStatus(p).getLen()); //Should be zero
			out.sync(); //I dont know why it doesn't work
			System.out.println(fs.getFileStatus(p).getLen());
			out.close();
			System.out.println(fs.getFileStatus(p).getLen());
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
