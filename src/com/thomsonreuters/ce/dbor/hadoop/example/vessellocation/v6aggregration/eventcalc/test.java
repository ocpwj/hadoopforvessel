package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.eventcalc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class test {

	public static void main(String[] args)
	{
		//Initialize MiniDFSCluster
		Configuration conf = new Configuration();
		String uri = "hdfs://bjzd-labvm148:9000/user/jing.wang/aggregration";
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
			Path file = new Path(uri);
			System.out.print("Path.getName()"+ file.getName());
			FileStatus stat = fs.getFileStatus(file);
			//Print status for file
			System.out.println("Status for file:");
			fileStatus(stat);
			
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


	public static void fileStatus(FileStatus stat) {
		System.out.println("stat.getPath().toUri().getPath()="+stat.getPath().toUri().getPath());
		System.out.println("stat.isDir()="+stat.isDir());
		System.out.println("stat.getLen()="+stat.getLen());
		System.out.println("stat.getModificationTime()="+stat.getModificationTime());
		System.out.println("stat.getReplication()="+stat.getReplication());
		System.out.println("stat.getBlockSize()="+stat.getBlockSize());
		System.out.println("stat.getOwner()="+stat.getOwner());
		System.out.println("stat.getGroup()="+stat.getGroup());
		System.out.println("stat.getPermission().toString()="+stat.getPermission().toString());
	}
	
}
