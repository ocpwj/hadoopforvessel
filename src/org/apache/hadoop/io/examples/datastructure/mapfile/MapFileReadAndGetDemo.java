package org.apache.hadoop.io.examples.datastructure.mapfile;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileReadAndGetDemo {
	
	public static void main(String[] args) throws IOException {
		//String uri = args[0];
		String uri = "hdfs://10.52.137.201:9000/user/jing.wang/numbers.map";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		MapFile.Reader reader = null;
		
		try {
			reader = new MapFile.Reader(fs, uri, conf);
			WritableComparable key = (WritableComparable)
					ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable)
					ReflectionUtils.newInstance(reader.getValueClass(), conf);
			
			//Iterating through the entries in order
			while (reader.next(key, value)) {
				System.out.println(key.toString()+" "+value.toString());
			}
			
			//A random access lookup
			reader.get(new IntWritable(496), value);
			System.out.println(value.toString());
			
			
		} finally {
			IOUtils.closeStream(reader);
		}
	}

}
