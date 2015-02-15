package org.apache.hadoop.io.examples.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class StreamCompressor {

	/**
	 * @param args
	 */
	
	//echo "Text" | hadoop org.apache.hadoop.io.examples.compression.StreamCompressor org.apache.hadoop.io.compress.GzipCodec | gunzip
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		String codecClassname = args[0];
		Class<?> codecClass = Class.forName(codecClassname);
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
		CompressionOutputStream out = codec.createOutputStream(System.out);
		IOUtils.copyBytes(System.in, out, 4096, false);
		out.finish();

	}

}
