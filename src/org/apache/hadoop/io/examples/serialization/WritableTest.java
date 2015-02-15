package org.apache.hadoop.io.examples.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class WritableTest {

	public static void main(String[] args) throws Exception
	{
		IntWritable writable = new IntWritable();
		writable.set(163);

		byte[] bytes=serialize(writable);
		String ByteValue=StringUtils.byteToHexString(bytes);

		System.out.println(ByteValue);	
		
		IntWritable newWritable=new IntWritable();
		deserialize(newWritable,bytes);
		
		System.out.println(newWritable);	
	}

	public static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}

	public static void deserialize(Writable writable, byte[] bytes)
			throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
	}

}
