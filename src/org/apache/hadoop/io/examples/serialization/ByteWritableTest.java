package org.apache.hadoop.io.examples.serialization;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class ByteWritableTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		BytesWritable b = new BytesWritable(new byte[] { 3, 5 });
		byte[] bytes = serialize(b);
		System.out.println(StringUtils.byteToHexString(bytes));
		
		b.setCapacity(11);
		System.out.println(b.getLength());
		System.out.println(b.getBytes().length);
	}
	
	public static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}

}
