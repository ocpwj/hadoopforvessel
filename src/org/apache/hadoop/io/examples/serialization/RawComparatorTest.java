package org.apache.hadoop.io.examples.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IntWritable;

public class RawComparatorTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);
		
		IntWritable w1 = new IntWritable(163);
		IntWritable w2 = new IntWritable(67);
		
		//Compare after deserialization
		System.out.println(comparator.compare(w1, w2));

		byte[] b1 = serialize(w1);
		byte[] b2 = serialize(w2);
		
		//Raw compare
		System.out.println(comparator.compare(b1, 0, b1.length, b2, 0, b2.length));
	}

	public static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}
}
