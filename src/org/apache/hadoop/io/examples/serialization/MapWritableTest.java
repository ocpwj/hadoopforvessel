package org.apache.hadoop.io.examples.serialization;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableUtils;

public class MapWritableTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		MapWritable src = new MapWritable();
		src.put(new IntWritable(1), new Text("cat"));
		src.put(new VIntWritable(2), new LongWritable(163));
		MapWritable dest = new MapWritable();
		WritableUtils.cloneInto(dest, src);
		System.out.println((Text) dest.get(new IntWritable(1)));
		System.out.println((LongWritable) dest.get(new VIntWritable(2)));
	}

}
