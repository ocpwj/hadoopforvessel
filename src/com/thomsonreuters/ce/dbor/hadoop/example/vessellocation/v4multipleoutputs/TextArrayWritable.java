package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v4multipleoutputs;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
public class TextArrayWritable extends ArrayWritable {

	public TextArrayWritable() {
		super(Text.class);
		// TODO Auto-generated constructor stub
	}

	public TextArrayWritable(Text[] arg0) {
		super(Text.class);
		super.set(arg0);
		// TODO Auto-generated constructor stub
	}
}
