package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.preparation;

import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class Text2DArrayWritable extends TwoDArrayWritable {

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		Writable[][] allfields=super.get();
		
		String all="\r\nLocations:\r\n";
		int i=0;
		
		for (Writable[] thisrow : allfields)
		{
			i++;
			
			String strrow=null;
			
			for (Writable thisfield : thisrow)
			{
				if (strrow==null)
				{
					strrow=((Text)thisfield).toString();
				}
				else
				{
					strrow=strrow + " | " +((Text)thisfield).toString();
				}
			}
			
			all=all+"("+String.valueOf(i)+"):" + strrow +"\r\n";
		}
		
		return all;
	}

	public Text2DArrayWritable() {
		super(Text.class);
		// TODO Auto-generated constructor stub
	}

	public Text2DArrayWritable(Writable[][] values) {
		super(Text.class, values);
		// TODO Auto-generated constructor stub
	}

}
