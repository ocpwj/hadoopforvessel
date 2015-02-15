package com.thomsonreuters.ce.dbor.hadoop.example.vessellocation.v6aggregration.eventcalc;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class DatafileOnly implements PathFilter,Configurable {
	
	private Configuration Conf;

	public DatafileOnly() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean accept(Path arg0) {
		// TODO Auto-generated method stub
		
		try {
			
			FileSystem fs = FileSystem.get(Conf);
			FileStatus stat = fs.getFileStatus(arg0);
			String filename=arg0.getName();
			
			if (stat.isDir())
			{
				return true;
			}


			if (filename.equals("data"))
			{
				if (stat.getLen()==0)
				{
					System.out.println("Ignoring file:"+ arg0.getName());
					return false;
				}
				return true;
			}
			else
			{
				return false;
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return this.Conf;
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.Conf = conf;
	}

}
