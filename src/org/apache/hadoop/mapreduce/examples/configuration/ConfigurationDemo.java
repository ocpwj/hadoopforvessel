package org.apache.hadoop.mapreduce.examples.configuration;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.addResource(ConfigurationDemo.class.getResourceAsStream("configuration-1.xml"));
		System.out.println(conf.get("color"));
		System.out.println(conf.getInt("size",0)); //default value
		System.out.println(conf.get("breadth", "wide"));
		
		System.out.println(conf.get("size-weight"));
		
		System.setProperty("size", "14");//System property over any property in configuration
		System.out.println(conf.get("size-weight"));
		
		conf.set("size", "11");//dont work for System property
		System.out.println(conf.getInt("size",0));
		System.out.println(System.getProperty("size"));
		System.out.println(conf.get("size-weight"));
		
		
	}

}
