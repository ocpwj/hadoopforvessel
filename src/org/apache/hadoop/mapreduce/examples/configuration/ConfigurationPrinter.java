package org.apache.hadoop.mapreduce.examples.configuration;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;

public class ConfigurationPrinter extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		System.out.println("Run received parameters below, obviously GenericOptionsParser filter out some parameters");
		
		for(String arg : arg0)
		{
			System.out.println(arg);
		}
		
		System.out.println("--------------------------------");
		
		Configuration conf = getConf();
		for (Entry<String, String> entry : conf) {
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
		}
		
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		//Parameters supported by GenericOptionsParser
		//hadoop ConfigurationPrinter -D color=yellow
		//hadoop ConfigurationPrinter -conf conf/hadoop-localhost.xml
		
		System.out.println("GenericOptionsParser picks up args below:");
		
		for(String arg : args)
		{
			System.out.println(arg);
		}
		
		System.out.println("--------------------------------");
		
		int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
		
			
		System.exit(exitCode);
	}

}
