package com.ljc.bigdata.mapreduce;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool{
	
	static final String PROCESS_OUTPUT_FILE = "resultjson";
	private static final Log LOG = LogFactory.getLog(Driver.class);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			int res = ToolRunner.run(new Configuration(), new Driver(), args);
			System.exit(res);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "My Average Temperature Program");
		job.setJarByClass(Driver.class);
		LOG.info("Beginning: " + new Date());
		
		final String numeroReducers = job.getConfiguration().get("num-reducers");
		
		Path outputPath = new Path(arg0[1]);
		
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.setInputPaths(job, arg0[0]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//Agregamos el fichero para salida al job.
		MultipleOutputs.addNamedOutput(job, PROCESS_OUTPUT_FILE, TextOutputFormat.class, Text.class, Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if (numeroReducers != null && numeroReducers.length() > 0) {
			job.setNumReduceTasks(Integer.valueOf(numeroReducers));
		} else {
			job.setNumReduceTasks(9);
		}
		
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(getConf()).delete(outputPath, false);
		
		LOG.info("Finaliza el job " + new Date());
		
		job.waitForCompletion(true);
		
		return 0;
	}

}