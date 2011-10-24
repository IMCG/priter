package org.apache.hadoop.examples.priorityiteration;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GenGraph extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 6) {
		      System.err.println("Usage: disgengraph <in> <outpath> <node num> <argument> <type> <partitions>");
		      System.exit(2);
		}
		
		String outpath = args[1];
		int capacity = Integer.parseInt(args[2]);
		int argument = Integer.parseInt(args[3]);
		String type = args[4];
		int paritions = Integer.parseInt(args[5]);
		
		
	    JobConf job = new JobConf(getConf());
	    job.setJobName("gengraph " + capacity + ":" + type);    
	    
	    job.setInt(MainDriver.GEN_CAPACITY, capacity);
	    job.setInt(MainDriver.GEN_ARGUMENT, argument);
	    job.set(MainDriver.GEN_TYPE, type);
	    job.set(MainDriver.GEN_OUT, outpath);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(outpath));
	    
	    job.setJarByClass(GenGraph.class);
	        
	    job.setInputFormat(TextInputFormat.class);
	    job.setOutputFormat(NullOutputFormat.class);
	    
	    job.setMapperClass(GenGraphMap.class);
	    job.setReducerClass(IdentityReducer.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(NullWritable.class);
	    
	    job.setNumMapTasks(paritions);
	    
	    JobClient.runJob(job);
		return 0;
	}


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GenGraph(), args);
	    System.exit(res);
	}

}
