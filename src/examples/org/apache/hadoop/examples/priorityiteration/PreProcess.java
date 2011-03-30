package org.apache.hadoop.examples.priorityiteration;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PreProcess extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 5) {
		      System.err.println("Usage: preprocess <in_static> <out_static> <partitions> <pages> <correct>");
		      System.exit(2);
		}

		String inStatic = args[0];
		String outStatic = args[1];
		int partitions = Integer.parseInt(args[2]);
		int totalpages = Integer.parseInt(args[3]);		
		boolean correct = Boolean.parseBoolean(args[4]);
		
		/*
		//distribute graph job
	    JobConf job = new JobConf(getConf());
	    String jobname = "distribute state data";
	    job.setJobName(jobname);
	    
	    job.set(MainDriver.SUBRANK_DIR, outState);
	    job.setInputFormat(KeyValueTextInputFormat.class);
	    job.setOutputFormat(NullOutputFormat.class);
	    TextInputFormat.addInputPath(job, new Path(inState));
	    
	    job.setJarByClass(PreProcess.class);
	    job.setMapperClass(StateDistributeMap.class);
	    job.setReducerClass(StateDistributeReduce.class);

	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setPartitionerClass(UniDistIntPartitioner.class);
	    
	    job.setNumReduceTasks(ttnum);
	    
	    JobClient.runJob(job);
	    */
	    //################################################################
	    

	    
	    
	    JobConf job2 = new JobConf(getConf());
	    String jobname2 = "distribute static data";
	    job2.setJobName(jobname2);
	    
	    job2.setInt(MainDriver.TOTAL_NODE, totalpages);
	    job2.set(MainDriver.SUBGRAPH_DIR, outStatic);
	    job2.setBoolean(MainDriver.CORRECT, correct);
	    job2.setInputFormat(KeyValueTextInputFormat.class);
	    job2.setOutputFormat(NullOutputFormat.class);
	    TextInputFormat.addInputPath(job2, new Path(inStatic));
	    
	    job2.setJarByClass(PreProcess.class);
	    job2.setMapperClass(StaticDistributeMap.class);
	    job2.setReducerClass(StaticDistributeReduce.class);

	    job2.setMapOutputKeyClass(IntWritable.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(NullWritable.class);
	    job2.setOutputValueClass(NullWritable.class);
	    job2.setPartitionerClass(UniDistIntPartitioner.class);
	    
	    job2.setInt("mapred.iterative.partitions", partitions);   
	    
	    //job2.setNumMapTasks(ttnum);
	    job2.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job2);
	    
	    return 0;

	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new PreProcess(), args);
	    System.exit(res);
	}

}
