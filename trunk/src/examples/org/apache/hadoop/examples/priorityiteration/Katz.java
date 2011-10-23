package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Katz extends Configured implements Tool {
	private String input;
	private String output;
	private String subGraphDir;
	private int partitions;
	private int topk;
	private float qportion;
	private float stopthresh;
	private float beta;

	private int katz() throws IOException{
	    JobConf job = new JobConf(getConf());
	    String jobname = "katz metric";
	    job.setJobName(jobname);
       
	    job.set(MainDriver.SUBGRAPH_DIR, subGraphDir);
	    job.setFloat(MainDriver.KATZ_BETA, beta);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormat(TextOutputFormat.class);
	    	    
	    //set for iterative process   
	    job.setBoolean("priter.job", true);
	    job.setInt("priter.graph.partitions", partitions);				//graph partitions
	    job.setLong("priter.snapshot.interval", 10000);					//snapshot interval	
	    job.setInt("priter.snapshot.topk", topk);						//topk
	    job.setFloat("priter.queue.portion", qportion);					//execution queue
	    job.setFloat("priter.stop.difference", stopthresh);				//termination check
	    		
	    job.setJarByClass(Katz.class);
	    job.setActivatorClass(KatzActivator.class);	
	    job.setUpdaterClass(KatzUpdator.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(FloatWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(FloatWritable.class);
	    job.setPriorityClass(FloatWritable.class);

	    job.setNumMapTasks(partitions);
	    job.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job);
	    
	    return 0;
	}
	@Override
	public int run(String[] args) throws Exception {	
		if (args.length != 7) {
		      System.err.println("Usage: katz <indir> <outdir> <partitions> <topk> <qportion> <stopthreshold> <beta>");
		      System.exit(2);
		}
	    
		input = args[0];
	    output = args[1];
	    partitions = Integer.parseInt(args[2]);
	    topk = Integer.parseInt(args[3]);
	    qportion = Float.parseFloat(args[4]);
	    stopthresh = Float.parseFloat(args[5]);
	    beta = Float.parseFloat(args[6]);
	    
	    subGraphDir = input + "/subgraph";
	    new Distributor().partition(input, output, partitions, IntWritable.class, HashPartitioner.class);
	    katz();
	    
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new Katz(), args);
	    System.exit(res);
	}
}
