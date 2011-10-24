package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SSSP extends Configured implements Tool {
	private String input;
	private String output;
	private String subGraphDir;
	private int partitions;
	private int topk;
	private int startnode;
	private int queuelen;
	private int stopthresh;
	
	private int sssp() throws IOException{
	    JobConf job = new JobConf(getConf());
	    String jobname = "shortest path";
	    job.setJobName(jobname);
       
	    job.set(MainDriver.SUBGRAPH_DIR, subGraphDir);
	    job.setInt(MainDriver.START_NODE, startnode);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormat(TextOutputFormat.class);
	    
	    //set for iterative process   
	    job.setBoolean("priter.job", true);
	    job.setInt("priter.graph.partitions", partitions);				//graph partitions
	    job.setLong("priter.snapshot.interval", 5000);					//snapshot interval	 
	    job.setInt("priter.snapshot.topk", topk);						//topk 
	    job.setInt("priter.queue.lenth", queuelen);						//execution queue
	    job.setFloat("priter.stop.difference", stopthresh);				//termination check
	    
	    job.setJarByClass(SSSP.class);
	    job.setActivatorClass(SSSPActivator.class);	
	    job.setUpdaterClass(SSSPUpdater.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setPriorityClass(IntWritable.class);    

	    job.setNumMapTasks(partitions);
	    job.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job);
	    return 0;
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 7) {
		      System.err.println("Usage: sssp <indir> <outdir> <partitions> <startnode> <topk> <qlen> <stopthreshold>");
		      System.exit(2);
		}
	    
		input = args[0];
	    output = args[1];
	    partitions = Integer.parseInt(args[2]);
	    startnode = Integer.parseInt(args[3]);
	    topk = Integer.parseInt(args[4]);
	    queuelen = Integer.parseInt(args[5]);
	    stopthresh = Integer.parseInt(args[6]);
	    
	    subGraphDir = input + "/subgraph";
	    new Distributor().partition(input, output, partitions, IntWritable.class, HashPartitioner.class);
	    sssp();
	    
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new SSSP(), args);
	    System.exit(res);
	}

}