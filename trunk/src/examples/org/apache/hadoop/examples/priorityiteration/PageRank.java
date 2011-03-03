package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class PageRank extends Configured implements Tool {
	private String input;
	private String output;
	private String subGraphDir;
	private int partitions;
	private int topk;
	private int nPages;
	private int startPages;
	private float alpha;
	private float stopthresh;
	//private float avgdeg;
	
	//damping factor
	public static final double DAMPINGFAC = 0.8;
	public static final double RETAINFAC = 0.2;
	public static final long OVHDTIME = 1000;

	private int pagerank() throws IOException{
	    JobConf job = new JobConf(getConf());
	    String jobname = "pagerank";
	    job.setJobName(jobname);
       
	    job.set(MainDriver.SUBGRAPH_DIR, subGraphDir);
	    job.setInt(MainDriver.START_NODE, startPages);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormat(TextOutputFormat.class);
	    	    
	    //set for iterative process   
	    job.setBoolean("priter.job", true);
	    job.setInt("priter.graph.partitions", partitions);				//graph partitions
	    job.setInt("priter.graph.nodes", nPages);						//total nodes
	    job.setLong("priter.snapshot.interval", 20000);					//snapshot interval	
	    job.setInt("priter.snapshot.topk", topk);						//topk
	    job.setFloat("priter.queue.portion", alpha);					//execution queue
	    job.setFloat("priter.stop.difference", stopthresh);				//termination check
	    		
	          
	    job.setJarByClass(PageRank.class);
	    job.setActivatorClass(PageRankActivator.class);	
	    job.setUpdatorClass(PageRankUpdator.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(DoubleWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    job.setPriorityClass(DoubleWritable.class);

	    job.setNumMapTasks(partitions);
	    job.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job);
	    return 0;
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 9) {
		      System.err.println("Usage: pagerank <indir> <outdir> <subgraph> <partitions> <topk> <number of nodes> <start nodes> <alpha> <stopthreshold>");
		      System.exit(2);
		}
	    
		input = args[0];
	    output = args[1];
	    subGraphDir = args[2];
	    partitions = Integer.parseInt(args[3]);
	    topk = Integer.parseInt(args[4]);
	    nPages = Integer.parseInt(args[5]);
	    startPages = Integer.parseInt(args[6]);
	    alpha = Float.parseFloat(args[7]);
	    stopthresh = Float.parseFloat(args[8]);
    
	    pagerank();
	    
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
	    System.exit(res);
	}
}
