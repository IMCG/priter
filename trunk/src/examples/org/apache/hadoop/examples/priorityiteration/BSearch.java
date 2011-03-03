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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BSearch extends Configured implements Tool {
	private String input;
	private String output;
	private String subGraphDir;
	private int partitions;
	private int topk;
	private int nNodes;
	private int startnode;
	private int queuelen;
	private int stopthresh;
	
	private int bsearch() throws IOException{
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
	    job.setInt("priter.graph.nodes", nNodes);						//total nodes
	    job.setLong("priter.snapshot.interval", 5000);					//snapshot interval	 
	    job.setInt("priter.snapshot.topk", topk);						//topk 
	    job.setInt("priter.queue.lenth", queuelen);						//execution queue
	    job.setFloat("priter.stop.difference", stopthresh);				//termination check
	    
	    
	    job.setJarByClass(BSearch.class);
	    job.setActivatorClass(BSearchActivator.class);	
	    job.setUpdatorClass(BSearchUpdator.class);
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
		if (args.length != 9) {
		      System.err.println("Usage: bsearch <indir> <outdir> <subgraph> <parititons> <topk> <num of nodes> <startnode> <queuelen> <stop>");
		      System.exit(2);
		}
	    
		input = args[0];
		output = args[1];
	    subGraphDir = args[2];
	    partitions = Integer.parseInt(args[3]);
	    topk = Integer.parseInt(args[4]);
	    nNodes = Integer.parseInt(args[5]);
	    startnode = Integer.parseInt(args[6]);
	    queuelen = Integer.parseInt(args[7]);
	    stopthresh = Integer.parseInt(args[8]);
    
	    bsearch();
	    
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new BSearch(), args);
	    System.exit(res);
	}

}
