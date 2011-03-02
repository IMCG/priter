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


public class ConnectComponent extends Configured implements Tool {
	private String input;
	private String output;
	private String subGraphDir;
	private int partitions;
	private int topk;
	private int nNodes;
	private int exetop;
	private long stoptime;
	
	
	private int conncomp() throws IOException{
	    JobConf job = new JobConf(getConf());
	    String jobname = "connect component";
	    job.setJobName(jobname);
       
	    job.set(MainDriver.SUBGRAPH_DIR, subGraphDir);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormat(TextOutputFormat.class);

	    //set for iterative process   
	    job.setBoolean("priter.job", true);
	    job.setInt("priter.graph.partitions", partitions);			//graph partitions
	    job.setInt("priter.graph.nodes", nNodes);					//total nodes
	    job.setLong("priter.snapshot.interval", 5000);				//snapshot interval	
	    job.setInt("priter.snapshot.topk", topk);					//topk
	    job.setInt("mapred.iterative.topk.scale", partitions);		//expand all topks on each node
	    job.setInt("priter.queue.uniqlength", exetop);				//execution queue
	    job.setLong("priter.stop.maxtime", stoptime);				//termination check

	    
	    job.setJarByClass(ConnectComponent.class);
	    job.setMapperClass(ConnectComponentMap.class);	
	    job.setReducerClass(ConnectComponentReduce.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setPriorityClass(IntWritable.class);
	    job.setPartitionerClass(UniDistIntPartitioner.class);

	    job.setNumMapTasks(partitions);
	    job.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job);
	    return 0;
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 8) {
		      System.err.println("Usage: conncomp <indir> <outdir> <subgraph> <parititons> <topk> <num of nodes> <exetop> <stopthresh>");
		      System.exit(2);
		}
	    
		input = args[0];
		output = args[1];
	    subGraphDir = args[2];
	    partitions = Integer.parseInt(args[3]);
	    topk = Integer.parseInt(args[4]);
	    nNodes = Integer.parseInt(args[5]);
	    exetop = Integer.parseInt(args[6]);
	    stoptime = Long.parseLong(args[7]);
    
	    conncomp();
	    
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new ConnectComponent(), args);
	    System.exit(res);
	}
}
