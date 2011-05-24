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

public class PWCluster extends Configured implements Tool {
	private String input;
	private String output;
	private String subNodessDir;
	private String mainGraph;
	private String sciTablefile;
	private String sclusterTablefile;
	private int partitions;
	private int topk;
	private float alpha;
	private int nNodes;

	private int pwcluster() throws IOException{
	    JobConf job = new JobConf(getConf());
	    String jobname = "pwcluster";
	    job.setJobName(jobname);
       
	    job.set("pwcluster.subnodes", subNodessDir);
	    job.set("pwcluster.mainGraph", mainGraph);
	    job.set("pwcluster.sciTablefile", sciTablefile);
	    job.set("pwcluster.sclusterTablefile", sclusterTablefile);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormat(TextOutputFormat.class);
	    	    
	    //set for iterative process   
	    job.setBoolean("priter.job", true);
	    job.setBoolean("priter.job.async", false);
	    job.setInt("priter.graph.partitions", partitions);				//graph partitions
	    job.setInt("priter.graph.nodes", nNodes);	
	    job.setInt("priter.stop.maxiteration", 5);						//total iteration
	    job.setLong("priter.snapshot.interval", 20000);					//snapshot interval	
	    job.setInt("priter.snapshot.topk", topk);						//topk
	    job.setFloat("priter.queue.portion", alpha);					//execution queue
	    //job.setFloat("priter.stop.difference", stopthresh);				//termination check
	    //job.set("priter.scheduler", scheduler);							//specify scheduler type		
	          
	    job.setJarByClass(PWCluster.class);
	    job.setActivatorClass(PWClusterActivator.class);	
	    job.setUpdatorClass(PWClusterUpdator.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(ClusterWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(ClusterWritable.class);
	    job.setPriorityClass(DoubleWritable.class);

	    job.setNumMapTasks(partitions);
	    job.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job);
	    return 0;
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 10) {
		      System.err.println("Usage: pwcluster <indir> <outdir> <subnodegraph> <maingraph> <scitable> <sclustertable> <partitions> <topk> <number of nodes> <alpha>");
		      System.exit(2);
		}
	    
		//System.out.println(args[0] + "\t" + args[8]);
		input = args[0];
	    output = args[1];
	    subNodessDir = args[2];
	    mainGraph = args[3];
	    sciTablefile = args[4];
	    sclusterTablefile = args[5];
	    
	    partitions = Integer.parseInt(args[6]);
	    topk = Integer.parseInt(args[7]);
	    nNodes = Integer.parseInt(args[8]);
	    alpha = Float.parseFloat(args[9]);
	    
	    pwcluster();
	    
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new PWCluster(), args);
	    System.exit(res);
	}
}
