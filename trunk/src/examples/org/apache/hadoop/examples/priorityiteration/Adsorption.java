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
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Adsorption extends Configured implements Tool {
	private String input;
	private String output;
	private String subGraphDir;
	private int partitions;
	private int topk;
	private float qportion;
	private float stopthresh;
	
	//damping factor
	public static final double DAMPINGFAC = 0.8;
	public static final double RETAINFAC = 0.2;

	
	private int adsorption() throws IOException{
	    JobConf job = new JobConf(getConf());
	    String jobname = "adsorption";
	    job.setJobName(jobname);
       
	    job.set(MainDriver.SUBGRAPH_DIR, subGraphDir);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormat(TextOutputFormat.class);
	    
	    
	    //set for iterative process   
	    job.setBoolean("priter.job", true);
	    job.setInt("priter.graph.partitions", partitions);				//graph partitions
	    job.setLong("priter.snapshot.interval", 20000);					//snapshot interval	
	    job.setInt("priter.snapshot.topk", topk);						//topk
	    job.setFloat("priter.queue.portion", qportion);					//activation queue
	    job.setFloat("priter.stop.difference", stopthresh);				//termination check
	    
	        
	    job.setJarByClass(Adsorption.class);
	    job.setActivatorClass(AdsorptionActivator.class);	
	    job.setUpdaterClass(AdsorptionUpdater.class);
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
		if (args.length != 6) {
		      System.err.println("Usage: adsorption <indir> <outdir> <partitions> <topk> <qportion> <stopthreshold>");
		      System.exit(2);
		}
	    
		input = args[0];
	    output = args[1];
	    partitions = Integer.parseInt(args[2]);
	    topk = Integer.parseInt(args[3]);
	    qportion = Float.parseFloat(args[4]);
	    stopthresh = Float.parseFloat(args[5]);
	    
	    subGraphDir = input + "/subgraph";
	    new Distributor().partition(input, output, partitions, IntWritable.class, HashPartitioner.class);
	    adsorption();
	    
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Adsorption(), args);
	    System.exit(res);
	}
}