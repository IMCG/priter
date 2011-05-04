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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Kaz extends Configured implements Tool {
	private String input;
	private String output;
	private String subGraphDir;
	private int partitions;
	private int topk;
	private int nPages;
	private int startPages;
	private float alpha;
	private float stopthresh;
	private boolean basync;
	private boolean bPriExec;
	private float beta;
	//private float avgdeg;
	
	//damping factor
	public static final long OVHDTIME = 1000;

	private int kaz() throws IOException{
	    JobConf job = new JobConf(getConf());
	    String jobname = "kaz";
	    job.setJobName(jobname);
       
	    job.set(MainDriver.SUBGRAPH_DIR, subGraphDir);
	    job.setInt(MainDriver.START_NODE, startPages);
	    job.setFloat(MainDriver.KATZ_BETA, beta);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormat(TextOutputFormat.class);
	    	    
	    //set for iterative process   
	    job.setBoolean("priter.job", true);
	    job.setBoolean("priter.job.priority", bPriExec);
	    job.setBoolean("priter.job.async.self", basync);					//async by self trigger
	    job.setInt("priter.graph.partitions", partitions);				//graph partitions
	    job.setInt("priter.graph.nodes", nPages);						//total nodes
	    job.setLong("priter.snapshot.interval", 10000);					//snapshot interval	
	    job.setInt("priter.snapshot.topk", topk);						//topk
	    job.setFloat("priter.queue.portion", alpha);					//execution queue
	    job.setFloat("priter.stop.difference", stopthresh);				//termination check
	    		
	          
	    job.setJarByClass(Kaz.class);
	    job.setActivatorClass(KazActivator.class);	
	    job.setUpdatorClass(KazUpdator.class);
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
		if (args.length != 12) {
		      System.err.println("Usage: kaz <indir> <outdir> <subgraph> <partitions> <topk> <number of nodes> <start nodes> <alpha> <stopthreshold> <async> <allextract>");
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
	    basync = Boolean.parseBoolean(args[9]);
	    bPriExec = Boolean.parseBoolean(args[10]);
	    beta = Float.parseFloat(args[11]);
    
	    kaz();
	    
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new Kaz(), args);
	    System.exit(res);
	}
}
