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
	private float wearfactor;
	private int topk;
	private int nNodes;
	private int emitSize;
	
	//damping factor
	public static final double DAMPINGFAC = 0.8;
	public static final double RETAINFAC = 0.2;

	
	private int pagerank() throws IOException{
	    JobConf job = new JobConf(getConf());
	    String jobname = "pagerank";
	    job.setJobName(jobname);
       
	    job.set(MainDriver.SUBGRAPH_DIR, subGraphDir);
	    job.setInt(MainDriver.PG_TOTAL_PAGES, nNodes);
	    job.setInt(MainDriver.TOP_K, topk);
	    job.setBoolean(MainDriver.IN_MEM, true);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormat(TextOutputFormat.class);
	    
	    int ttnum = Util.getTTNum(job);
	    	    
	    //set for iterative process   
	    job.setBoolean("mapred.job.iterative", true);
	    job.setInt("mapred.iterative.ttnum", ttnum);
	    job.setInt("mapred.iterative.topk", topk);
	    job.setFloat("mapred.iterative.output.wearfactor", wearfactor);
	    job.setLong("mapred.iterative.snapshot.interval", 20000);
	    job.setInt("mapred.iterative.reduce.emitsize", emitSize);
	    job.setLong("mapred.iterative.reduce.window", -1);	  
	        
	    job.setJarByClass(PageRank.class);
	    job.setMapperClass(PageRankMap.class);	
	    job.setReducerClass(PageRankReduce.class);
	    job.setPriorityClass(DoubleWritable.class);			//set priority class
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(DoubleWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    job.setPartitionerClass(UniDistIntPartitioner.class);

	    job.setNumMapTasks(ttnum);
	    job.setNumReduceTasks(ttnum);
	    
	    JobClient.runJob(job);
	    return 0;
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 7) {
		      System.err.println("Usage: pagerank <indir> <outdir> <subgraph> <topk> <number of nodes> <reduce output granularity> <wear factor>");
		      System.exit(2);
		}
	    
		input = args[0];
	    output = args[1];
	    subGraphDir = args[2];
	    topk = Integer.parseInt(args[3]);
	    nNodes = Integer.parseInt(args[4]);
	    emitSize = Integer.parseInt(args[5]);
	    wearfactor = Float.parseFloat(args[6]);
    
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
