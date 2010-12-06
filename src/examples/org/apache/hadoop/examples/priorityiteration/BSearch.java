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
	private int topk;
	private int nNodes;
	private float wearfactor;
	private int emitSize;
	private int startnode;
	
	private int bsearch() throws IOException{
	    JobConf job = new JobConf(getConf());
	    String jobname = "shortest path";
	    job.setJobName(jobname);
       
	    job.set(MainDriver.SUBGRAPH_DIR, subGraphDir);
	    job.setInt(MainDriver.SP_TOTAL_NODES, nNodes);
	    job.setInt(MainDriver.SP_START_NODE, startnode);
	    job.setBoolean(MainDriver.IN_MEM, true);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormat(TextOutputFormat.class);
	    
	    int ttnum = Util.getTTNum(job);

	    //set for iterative process
	    job.setBoolean("mapred.job.iterative", true);
	    job.setBoolean("mapred.job.iterative.sort", false);
	    job.setInt("mapred.iterative.ttnum", ttnum);
	    job.setInt("mapred.iterative.topk", topk);
	    job.setFloat("mapred.iterative.output.wearfactor", wearfactor);
	    job.setLong("mapred.iterative.snapshot.interval", 5000);    
	    job.setInt("mapred.iterative.reduce.emitsize", emitSize);
	    job.setLong("mapred.iterative.reduce.window", -1);		//set -1 more accurate, ow more stable    
	    
	    job.setJarByClass(BSearch.class);
	    job.setMapperClass(BSearchMap.class);	
	    job.setReducerClass(BSearchReduce.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setPartitionerClass(UniDistIntPartitioner.class);

	    job.setNumMapTasks(ttnum);
	    job.setNumReduceTasks(ttnum);
	    
	    JobClient.runJob(job);
	    return 0;
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 8) {
		      System.err.println("Usage: bsearch <indir> <outdir> <subgraph> <topk> <num of nodes> <startnode> <emitsize> <wearfactor>");
		      System.exit(2);
		}
	    
		input = args[0];
		output = args[1];
	    subGraphDir = args[2];
	    topk = Integer.parseInt(args[3]);
	    nNodes = Integer.parseInt(args[4]);
	    startnode = Integer.parseInt(args[5]);
	    emitSize = Integer.parseInt(args[6]);
	    wearfactor = Float.parseFloat(args[7]);
    
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
