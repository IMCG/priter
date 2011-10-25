package org.apache.hadoop.examples.priorityiteration;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GenGraph extends Configured implements Tool {

	private String output;
	private int partitions;
	private long nodes;
	private String graphtype = "unweighted";
	private double degree_mu = 0.5;
	private double degree_sigma = 2;
	private double weight_mu = 0.5;
	private double weight_sigma = 1;
	
	private int disgengraph() throws IOException{
	    JobConf job = new JobConf(getConf());
	    job.setJobName("gengraph " + nodes + ":" + graphtype);    
	    
	    if(partitions == 0) partitions = Util.getTTNum(job);			//set default partitions = num of task trackers
	    
	    job.setLong(MainDriver.GEN_CAPACITY, nodes);
	    job.set(MainDriver.GEN_TYPE, graphtype);
	    job.set(MainDriver.GEN_OUT, output);
	    job.setFloat(MainDriver.DEGREE_MU, (float)degree_mu);
	    job.setFloat(MainDriver.DEGREE_SIGMA, (float)degree_sigma);
	    job.setFloat(MainDriver.WEIGHT_MU, (float)weight_mu);
	    job.setFloat(MainDriver.WEIGHT_MU, (float)weight_sigma); 
	    
	    job.setJarByClass(GenGraph.class);  
	    job.setInputFormat(RandomWriter.RandomInputFormat.class);
	    job.setOutputFormat(NullOutputFormat.class);
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    job.setMapperClass(GenGraphMap.class);
	    job.setReducerClass(IdentityReducer.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(NullWritable.class);
	    
	    job.setNumMapTasks(partitions);
	    
	    JobClient.runJob(job);
		return 0;
	}
	
	static int printUsage() {
		System.out.println("gengraph [-p <partitions>] [-n <num of nodes>] " +
				"[-t <graph type (weighted|unweighted)>] " +
				"[-degree_mu <lognormal degree mu>] " +
				"[-degree_sigma <lognormal degree sigma>] " +
				"[-weight_mu <lognormal weight mu>] " +
				"[-weight_sigma <lognormal weight sigma>] + output");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	@Override
	public int run(String[] args) throws Exception {
	    List<String> other_args = new ArrayList<String>();
	    for(int i=0; i < args.length; ++i) {
	      try {
	        if ("-p".equals(args[i])) {
	        	partitions = Integer.parseInt(args[++i]);
	        } else if ("-n".equals(args[i])) {
	        	nodes = Long.parseLong(args[++i]);
	        } else if ("-t".equals(args[i])) {
	        	graphtype = args[++i];
	        	if(!graphtype.equals("weighted") && !graphtype.equals("unweighted")){
	    	        System.out.println("ERROR: graph type should be weighted|unweighted");
	    	        return printUsage();
	        	}
	        } else if ("-degree_mu".equals(args[i])) {
	        	degree_mu = Double.parseDouble(args[++i]);
	        } else if ("-degree_sigma".equals(args[i])) {
	        	degree_sigma = Double.parseDouble(args[++i]);
	        } else if ("-weight_mu".equals(args[i])) {
	        	weight_mu = Double.parseDouble(args[++i]);
	        } else if ("-weight_sigma".equals(args[i])) {
	        	weight_sigma = Double.parseDouble(args[++i]);
	        } else {
	          other_args.add(args[i]);
	        }
	      } catch (NumberFormatException except) {
	        System.out.println("ERROR: Integer expected instead of " + args[i]);
	        return printUsage();
	      } catch (ArrayIndexOutOfBoundsException except) {
	        System.out.println("ERROR: Required parameter missing from " +
	                           args[i-1]);
	        return printUsage();
	      }
	    }
	    // Make sure there are exactly 2 parameters left.
	    if (other_args.size() != 1) {
	      System.out.println("ERROR: Wrong number of parameters: " +
	                         other_args.size() + " instead of 2.");
	      return printUsage();
	    }
	    
	    output = other_args.get(0);

	    disgengraph();
	    return 0;
	}


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GenGraph(), args);
	    System.exit(res);
	}

}
