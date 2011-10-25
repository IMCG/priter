package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
	private long snapinterval = 20000;
	private float stopthresh;
	
	//damping factor
	public static final double DAMPINGFAC = 0.8;
	public static final double RETAINFAC = 0.2;

	
	private int adsorption() throws IOException{
	    JobConf job = new JobConf(getConf());
	    String jobname = "adsorption";
	    job.setJobName(jobname);
       
	    job.set(MainDriver.SUBGRAPH_DIR, subGraphDir);
	    if(partitions == 0) partitions = Util.getTTNum(job);			//set default partitions = num of task trackers
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormat(TextOutputFormat.class);
	    
	    //set for iterative process   
	    job.setBoolean("priter.job", true);
	    job.setInt("priter.graph.partitions", partitions);				//graph partitions
	    job.setLong("priter.snapshot.interval", snapinterval);			//snapshot interval	
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
	
	static int printUsage() {
		System.out.println("adsorption [-p <partitions>] [-k <options>] [-q <qportion>] " +
				"[-i <snapshot interval>] [-t <termination threshod>] input output");
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
	        } else if ("-k".equals(args[i])) {
	        	topk = Integer.parseInt(args[++i]);
	        } else if ("-q".equals(args[i])) {
	        	qportion = Float.parseFloat(args[++i]);
	        } else if ("-i".equals(args[i])) {
	        	snapinterval = Long.parseLong(args[++i]);
	        } else if ("-t".equals(args[i])) {
	        	stopthresh = Float.parseFloat(args[++i]);
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
	    if (other_args.size() != 2) {
	      System.out.println("ERROR: Wrong number of parameters: " +
	                         other_args.size() + " instead of 2.");
	      return printUsage();
	    }
	    
	    input = other_args.get(0);
	    output = other_args.get(1);
	    subGraphDir = input + "_subgraph";
	    
	    Distributor.partition(input, subGraphDir, partitions, IntWritable.class, HashPartitioner.class);
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
