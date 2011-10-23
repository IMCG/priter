package org.apache.hadoop.examples.priorityiteration;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Distributor extends Configured {

	public int partition(String input, String output, int numparts, int nodes, Class partitionclass) throws Exception {
	    
	    JobConf job = new JobConf(getConf());
	    String jobname = "distribute input data";
	    job.setJobName(jobname);
	    
	    job.setInt(MainDriver.TOTAL_NODE, nodes);
	    job.set(MainDriver.SUBGRAPH_DIR, output);
	    job.setInputFormat(KeyValueTextInputFormat.class);
	    job.setOutputFormat(NullOutputFormat.class);
	    TextInputFormat.addInputPath(job, new Path(input));
	    
	    job.setJarByClass(Distributor.class);
	    job.setMapperClass(IdentityMapper.class);
	    job.setReducerClass(StaticDistributeReduce.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setPartitionerClass(partitionclass);
	    
	    job.setInt("mapred.iterative.partitions", numparts);   
	    
	    job.setNumMapTasks(numparts*2);
	    job.setNumReduceTasks(numparts);
	    
	    JobClient.runJob(job);
	    
	    return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
		      System.err.println("Usage: partition <in_static> <out_static> <partitions> <pages> <partition class>");
		      System.exit(2);
		}
		Class partitionerclass = Class.forName(args[4]);
		new Distributor().partition(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), partitionerclass);
	}

}
