package org.apache.hadoop.examples.priorityiteration;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.NullOutputFormat;


public class Distributor extends Configured {

	public int partition(String input, String output, int numparts, Class<? extends WritableComparable> keyclass, Class<? extends Partitioner> partitionclass) throws Exception {
	    
	    JobConf job = new JobConf(getConf());
	    String jobname = "distribute input data";
	    job.setJobName(jobname);
	    
	    job.set(MainDriver.SUBGRAPH_DIR, output);
	    job.setInputFormat(KeyValueTextInputFormat.class);
	    job.setOutputFormat(NullOutputFormat.class);
	    TextInputFormat.addInputPath(job, new Path(input));
	    
	    job.setJarByClass(Distributor.class);
	    if(keyclass == Text.class){
		    job.setMapperClass(DistributorMaps.TextMap.class);
	    }else if(keyclass == IntWritable.class){
	    	job.setMapperClass(DistributorMaps.IntMap.class);
	    }else if(keyclass == FloatWritable.class){
	    	job.setMapperClass(DistributorMaps.FloatMap.class);
	    }else if(keyclass == DoubleWritable.class){
	    	job.setMapperClass(DistributorMaps.DoubleMap.class);
	    }
	    job.setReducerClass(DistributorReduce.class);

	    job.setMapOutputKeyClass(keyclass);
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
		      System.err.println("Usage: partition <in_static> <out_static> <partitions> <key type> <partition class>");
		      System.exit(2);
		}
		
		Class<? extends WritableComparable> keyclass = null;
		Class<? extends Partitioner> partitionerclass = null;
		
		try{
			if(Class.forName(args[3]).getSuperclass() == WritableComparable.class){
				keyclass = (Class<? extends WritableComparable>) Class.forName(args[3]);
			}else{
				System.out.println(args[3] + " is not key Class");
			}
		}catch (ClassNotFoundException e){
			keyclass = Text.class;
			System.out.println("no key Class named " + args[3] + " found, use Text by default");
		}

		try{
			if(Class.forName(args[4]).getSuperclass() == Partitioner.class){
				partitionerclass = (Class<? extends Partitioner>) Class.forName(args[4]);
			}else{
				System.out.println(args[3] + " is not partitioner Class");
			}
		}catch (ClassNotFoundException e){
			partitionerclass = HashPartitioner.class;
			System.out.println("no Partitioner Class named " + args[4] + " found, use Hash Partitioner by default");
		}

		new Distributor().partition(args[0], args[1], Integer.parseInt(args[2]), keyclass, partitionerclass);
	}

}
