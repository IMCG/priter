package org.apache.hadoop.examples.priorityiteration;


import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapred.lib.NullOutputFormat;


public class Distributor {
	public int partition(String input, String output, int numparts, Class<? extends WritableComparable> keyclass, Class<? extends Partitioner> partitionclass) throws Exception {
	    
	    JobConf job = new JobConf(new Configuration());
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
}
