package org.apache.hadoop.examples.priorityiteration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TransposeGraph extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TransposeGraph(), args);
	    System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
		      System.err.println("Usage: transGraph <indir> <outdir> <numreduce>");
		      System.exit(2);
		}
		
		String input = args[0];
		String output = args[1];
		int numReduce = Integer.parseInt(args[2]);
		
	    JobConf job = new JobConf(getConf());
	    String jobname = "transpose graph";
	    job.setJobName(jobname);
	    
	    job.setJarByClass(TransposeGraph.class);
	    job.setMapperClass(TransposeGraphMap.class);	
	    job.setReducerClass(TransposeGraphReduce.class);
	    job.setInputFormat(KeyValueTextInputFormat.class);
	    job.setOutputFormat(TextOutputFormat.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));

	    JobClient jobclient = new JobClient(job);
	    ClusterStatus status = jobclient.getClusterStatus();
	    int ttnum = status.getTaskTrackers();
	    job.setInt("mapred.iterative.ttnum", ttnum);
	    
	    job.setNumMapTasks(ttnum);
	    job.setNumReduceTasks(numReduce);
	    
	    JobClient.runJob(job);
		return 0;
	}

}
