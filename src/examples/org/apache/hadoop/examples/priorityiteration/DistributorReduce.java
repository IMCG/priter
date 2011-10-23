package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DistributorReduce extends MapReduceBase implements
		Reducer<Writable, Text, NullWritable, NullWritable> {

	private FSDataOutputStream out;
	private BufferedWriter writer;
	
	@Override
	public void configure(JobConf job){
		String outDir = job.get(MainDriver.SUBGRAPH_DIR);
		FileSystem fs;
		try {
			fs = FileSystem.get(job);
			int taskid = Util.getTaskId(job);
			Path outPath = new Path(outDir + "/part" + taskid);
			out = fs.create(outPath);
			writer = new BufferedWriter(new OutputStreamWriter(out));
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	@Override
	public void reduce(Writable key, Iterator<Text> values,
			OutputCollector<NullWritable, NullWritable> arg2, Reporter arg3)
			throws IOException {
		while(values.hasNext()){
			Text value = values.next();
			writer.write(key + "\t" + value + "\n");
		}
	}

	@Override
	public void close(){
		try {
			writer.close();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
