package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileBasedActivator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.PrIterBase;
import org.apache.hadoop.mapred.Reporter;

public class PageRankFileActivator extends PrIterBase implements
	FileBasedActivator<IntWritable, FloatWritable, FloatWritable, IntArrayWritable> {

	private int iter = 0;
	private int kvs = 0;				//for tracking
	private int partitions;


	@Override
	public void configure(JobConf job) {
		partitions = job.getInt("priter.graph.partitions", 1);
	}

	@Override
	public void iterate() {
		System.out.println((iter++) + " passes " + kvs + " activations");
	}

	@Override
	public void activate(IntWritable nodeid, FloatWritable value,
			IntArrayWritable links,
			OutputCollector<IntWritable, FloatWritable> output,
			Reporter reporter) throws IOException {
		kvs++;
		reporter.setStatus(String.valueOf(kvs));
		
		int page = nodeid.get();

		if(links == null){
			System.out.println("no links found for page " + page);
			for(int i=0; i<partitions; i++){
				output.collect(new IntWritable(i), new FloatWritable(0));
			}
			return;
		}	
		int size = links.get().length;
		float delta = value.get() * PageRank.DAMPINGFAC / size;
		
		
		for(Writable end : links.get()){
			output.collect((IntWritable)end, new FloatWritable(delta));
		}
		
	}
}
