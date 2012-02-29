package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileBasedActivator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.PrIterBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class SSSPFileActivator extends PrIterBase implements 
	FileBasedActivator<IntWritable, FloatWritable, FloatWritable, LinkArrayWritable> {
	
	private int partitions;
	private int kvs = 0;
	private int iter = 0;
	
	@Override
	public void configure(JobConf job){
	    partitions = job.getInt("priter.graph.partitions", -1);
	}

	@Override
	public void iterate() {
		System.out.println((iter++) + " passes " + kvs + " activations");
	}

  @Override
  public void activate(IntWritable nodeid, FloatWritable value, LinkArrayWritable links, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter) throws IOException {
		kvs++;
		reporter.setStatus(String.valueOf(kvs));

		float distance = value.get();
    int node = nodeid.get();

    System.out.println("input: " + node + "\t" + distance);
    if(node == -1) {
      System.out.println("no links for node " + node);
      for(int i=0; i<partitions; i++){
        output.collect(new IntWritable(i), new FloatWritable(1000000));
      }
      return;
    }

    for(Writable l : links.get()){		
      LinkWritable link = (LinkWritable)l;
      output.collect(new IntWritable(link.getEndPoint()), new FloatWritable(distance + link.getWeight()));
    }
  }
}
