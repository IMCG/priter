package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileBasedActivator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.PrIterBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;


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

    if(links == null) {
      System.out.println("no links for node " + node);
      for(int i=0; i<partitions; i++){
        output.collect(new IntWritable(i), new FloatWritable(Float.MAX_VALUE));
      }
      return;
    }

    for(Writable l : links.get()){		
      LinkWritable link = (LinkWritable)l;
      output.collect(new IntWritable(link.getEndPoint()), new FloatWritable(distance + link.getWeight()));
    }
  }
}
