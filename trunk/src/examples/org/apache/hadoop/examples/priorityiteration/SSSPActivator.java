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
import org.apache.hadoop.mapred.Activator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.PrIterBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;


public class SSSPActivator extends PrIterBase implements 
	Activator<IntWritable, FloatWritable, FloatWritable> {
	
	private String subGraphsDir;
	private int partitions;
	private int startnode;
	private int kvs = 0;
	private int iter = 0;
	
	//graph in local memory
	private HashMap<Integer, ArrayList<Link>> linkList = new HashMap<Integer, ArrayList<Link>>();

	private class Link{
		int node;
		float weight;
		
		public Link(int n, float w){
			node = n;
			weight = w;
		}
		
		@Override
		public String toString() {
			return new String(node + "\t" + weight);
		}
	}

	private synchronized void loadGraphToMem(JobConf conf, int n){
		subGraphsDir = conf.get(MainDriver.SUBGRAPH_DIR);
		Path remote_link = new Path(subGraphsDir + "/part" + n);
		
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(conf);
			FSDataInputStream in = hdfs.open(remote_link);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					String node = line.substring(0, index);
					
					String linkstring = line.substring(index+1);
					ArrayList<Link> links = new ArrayList<Link>();
					StringTokenizer st = new StringTokenizer(linkstring);
					while(st.hasMoreTokens()){
						String link = st.nextToken();
						//System.out.println(link);
						String item[] = link.split(",");
						Link l = new Link(Integer.parseInt(item[0]), Float.parseFloat(item[1]));
						links.add(l);
					}

					this.linkList.put(Integer.parseInt(node), links);
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void configure(JobConf job){   
	    startnode = job.getInt(MainDriver.START_NODE, 0);
	    int taskid = Util.getTaskId(job);
	    partitions = job.getInt("priter.graph.partitions", -1);
		loadGraphToMem(job, taskid);
	}
	
	@Override
	public void activate(IntWritable key, FloatWritable value,
			OutputCollector<IntWritable, FloatWritable> output, Reporter report)
			throws IOException {
		kvs++;
		report.setStatus(String.valueOf(kvs));

		float distance = value.get();
		if(distance != Integer.MAX_VALUE){	
			int node = key.get();
			ArrayList<Link> links = null;
			links = this.linkList.get(node);
			
			if(links == null) {
				System.out.println("no links for node " + node);
				for(int i=0; i<partitions; i++){
					output.collect(new IntWritable(i), new FloatWritable(Float.MAX_VALUE));
				}
				return;
			}
				
			for(Link l : links){				
				output.collect(new IntWritable(l.node), new FloatWritable(distance + l.weight));
			}
		} else{
			for(int i=0; i<partitions; i++){
				output.collect(new IntWritable(i), new FloatWritable(Float.MAX_VALUE));
			}			
		}
	}

	@Override
	public void initStarter(InputPKVBuffer<IntWritable, FloatWritable> starter)
			throws IOException {
		starter.init(new IntWritable(startnode), new FloatWritable(0));
	}

	@Override
	public void iterate() {
		System.out.println((iter++) + " passes " + kvs + " activations");
	}
}
