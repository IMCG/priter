package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.Activator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;

public class PWClusterActivator extends MapReduceBase implements
	Activator<IntWritable, DoubleWritable, ClusterWritable> {

	private JobConf job;

	private String subNodessDir;
	private int kvs = 0;

	private int iter = 0;
	private double initvalue = 2;
	private int partitions;
	//private OutputCollector<IntWritable, DoubleWritable> outCollector;
	
	private HashMap<Integer, Integer> nodes = new HashMap<Integer, Integer>();
	//graph in local memory
	
	private synchronized void loadGraphToMem(JobConf conf, int n){
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(hdfs != null);
		
		subNodessDir = conf.get("pwcluster.subnodes");

		Path remote_link = new Path(subNodessDir + "/part" + n);
		try {
			FSDataInputStream in = hdfs.open(remote_link);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					String node = line.substring(0, index);
					
					String cluster = line.substring(index+1);
					
					this.nodes.put(Integer.parseInt(node), Integer.parseInt(cluster));
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void configure(JobConf job) {
		this.job = job;
		partitions = job.getInt("priter.graph.partitions", 1);
		int taskid = Util.getTaskId(job);
		loadGraphToMem(job, taskid);
	}
	
	@Override
	public void initStarter(InputPKVBuffer<IntWritable, ClusterWritable> starter)
			throws IOException {	
		int n = Util.getTaskId(job);
		
		ClusterWritable iv = new ClusterWritable(n, 0, 0.0);
		ClusterWritable iv1 = new ClusterWritable(n, 1, 0.0);
		
		starter.init(new IntWritable(0), iv);
		starter.init(new IntWritable(0), iv1);
	}

	@Override
	public void activate(IntWritable key, ClusterWritable value,
			OutputCollector<IntWritable, ClusterWritable> output, Reporter report)
			throws IOException {
		
		//if(outCollector == null) outCollector = output;
		
		kvs++;
		report.setStatus(String.valueOf(kvs));
		
		int nodeid = 0;
		int clusterid = value.getClusterid();
		
		if(clusterid >= 0)
		{
		
			nodeid = value.getNodeid();
			int oldc = -1;
			if(nodes.get(nodeid) != null)
				oldc = nodes.get(nodeid);
		
			if(oldc != clusterid && oldc != -1)
			{
				System.out.println("move node " + nodeid);
				
				nodes.put(nodeid, clusterid); //update the's cluster
				
				double newvalue = 0.0;
				
				ClusterWritable cw = new ClusterWritable(nodeid, clusterid, newvalue);
				
				System.out.println("send value: " + newvalue);
				
				for(int i=0; i<partitions; i++){
					output.collect(new IntWritable(i), cw);
				}
			}
		}
	}
	
	/*
	@Override
	public void close() throws IOException {
			
		System.out.println("call close method");
		for(int i=0; i<partitions; i++)
			outCollector.collect(new IntWritable(i), new DoubleWritable(-10.0));		
	}*/

	@Override
	public void iterate() {
		System.out.println("iter " + (iter++) + " workload is " + kvs);
	}
}

