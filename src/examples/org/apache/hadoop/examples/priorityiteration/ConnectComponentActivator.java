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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Activator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;


public class ConnectComponentActivator extends MapReduceBase implements
		Activator<IntWritable, IntWritable> {

	private JobConf job;

	private String subGraphsDir;
	private int kvs = 0;
	private int iter = 0;
	private int nNodes;
	private int partitions;
	
	//graph in local memory
	private HashMap<Integer, ArrayList<Integer>> linkList = new HashMap<Integer, ArrayList<Integer>>();

	
	@Override
	public void configure(JobConf job) {
		this.job = job;
		int taskid = Util.getTaskId(job);
		nNodes = job.getInt("priter.graph.nodes", 0);
		partitions = job.getInt("priter.graph.partitions", 0);
		loadGraphToMem(job, taskid);
	}
	
	private synchronized void loadGraphToMem(JobConf conf, int n){
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(hdfs != null);
		
		subGraphsDir = conf.get(MainDriver.SUBGRAPH_DIR);

		Path remote_link = new Path(subGraphsDir + "/part" + n);
		try {
			FSDataInputStream in = hdfs.open(remote_link);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					String node = line.substring(0, index);
					
					String linkstring = line.substring(index+1);
					ArrayList<Integer> links = new ArrayList<Integer>();
					StringTokenizer st = new StringTokenizer(linkstring);
					while(st.hasMoreTokens()){
						links.add(Integer.parseInt(st.nextToken()));
					}
					
					this.linkList.put(Integer.parseInt(node), links);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void activate(IntWritable key, IntWritable value,
			OutputCollector<IntWritable, IntWritable> output, Reporter report)
			throws IOException {
		int node = key.get();
		if(linkList.get(node) == null) return;
		for(int linkend : linkList.get(node)){
			output.collect(new IntWritable(linkend), value);
		}
		kvs++;
		report.setStatus(String.valueOf(kvs));
	}

	@Override
	public void initStarter(InputPKVBuffer<IntWritable> starter)
			throws IOException {
		int n = Util.getTaskId(job);
		for(int i=n; i<nNodes; i=i+partitions){
			starter.init(new IntWritable(i), new IntWritable(i));
		}
	}

	@Override
	public void iterate() {
		iter++;
		System.out.println("iteration " + iter + " total parsed " + kvs);
	}

}
