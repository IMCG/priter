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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Activator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;

public class KatzActivator extends MapReduceBase implements
	Activator<IntWritable, FloatWritable, FloatWritable> {

	private String subGraphsDir;
	private int kvs = 0;
	private int iter = 0;
	private int partitions;
	private float beta;
	
	//graph in local memory
	private HashMap<Integer, ArrayList<Integer>> linkList = new HashMap<Integer, ArrayList<Integer>>();
	 
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
					ArrayList<Integer> links = new ArrayList<Integer>();
					StringTokenizer st = new StringTokenizer(linkstring);
					while(st.hasMoreTokens()){
						links.add(Integer.parseInt(st.nextToken()));
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
	public void configure(JobConf job) {
		int taskid = Util.getTaskId(job);
		partitions = job.getInt("priter.graph.partitions", 1);
		beta = job.getFloat(MainDriver.KATZ_BETA, (float)0.05);
		loadGraphToMem(job, taskid);
	}
	
	@Override
	public void initStarter(InputPKVBuffer<IntWritable, FloatWritable> starter)
			throws IOException {	
		starter.init(new IntWritable(0), new FloatWritable(1000000));
	}

	@Override
	public void activate(IntWritable key, FloatWritable value,
			OutputCollector<IntWritable, FloatWritable> output, Reporter report)
			throws IOException {
		kvs++;
		report.setStatus(String.valueOf(kvs));
		
		int page = key.get();
		ArrayList<Integer> links = null;
		links = this.linkList.get(key.get());

		if(links == null){
			System.out.println("no links found for node " + page);
			for(int i=0; i<partitions; i++){
				output.collect(new IntWritable(i), new FloatWritable(0));
			}
			return;
		}	
		float delta = value.get() * beta;
		
		for(int link : links){
			output.collect(new IntWritable(link), new FloatWritable(delta));
		}	
	}

	@Override
	public void iterate() {
		System.out.println((iter++) + " passes " + kvs + " activations");
	}
}
