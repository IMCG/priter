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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Activator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;

public class PageRankActivator extends MapReduceBase implements
	Activator<DoubleWritable, DoubleWritable> {

	private String subGraphsDir;
	private int iter = 0;
	private int kvs = 0;				//for tracking
	private double initvalue;
	private int partitions;
	
	//graph in local memory
	private HashMap<Integer, ArrayList<Integer>> linkList = new HashMap<Integer, ArrayList<Integer>>();
	
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
	public void configure(JobConf job) {
		int taskid = Util.getTaskId(job);
		initvalue = PageRank.RETAINFAC;
		partitions = job.getInt("priter.graph.partitions", 1);
		loadGraphToMem(job, taskid);
	}
	
	@Override
	public void initStarter(InputPKVBuffer<DoubleWritable> starter) throws IOException {	
		for(int k : linkList.keySet()){
			starter.init(new IntWritable(k), new DoubleWritable(initvalue));
		}
	}

	@Override
	public void activate(IntWritable key, DoubleWritable value,
			OutputCollector<IntWritable, DoubleWritable> output, Reporter report)
			throws IOException {
		kvs++;
		report.setStatus(String.valueOf(kvs));
		
		int page = key.get();
		ArrayList<Integer> links = null;
		links = this.linkList.get(key.get());

		if(links == null){
			System.out.println("no links found for page " + page);
			for(int i=0; i<partitions; i++){
				output.collect(new IntWritable(i), new DoubleWritable(0.0));
			}
			return;
		}	
		double delta = value.get() * PageRank.DAMPINGFAC / links.size();
		
		for(int link : links){
			output.collect(new IntWritable(link), new DoubleWritable(delta));
		}	
	}

	@Override
	public void iterate() {
		System.out.println((iter++) + " passes " + kvs + " activations");
	}
}
