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

public class HittingTimeActivator extends MapReduceBase implements Activator<DoubleWritable, DoubleWritable> {
	private String subGraphsDir;
	private int partitions;
	private int startnode;
	private int workload = 0;
	private int addition = 0;
	
	//graph in local memory
	private HashMap<Integer, ArrayList<Link>> linkList = new HashMap<Integer, ArrayList<Link>>();

	private class Link{
		int node;
		double weight;
		
		public Link(int n, double w){
			node = n;
			weight = w;
		}
		
		@Override
		public String toString() {
			return new String(node + "\t" + weight);
		}
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
		//numSubGraph = MainDriver.MACHINE_NUM;		
		
		Path remote_link = new Path(subGraphsDir + "/part" + n);
		//System.out.println(remote_link);
		try {
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
						//System.out.println(node + "\t" + link);
						String item[] = link.split(",");
						Link l = new Link(Integer.parseInt(item[0]), Double.parseDouble(item[1]));
						links.add(l);
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
	public void configure(JobConf job){   
	    startnode = job.getInt(MainDriver.START_NODE, 0);
	    int taskid = Util.getTaskId(job);
	    partitions = job.getInt("priter.graph.partitions", -1);
		this.loadGraphToMem(job, taskid);
	}
	
	@Override
	public void activate(IntWritable key, DoubleWritable value,
			OutputCollector<IntWritable, DoubleWritable> output, Reporter report)
			throws IOException {
		report.setStatus(String.valueOf(workload) + ":" + addition);
			
		int node = key.get();
		ArrayList<Link> links = null;
		
		links = this.linkList.get(node);
		
		//System.out.println(node + "\t" + links);
		
		double distance = value.get();
		
		if(links == null) {
			//System.out.println("no links for node " + node);
			for(int i=0; i<partitions; i++){
				output.collect(new IntWritable(i), new DoubleWritable(0.0));
			}
			return;
		}
		
		for(Link l : links){				
			addition++;
			output.collect(new IntWritable(l.node), new DoubleWritable(distance*l.weight));
			report.setStatus(String.valueOf(workload) + ":" + addition);
			//System.out.println("output " + l.node + "\t" + ((distance+1.0)*l.weight));
		}
		workload++;
	}

	@Override
	public void initStarter(InputPKVBuffer<DoubleWritable> starter)
			throws IOException {
		starter.init(new IntWritable(startnode), new DoubleWritable(0.0));
	}

	@Override
	public void iterate() {
		//System.out.println("iter " + (iter++) + " workload is " + workload + " addition is " + addition);
	}
}
