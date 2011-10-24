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


public class AdsorptionActivator extends MapReduceBase implements
		Activator<IntWritable, DoubleWritable, DoubleWritable> {

	private String subGraphsDir;
	private int kvs = 0;
	private int iter = 0;
	private int partitions;
	
	class LinkWeight {
		public int link;
		public double weight;
		
		public LinkWeight(int link, double weight){
			this.link = link;
			this.weight = weight;
		}
	}
	//graph in local memory
	private HashMap<Integer, ArrayList<LinkWeight>> linkList = new HashMap<Integer, ArrayList<LinkWeight>>();
	 
	private synchronized void loadGraphToMem(JobConf conf, int n) {
		subGraphsDir = conf.get(MainDriver.SUBGRAPH_DIR);
		Path remote_link = new Path(subGraphsDir + "/part-" + n);
		
		FileSystem hdfs = null;
		try{
			hdfs = FileSystem.get(conf);
			FSDataInputStream in = hdfs.open(remote_link);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					String node = line.substring(0, index);
					int index2 = line.indexOf(":");
					if(index2 != -1){
						String linkstring = line.substring(index2+1);
						ArrayList<LinkWeight> links = new ArrayList<LinkWeight>();
						StringTokenizer st = new StringTokenizer(linkstring);
						while(st.hasMoreTokens()){
							String[] field = st.nextToken().split(",");
							int linkend = Integer.parseInt(field[0]);
							double weight = Double.parseDouble(field[1]);
							LinkWeight linkweight = new LinkWeight(linkend, weight);
							links.add(linkweight);
						}
						
						this.linkList.put(Integer.parseInt(node), links);
					}		
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void configure(JobConf job) {
		int taskid = Util.getTaskId(job);
		this.loadGraphToMem(job, taskid);
		partitions = job.getInt("priter.graph.partitions", 1);
	}
	
	@Override
	public void initStarter(InputPKVBuffer<IntWritable, DoubleWritable> starter)
			throws IOException {
		double initvalue = Adsorption.RETAINFAC * 10000;
		int count = 0;
		for(int k : linkList.keySet()){
			if(++count > 25) break;
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
		ArrayList<LinkWeight> links = null;
		links = this.linkList.get(key.get());

		if(links == null){
			System.out.println("no links found for page " + page);
			for(int i=0; i<partitions; i++){
				output.collect(new IntWritable(i), new DoubleWritable(0));
			}
			return;
		}	
		double delta = value.get() * Adsorption.DAMPINGFAC;
		
		for(LinkWeight link : links){
			output.collect(new IntWritable(link.link), new DoubleWritable(delta*link.weight));
		}	
	}

	@Override
	public void iterate() {
		System.out.println((iter++) + " passes " + kvs + " activations");
	}
}
