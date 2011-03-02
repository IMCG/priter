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
import org.apache.hadoop.mapred.IterativeMapper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;


public class AveragingMap extends MapReduceBase implements
		IterativeMapper<DoubleWritable, IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

	private JobConf job;

	private String subGraphsDir;
	private boolean inMem = true;
	private int kvs = 0;
	private int expand = 0;
	private int iter = 0;
	private int nNodes;
	private int startNodes;
	private double initvalue;
	
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
	 
	private synchronized void loadGraphToMem(JobConf conf, int n) throws IOException{
		FileSystem hdfs = null;
		hdfs = FileSystem.get(conf);

		assert(hdfs != null);
		
		subGraphsDir = conf.get(MainDriver.SUBGRAPH_DIR);

		Path remote_link = new Path(subGraphsDir + "/part-" + n);
		
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
	}

	@Override
	public void configure(JobConf job) {
		this.job = job;
		int taskid = Util.getTaskId(job);
		try {
			this.loadGraphToMem(job, taskid);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		nNodes = job.getInt("priter.graph.nodes", 0);
		startNodes = job.getInt(MainDriver.START_NODE, nNodes);
		initvalue = Averaging.RETAINFAC * nNodes / startNodes;
	}
	
	@Override
	public void initStarter(InputPKVBuffer<IntWritable, DoubleWritable> starter)
			throws IOException {	
		int ttnum = Util.getTTNum(job);		
		int n = Util.getTaskId(job);
		for(int i=n; i<startNodes; i=i+ttnum){
			starter.init(new IntWritable(i), new DoubleWritable(initvalue));
		}
	}

	@Override
	public void map(IntWritable key, DoubleWritable value,
			OutputCollector<IntWritable, DoubleWritable> output, Reporter report)
			throws IOException {
			
		kvs++;
		report.setStatus(String.valueOf(kvs));
		
		int page = key.get();
		ArrayList<LinkWeight> links = null;
		if(inMem){
			//for in-memory graph
			links = this.linkList.get(key.get());
		}

		if(links == null){
			System.out.println("no links found for page " + page);
			return;
		}	
		double delta = value.get() * Averaging.DAMPINGFAC;
		
		for(LinkWeight link : links){
			output.collect(new IntWritable(link.link), new DoubleWritable(delta*link.weight));
			expand++;
		}	
	}

	@Override
	public void iterate() {
		System.out.println("iter " + (iter++) + " workload is " + kvs + " total expand is " + expand);
	}
}
