package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;
;

public class PageRankReduce extends MapReduceBase implements
		IterativeReducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	
	private Map<Integer, Double> ranks = null;
	private int topk;
	private JobConf conf;

	//private String subRankDir;
	private String subGraphsDir;
	
	private Map<Integer, Integer> linkNum = new HashMap<Integer, Integer>();
	
	private int nPages;
	private int numSubGraph;
	private int snapshot_index = 0;
	private int workload = 0;
	private int addition = 0;
	private int iterate = 0;

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
		
		
		
		Path remote_link = new Path(subGraphsDir + "/" + n +"-linklist");
		try {
			FSDataInputStream in = hdfs.open(remote_link);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					String node = line.substring(0, index);
					
					String linkstring = line.substring(index+1);
					StringTokenizer st = new StringTokenizer(linkstring);
					int linknum = 0;
					while(st.hasMoreTokens()){
						st.nextToken();
						linknum++;
					}
					
					this.linkNum.put(Integer.parseInt(node), linknum);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void loadInitVec(JobConf job, int n) {
		ranks = new HashMap<Integer, Double>();
		/*
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(job);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(hdfs != null);
		
		subRankDir = job.get(MainDriver.SUBRANK_DIR);

		try {	       
			//read the subgraph file to memory from hdfs, usually locally
			String rankfilename = subRankDir + "/part-0000" + n;
			//System.out.println(linkfilename);
			FSDataInputStream rankFileIn = hdfs.open(new Path(rankfilename));
			BufferedReader rankReader = new BufferedReader(new InputStreamReader(rankFileIn));
			
			String line;
			while((line = rankReader.readLine()) != null){
				//System.out.println(line);
				int index = line.indexOf("\t");
				if(index != -1){
					int page = Integer.parseInt(line.substring(0, index));
					double rank = Double.parseDouble(line.substring(index+1));
					this.baseRank.put(page, rank);
				}
			}
			
			rankReader.close();
			rankFileIn.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		for(int i=n; i<nPages; i=i+numSubGraph){
			double initRank = 0.0;
			//if(i<100){
				initRank = 0.2;
			//}
			this.ranks.put(i, initRank);
		}
	}
	
	@Override
	public void configure(JobConf job) {
		ranks = new HashMap<Integer, Double>();
		
		topk = job.getInt(MainDriver.TOP_K, 0);
		nPages = job.getInt(MainDriver.PG_TOTAL_PAGES, 0);
		
		conf = job;

	    int ttnum = 0;
		try {
			JobClient jobclient = new JobClient(conf);
			ClusterStatus status = jobclient.getClusterStatus();
		    ttnum = status.getTaskTrackers();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    
		numSubGraph = ttnum;
		
		int n = PageRankMap.getTaskId(conf);
		//this.loadGraphToMem(job, n);
		this.loadInitVec(job, n);
	}
	
	@Override
	public void reduce(IntWritable key, Iterator<DoubleWritable> values,
			OutputPKVBuffer<DoubleWritable, IntWritable, DoubleWritable> output, Reporter report)
			throws IOException {
		workload++;
		
		synchronized(this.ranks){
			int page = key.get();
			
			double delta = 0.0;
			while(values.hasNext()){				
				delta += values.next().get();	
				addition++;
				//System.out.println("input: " + key + " : " + delta);
			}
			
			if(ranks.containsKey(page)){
				double baseRank = ranks.get(page);			
				ranks.put(page, baseRank + delta);
				//System.out.println(page + "\t" + baseRank + "\t" + delta);
			}else{
				System.out.println("wrong");
			}

			DoubleWritable priority = new DoubleWritable(delta);
			/*
			DoubleWritable priority = null;
			if(!this.linkNum.containsKey(page)){
				System.out.println("no " + page + " found!");
			}else{
				if (this.linkNum.get(page)==0){
					priority = new DoubleWritable(Double.MIN_VALUE);
				}else{
					
				}	
			}
			*/
			IntWritable outKey = new IntWritable(page);
			DoubleWritable outVal = new DoubleWritable(delta);
			output.collect(priority, outKey, outVal);
			
			
			//System.out.println(ranks.size());
			//System.out.println("output: " + priority + " : " + outKey + " : " + outVal);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized boolean snapshot(BufferedWriter writer) throws IOException{
		//System.out.println("snapshot topk is " + topk);
		snapshot_index++;
		//write snapshot
		synchronized(this.ranks){
			System.out.println("ranking set size: " + this.ranks.size());
			List<Integer> keys = new ArrayList<Integer>(this.ranks.keySet());
			 
			System.out.println("ranking key set size: " + keys.size() + "snapshot" + snapshot_index + " total reduce is " + workload);
			//Sort keys by values.
			final Map<Integer, Double> langForComp = this.ranks;
			Collections.sort(keys, 
				new Comparator(){
					public int compare(Object left, Object right){
						Integer leftKey = (Integer)left;
						Integer rightKey = (Integer)right;
	 
						Double leftValue = (Double)langForComp.get(leftKey);
						Double rightValue = (Double)langForComp.get(rightKey);
						return (leftValue == rightValue) ? 0 : (leftValue > rightValue) ? -1 : 1;
					}
				});
	 
			//List the key value
			//System.out.println("ranking key set size: " + keys.size());
			Iterator<Integer> itr =keys.iterator();
			/*
			for(int key : keys){
				System.out.println("keys: " + key);
			}
			*/
			int count = 0;
			while(itr.hasNext() && count<topk){
				Object k = itr.next();
				double rank = this.ranks.get(k);
				writer.write(rank + "\t" + k + "\t" + rank + "\n");
				count++;
			}
		}

		System.out.println("snapshot " + snapshot_index + " iterations " + iterate + " reduce " + workload + " addition " + addition);
		return true;
	}

	@Override
	public void reduce(IntWritable arg0, Iterator<DoubleWritable> arg1,
			OutputCollector<IntWritable, DoubleWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DoubleWritable setPriority(IntWritable key, DoubleWritable value) {
		double priority = 0.0;
		if(this.linkNum.containsKey(key.get())){
			priority = value.get() / this.linkNum.get(key.get());
		}

		return new DoubleWritable(priority);
	}

	/*
	@Override
	public PriorityRecord<DoubleWritable, DoubleWritable> iter(
			PriorityRecord<DoubleWritable, DoubleWritable> smallestRec) {
		return new PriorityRecord<DoubleWritable, DoubleWritable>(smallestRec.getPriority(), smallestRec.getValue());
	}
*/
	@Override
	public DoubleWritable bound(DoubleWritable min, DoubleWritable max) {
		if(max.get() == 0.0){
			return new DoubleWritable(0.05);
		}else{
			double bound = max.get() / 20;
			//if(bound < 0.000000001) bound = 0.000000001;
			System.out.println("max is " + max.get() + " min is " + min.get() + " bound is " + bound);
			return new DoubleWritable(bound);
		}
	}

	@Override
	public int bucketTransfer(int[] arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DoubleWritable[] bufSplit(DoubleWritable min, DoubleWritable max,
			boolean arg2) {
		min.set(Double.MAX_VALUE);
		max.set(0.0);
		return null;
	}

	@Override
	public void combine(DoubleWritable pri1, DoubleWritable value1,
			DoubleWritable pri2, DoubleWritable value2,
			PriorityRecord<DoubleWritable, DoubleWritable> output) {
		output.setValue(new DoubleWritable(value1.get() + value2.get()));
		output.setPriority(new DoubleWritable(pri1.get() + pri2.get()));
	}

	@Override
	public void defaultKV(IntWritable arg0, DoubleWritable arg1) {
		// TODO Auto-generated method stub
		arg0.set(0);
		arg1.set(0.0);
	}

	@Override
	public void iterate() {
		iterate++;
	}
}
