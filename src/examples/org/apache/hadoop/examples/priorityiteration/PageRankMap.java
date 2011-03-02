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

public class PageRankMap extends MapReduceBase implements
		IterativeMapper<DoubleWritable, IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

	private JobConf job;

	private String subGraphsDir;
	private int kvs = 0;
	private int expand = 0;
	private int iter = 0;
	private int nPages;
	private int startPages;
	private double initvalue;
	private int partitions;
	
	//graph in local memory
	private HashMap<Integer, ArrayList<Integer>> linkList = new HashMap<Integer, ArrayList<Integer>>();
	
	public static int getTaskId(JobConf conf) throws IllegalArgumentException{
       if (conf == null) {
           throw new NullPointerException("conf is null");
       }

       String taskId = conf.get("mapred.task.id");
       if (taskId == null) {
    	   throw new IllegalArgumentException("Configutaion does not contain the property mapred.task.id");
       }

       String[] parts = taskId.split("_");
       if (parts.length != 6 ||
    		   !parts[0].equals("attempt") ||
    		   (!"m".equals(parts[3]) && !"r".equals(parts[3]))) {
    	   throw new IllegalArgumentException("TaskAttemptId string : " + taskId + " is not properly formed");
       }

       return Integer.parseInt(parts[4]);
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
	public void configure(JobConf job) {
		this.job = job;
		int taskid = Util.getTaskId(job);
		nPages = job.getInt("priter.graph.nodes", 0);
		startPages = job.getInt(MainDriver.START_NODE, nPages);
		initvalue = PageRank.RETAINFAC * nPages / startPages;
		partitions = job.getInt("mapred.iterative.partitions", 1);
		loadGraphToMem(job, taskid);
	}
	
	@Override
	public void initStarter(InputPKVBuffer<IntWritable, DoubleWritable> starter)
			throws IOException {	
		int n = getTaskId(job);
		for(int i=n; i<startPages; i=i+partitions){
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
			expand++;
		}	
	}

	@Override
	public void iterate() {
		System.out.println("iter " + (iter++) + " workload is " + kvs);
	}
}
