package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
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
	private Path graphLinks;
	private RandomAccessFile linkFileIn;
	private Path graphIndex;
	private FSDataInputStream indexFileIn;
	private BufferedReader indexReader;
	private HashMap<Integer, Long> subGraphIndex;
	private int lookupScale;
	private boolean inMem = true;
	private int kvs = 0;
	private int expand = 0;
	private int iter = 0;
	
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

		Path remote_link = new Path(subGraphsDir + "/subgraph" + n);
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

	private synchronized void loadGraphToDisk(JobConf conf, int n) {
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(hdfs != null);
		
		int ttnum = Util.getTTNum(job);
		subGraphsDir = conf.get(MainDriver.SUBGRAPH_DIR);		
		lookupScale = ttnum * MainDriver.INDEX_BLOCK_SIZE;
		
		if(subGraphIndex == null){
			//int n = getTaskId(conf);
			try {	       
				//read the subgraph file to memory from hdfs, usually locally
				Path remote_link = new Path(subGraphsDir + "/" + n +"-linklist");		
				Path remote_index = new Path(subGraphsDir + "/" + n +"-index");
				    
				String localLinks = n + "-linklist";
				graphLinks = new Path("/tmp/hadoop-yzhang/subgraph/" + localLinks);
				String localIndex = n + "-index";
				graphIndex = new Path("/tmp/hadoop-yzhang/subgraph/" + localIndex);
				
				FileSystem localFs = FileSystem.getLocal(conf);
		        Path graphDir = graphLinks.getParent();
		        if (localFs.exists(graphDir)){
		          localFs.delete(graphDir, true);
		          boolean b = localFs.mkdirs(graphDir);
		          if (!b)
		            throw new IOException("Not able to create job directory "
		                                  + graphDir.toString());
		        }
				
				if(localFs.exists(graphLinks)){
					localFs.delete(graphLinks, true);
				}
				
				if(localFs.exists(graphIndex)){
					localFs.delete(graphIndex, true);
				}
				
				hdfs.copyToLocalFile(remote_link, graphLinks);
				hdfs.copyToLocalFile(remote_index, graphIndex);
				
				//linkFileIn = localFs.open(graphLinks);
				linkFileIn = new RandomAccessFile(graphLinks.toString(), "r");
				indexFileIn = localFs.open(graphIndex);
				
				if((linkFileIn == null) || (indexFileIn == null)){
					throw new IOException("no index or file found");
				}
				
				indexReader = new BufferedReader(new InputStreamReader(indexFileIn));
				
				subGraphIndex = new HashMap<Integer, Long>();
				String line;
				while((line = indexReader.readLine()) != null){
					//System.out.println(line);
					int index = line.indexOf("\t");
					if(index != -1){
						String node = line.substring(0, index);
						long offset = Long.parseLong(line.substring(index+1));
						subGraphIndex.put(Integer.parseInt(node), offset);
						
						//System.out.println(node + "\t" + offset);
					}
					
				}
				
				indexReader.close();
				indexFileIn.close();
				
				System.out.println("load graph finished");
				//localFs.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	private ArrayList<Integer> getLinks(int node) throws Exception {
		Long offset = subGraphIndex.get(node / lookupScale);
		//System.out.println(node);
		//System.out.println(node / lookupScale);
							
		if(offset == null){
			System.out.println("no matched!!! for node " + node);
		}
		
		linkFileIn.seek(offset);	
		//System.out.println(offset);
		//linkReader = new BufferedReader(new InputStreamReader(linkFileIn));
		
		int ttnum = Util.getTTNum(job);
		int linenum = (node / ttnum) % MainDriver.INDEX_BLOCK_SIZE;
		//System.out.println(linenum);
		String record = new String();
		while(/*linkReader.ready() && */(linenum-- >= 0)){
			//record = linkReader.readLine();
			record = linkFileIn.readLine();
			//System.out.println(record);
		}
		//System.out.println(record);		
		//System.out.println(" index is " + offset + " and the record is " + record);
		
		int lindex = record.indexOf("\t");
		
		if(lindex == -1){
			throw new Exception("no \t, why? " + record);
		}
		
		if(node != Integer.parseInt(record.substring(0, lindex))){
			//not match
			throw new Exception(node + " not match in index file\n");
		}
		
		String linkstring = record.substring(lindex+1);
		ArrayList<Integer> links = new ArrayList<Integer>();
		StringTokenizer st = new StringTokenizer(linkstring);
		while(st.hasMoreTokens()){
			links.add(Integer.parseInt(st.nextToken()));
		}
		
		return links;
	}
	
	@Override
	public void configure(JobConf job) {
		this.job = job;
		int taskid = Util.getTaskId(job);
		inMem = job.getBoolean(MainDriver.IN_MEM, true);
		if(inMem){
			//load graph to memory
			this.loadGraphToMem(job, taskid);
		}else{
			//load graph to local disk
			loadGraphToDisk(job, taskid);
		}	
	}
	
	@Override
	public void initStarter(InputPKVBuffer<IntWritable, DoubleWritable> starter)
			throws IOException {	
		int ttnum = Util.getTTNum(job);		
		int n = getTaskId(job);
		for(int i=n; i<100; i=i+ttnum){
			starter.init(new IntWritable(i), new DoubleWritable(0.2));
		}
		
		//load from file
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
	}

	@Override
	public void map(IntWritable key, DoubleWritable value,
			OutputCollector<IntWritable, DoubleWritable> output, Reporter report)
			throws IOException {
				
		//System.out.println("input: " + key + " : " + value);
		kvs++;
		report.setStatus(String.valueOf(kvs));
		
		int page = key.get();
		ArrayList<Integer> links = null;
		if(inMem){
			//for in-memory graph
			links = this.linkList.get(key.get());
		}else{
			//for on-disk graph
			try {
				links = getLinks(page);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if(links == null){
			System.out.println("no links found for page " + page);
			//output.collect(new IntWritable(page), new DoubleWritable(value.get()));
			return;
		}	
		double delta = value.get() * PageRank.DAMPINGFAC / links.size();
		
		for(int link : links){
			output.collect(new IntWritable(link), new DoubleWritable(delta));
			expand++;
			//System.out.println("output: " + link + " : " + delta);
		}	
	}

	@Override
	public void iterate() {
		System.out.println("iter " + (iter++) + " workload is " + kvs);
	}
}
