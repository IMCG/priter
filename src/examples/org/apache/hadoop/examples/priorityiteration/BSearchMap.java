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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.IterativeMapper;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;


//input <node, shortest length and point to list>
//output <node, shortest length>
public class BSearchMap extends MapReduceBase implements IterativeMapper<IntWritable, IntWritable, IntWritable, IntWritable, IntWritable> {
	private JobConf conf;
	
	private String subGraphsDir;
	private Path graphLinks;
	private RandomAccessFile linkFileIn;
	private Path graphIndex;
	private FSDataInputStream indexFileIn;
	private BufferedReader indexReader;
	private HashMap<Integer, Long> subGraphIndex;
	private int numSubGraph;
	private int lookupScale;
	private int startnode;
	private int workload = 0;
	private int addition = 0;
	private int iter = 0;
	
	//graph in local memory
	private HashMap<Integer, ArrayList<Link>> linkList = new HashMap<Integer, ArrayList<Link>>();

	private class Link{
		int node;
		int weight;
		
		public Link(int n, int w){
			node = n;
			weight = w;
		}
		
		@Override
		public String toString() {
			return new String(node + "\t" + weight);
		}
	}
	
	private static int getTaskId(JobConf conf) throws IllegalArgumentException
	{
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

	private synchronized void loadGraphToDisk(JobConf conf, int n) {
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
		
		lookupScale = numSubGraph * MainDriver.INDEX_BLOCK_SIZE;
		//System.out.println(numSubGraph);
		
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
				localFs.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
		
		Path remote_link = new Path(subGraphsDir + "/subgraph" + n);
		System.out.println(remote_link);
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
						//System.out.println(link);
						String item[] = link.split(",");
						Link l = new Link(Integer.parseInt(item[0]), Integer.parseInt(item[1]));
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
	
	private ArrayList<String> getLinks(int node) throws Exception {
		Long offset = subGraphIndex.get(node / lookupScale);
		//System.out.println(node);
		//System.out.println(node / lookupScale);
							
		if(offset == null){
			System.out.println("no matched!!! for node " + node);
			return null;
		}
		
		linkFileIn.seek(offset);	
		//System.out.println(offset);
		//linkReader = new BufferedReader(new InputStreamReader(linkFileIn));
		
		int linenum = (node / numSubGraph) % MainDriver.INDEX_BLOCK_SIZE;
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
			System.out.println("no \t, why? " + record);
			return null;
		}
		
		if(node != Integer.parseInt(record.substring(0, lindex))){
			//not match
			//throw new Exception(node + " not match in index file\n");
			System.out.println(node + " not match in index file\n");
			return null;
		}
		
		String linkstring = record.substring(lindex+1);
		ArrayList<String> links = new ArrayList<String>();
		StringTokenizer st = new StringTokenizer(linkstring);
		while(st.hasMoreTokens()){
			links.add(st.nextToken());
		}
		
		return links;
	}
	
	@Override
	public void configure(JobConf job){    
	    this.conf = job;
	    startnode = job.getInt(MainDriver.SP_START_NODE, 0);
	    
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
		
		//for shortest path, we load graph on local disk
		int n = getTaskId(conf);
		//loadGraphToDisk(conf, n);
		loadGraphToMem(conf, n);
	}
	
	//format node	f:len
	//       node	v:shortest_length
	@Override
	public void map(IntWritable key, IntWritable value,
			OutputCollector<IntWritable, IntWritable> output, Reporter report)
			throws IOException {
		//System.out.println("input: " + key + " :" + value);	
		report.setStatus(String.valueOf(workload) + ":" + addition);
			
		int node = key.get();
		int distance = value.get();
		
		if(distance != Integer.MAX_VALUE){	
			/*for disk graph
			ArrayList<String> links = null;
			try {
				links = getLinks(nnode);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
			
			if(links == null) return;
						
			//output.collect(key, new Text(v));
			
			for(String linkstr : links){
				int index2 = linkstr.indexOf(",");
				int node2 = Integer.parseInt(linkstr.substring(0, index2));
				int length = Integer.parseInt(linkstr.substring(index2+1));
				
				String f = "f:" + String.valueOf(base_len+length);
				output.collect(new IntWritable(node2), new Text(f));
				
				//System.out.println(node2 + " : " + f);
			}
			*/
			
			//for memory graph
			ArrayList<Link> links = this.linkList.get(node);
			if(links == null) {
				for(int i=0; i<numSubGraph; i++){
					output.collect(new IntWritable(i), new IntWritable(Integer.MAX_VALUE));
				}
				return;
			}
			
			workload++;
			
			for(Link l : links){				
				addition++;
				output.collect(new IntWritable(l.node), new IntWritable(distance + l.weight));
				report.setStatus(String.valueOf(workload) + ":" + addition);
			}
			
			//System.out.println("iter " + (iter++) + " workload is " + workload + " addition is " + addition);
		} else{
			//triger reduce to run
			for(int i=0; i<numSubGraph; i++){
				output.collect(new IntWritable(i), new IntWritable(Integer.MAX_VALUE));
			}			
		}
	}

	@Override
	public void initStarter(InputPKVBuffer<IntWritable, IntWritable> starter)
			throws IOException {
		starter.init(new IntWritable(startnode), new IntWritable(0));
	}

	@Override
	public void iterate() {
		// TODO Auto-generated method stub
		System.out.println("iter " + (iter++) + " workload is " + workload + " addition is " + addition);
	}
}
