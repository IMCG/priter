package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class PrIterBase implements JobConfigurable {

	public final static String SUBGRAPHDIR = "priter.subgraph.dir";
	
	private String subGraphsDir;
	
	//graph in local memory
	private HashMap<Integer, ArrayList<Integer>> unweightedLinkList = new HashMap<Integer, ArrayList<Integer>>();
	private HashMap<Integer, ArrayList<Link>> weightedLinkList = new HashMap<Integer, ArrayList<Link>>();

	protected class Link{
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

	protected synchronized void loadUnweightedGraph(JobConf conf){
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(hdfs != null);
		
		subGraphsDir = conf.get(SUBGRAPHDIR);

		int taskid = getTaskId(conf);
		Path remote_link = new Path(subGraphsDir + "/part" + taskid);
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
					
					unweightedLinkList.put(Integer.parseInt(node), links);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected synchronized void loadWeightedGraph(JobConf conf){
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(hdfs != null);
		
		subGraphsDir = conf.get(SUBGRAPHDIR);

		int taskid = getTaskId(conf);
		Path remote_link = new Path(subGraphsDir + "/part" + taskid);
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

					weightedLinkList.put(Integer.parseInt(node), links);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected HashMap<Integer, ArrayList<Link>> getWeightedGraph(){
		return this.weightedLinkList;
	}
	
	protected HashMap<Integer, ArrayList<Integer>> getUnweightedGraph(){
		return this.unweightedLinkList;
	}
	
	protected static int getTaskId(JobConf conf) throws IllegalArgumentException{
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
	
	protected static int getTTNum(JobConf job) {
	    int ttnum = 0;
		try {
			JobClient jobclient = new JobClient(job);
			ClusterStatus status = jobclient.getClusterStatus();
		    ttnum = status.getTaskTrackers();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		return ttnum;
	}
	
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub

	}

}
