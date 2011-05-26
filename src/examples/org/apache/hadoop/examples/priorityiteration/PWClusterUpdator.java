package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Updator;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;

public class PWClusterUpdator extends MapReduceBase implements
		Updator<DoubleWritable, ClusterWritable> {
	private int reduce = 0;
	private int iterate = 0;
	private String subNodessDir;
	private String mainGraph;
	private String sciTablefile;
	private String sclusterTablefile;
	
	//OutputPKVBuffer<DoubleWritable, DoubleWritable> outbuffer;
	
	private HashMap<Integer, HashMap<Integer, Float>> linkList = new HashMap<Integer, HashMap<Integer, Float>>();
	
	private HashMap<Integer, Integer> nodes = new HashMap<Integer, Integer>();
	private HashMap<Integer, Integer> clusters = new HashMap<Integer, Integer>();
	
	private HashMap<Integer, HashMap<Integer, Float>> sciTable = new HashMap<Integer, HashMap<Integer, Float>>();
	private HashMap<Integer, Float> sclusterTable = new HashMap<Integer, Float>();
	
	private synchronized void loadNodeToMem(JobConf conf, int n){
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(hdfs != null);
		
		subNodessDir = conf.get("pwcluster.subnodes");

		Path remote_link = new Path(subNodessDir + "/part" + n);
		try {
			FSDataInputStream in = hdfs.open(remote_link);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					String node = line.substring(0, index);
					
					String cluster = line.substring(index+1);
					
					this.nodes.put(Integer.parseInt(node), Integer.parseInt(cluster));
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private synchronized void loadGraphToMem(JobConf conf){
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(hdfs != null);
		
		//numSubGraph = MainDriver.MACHINE_NUM;
		
		mainGraph = conf.get("pwcluster.mainGraph");
		
		Path remote_link = new Path(mainGraph);
		System.out.println(remote_link);
		try {
			FSDataInputStream in = hdfs.open(remote_link);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			//int cc2 = 0;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					String node = line.substring(0, index);
					
					int node_id = Integer.parseInt(node);
					
					if(this.nodes.get(node_id) != null) // the nodes in this worker contain node_id
					{
						//cc2++;
						//System.out.println("this worker contains node " + node_id);
						String linkstring = line.substring(index+1);
						HashMap<Integer, Float> links = new HashMap<Integer, Float>();
						StringTokenizer st = new StringTokenizer(linkstring);
						while(st.hasMoreTokens()){
							String link = st.nextToken();
							//System.out.println(link);
							String item[] = link.split(",");
							int targetnode_id = Integer.parseInt(item[0]);
							float value = Float.parseFloat(item[1]);
							if(value>0)
								links.put(targetnode_id, value);
							//System.out.println("this worker contains similarity between node " + node_id + " and " + targetnode_id);
						}
						this.linkList.put(node_id, links);
					}
				}
			}
			//System.out.println("this worker totally contains nodes: " + cc2);
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void initsciTable(JobConf conf)
	{
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(hdfs != null);
		
		//numSubGraph = MainDriver.MACHINE_NUM;
		
		sciTablefile = conf.get("pwcluster.sciTablefile");
		
		Path remote_link = new Path(sciTablefile+"/part-00000");
		System.out.println(remote_link);
		try {
			FSDataInputStream in = hdfs.open(remote_link);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			//int cc2 = 0;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					String node = line.substring(0, index);
					
					int node_id = Integer.parseInt(node);
					
					if(this.nodes.get(node_id) != null) // the nodes in this worker contain node_id
					{
						//cc2++;
						//System.out.println("this worker contains node " + node_id);
						String linkstring = line.substring(index+1);
						HashMap<Integer, Float> links = new HashMap<Integer, Float>();
						StringTokenizer st = new StringTokenizer(linkstring);
						while(st.hasMoreTokens()){
							String link = st.nextToken();
							//System.out.println(link);
							String item[] = link.split(",");
							int targetnode_id = Integer.parseInt(item[0]);
							links.put(targetnode_id, Float.parseFloat(item[1]));
							//System.out.println("this worker contains similarity between node " + node_id + " and " + targetnode_id);
						}
						this.sciTable.put(node_id, links);
					}
				}
			}
			//System.out.println("this worker totally contains nodes: " + cc2);
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void initsclusterTable(JobConf conf)
	{
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(hdfs != null);
		
		//numSubGraph = MainDriver.MACHINE_NUM;
		
		sclusterTablefile = conf.get("pwcluster.sclusterTablefile");
		
		Path remote_link = new Path(sclusterTablefile+"/part-00000");
		System.out.println(remote_link);
		try {
			FSDataInputStream in = hdfs.open(remote_link);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			//int cc2 = 0;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					String node = line.substring(0, index);
					int node_id = Integer.parseInt(node);
					String linkstring = line.substring(index+1);
					String item[] = linkstring.split(",");

					this.clusters.put(node_id, Integer.parseInt(item[0]));
					this.sclusterTable.put(node_id, Float.parseFloat(item[1]));
					
				}
			}
			//System.out.println("this worker totally contains nodes: " + cc2);
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private float incrAddsci(int vc, int vt, int vi)
	{
		float value = 0;
		if(this.clusters.get(vc) == null) // there is no items in cluster vc
			return 0;
		float v1 = 0;
		float v2 = 0;
		int itemnum = this.clusters.get(vc);
		
		HashMap<Integer, Float> row = this.sciTable.get(vi);
		if(row == null)
			v1 = 0;
		else
		{
			if(row.get(vc) == null)
			{
				v1 = 0;
			}
			else
			{
				v1 = row.get(vc);
			}
		}
		
		HashMap<Integer, Float> lists = this.linkList.get(vi);
		if(lists == null)
			v2 = 0;
		else
		{
			if(lists.get(vt) == null)
			{
				v2 = 0;
				//System.out.println("similarity between " + i + " and " + j + " is zero.");
			}
			else
			{
				v2 = lists.get(vt);
				//System.out.println("similarity between " + i + " and " + j + " is not zero.");
			}
		}
		
		value = (itemnum*v1+v2)/(itemnum+1);
		//workload++;
		return value;
	}
	
	private float incrRemovesci(int vc, int vt, int vi)
	{
		float value = 0;
		if(this.clusters.get(vc) == null || this.clusters.get(vc) == 1) // there is no items in cluster vc
			return 0;
		float v1 = 0;
		float v2 = 0;
		int itemnum = this.clusters.get(vc);
		
		HashMap<Integer, Float> row = this.sciTable.get(vi);
		if(row == null)
			v1 = 0;
		else
		{
			if(row.get(vc) == null)
			{
				v1 = 0;
			}
			else
			{
				v1 = row.get(vc);
			}
		}
		
		HashMap<Integer, Float> lists = this.linkList.get(vi);
		if(lists == null)
			v2 = 0;
		else
		{
			if(lists.get(vt) == null)
			{
				v2 = 0;
				//System.out.println("similarity between " + i + " and " + j + " is zero.");
			}
			else
			{
				v2 = lists.get(vt);
				//System.out.println("similarity between " + i + " and " + j + " is not zero.");
			}
		}
		value = (itemnum*v1-v2)/(itemnum-1);
		//workload++;
		return value;
	}
	
	private float incrAddsc(int vc, int vt)
	{
		float value = 0;
		if(this.clusters.get(vc) == null) // there is no items in cluster vc
			return 0;
		float v1 = 0;
		float v2 = 0;
		int itemnum = this.clusters.get(vc);
		if(this.sclusterTable.get(vc) == null)
			v1 = 0;
		else
			v1 = this.sclusterTable.get(vc);
		
		HashMap<Integer, Float> row = this.sciTable.get(vt);
		if(row == null)
			v2 = 0;
		else
		{
			if(row.get(vc) == null)
			{
				v2 = 0;
			}
			else
			{
				v2 = row.get(vc);
			}
		}
		
		value = (itemnum*itemnum*v1 + 2*itemnum*v2)/((itemnum+1)*(itemnum+1));
		//workload++;
		return value;
	}
	
	private float incrRemovesc(int vc, int vt)
	{
		float value = 0;
		if(this.clusters.get(vc) == null || this.clusters.get(vc) == 1) // there is no items in cluster vc
			return 0;
		float v1 = 0;
		float v2 = 0;
		int itemnum = this.clusters.get(vc);
		if(this.sclusterTable.get(vc) == null)
			v1 = 0;
		else
			v1 = this.sclusterTable.get(vc);
		
		HashMap<Integer, Float> row = this.sciTable.get(vt);
		if(row == null)
			v2 = 0;
		else
		{
			if(row.get(vc) == null)
			{
				v2 = 0;
			}
			else
			{
				v2 = row.get(vc);
			}
		}
		
		value = (itemnum*itemnum*v1 - 2*itemnum*v2)/((itemnum-1)*(itemnum-1));
		//workload++;
		return value;
	}
	
	private float deltas(int vc, int vi)
	{
		float value = 0;
		if(this.clusters.get(vc) == null) // there is no items in cluster vc
			return 0;
		float v1 = 0;
		float v2 = 0;
		int itemnum = this.clusters.get(vc);
		//System.out.println("s(c" + vc + "," + vi + ")=" + v1);
		//System.out.println("s(c" + vc + ")=" + v2);
		
		if(this.nodes.get(vi) != vc)
		{
			HashMap<Integer, Float> row = this.sciTable.get(vi);
			if(row == null)
				v1 = 0;
			else
			{
				if(row.get(vc) == null)
				{
					v1 = 0;
					//System.out.println("similarity between " + i + " and " + j + " is zero.");
				}
				else
				{
					v1 = row.get(vc);
					//System.out.println("similarity between " + i + " and " + j + " is not zero.");
				}
			}
			
			if(this.sclusterTable.get(vc) == null)
				v2 = 0;
			else
				v2 = this.sclusterTable.get(vc);
			
			value = 2*v1 - v2;
			value = value*itemnum/(itemnum+1);
		}
		else
		{
			if(this.clusters.get(vc) == 1)
				return 0;
			v1 = incrRemovesci(vc, vi, vi);
			v2 = incrRemovesc(vc, vi);
			value = 2*v1 - v2;
			value = value*(itemnum-1)/itemnum;
		}
		return value;
	}
	
	private void updatesciTable(int oldc, int newc, int vi)
	{
		List<Integer> keys = new ArrayList<Integer>(nodes.keySet());
		
		for(int i : keys)
		{
			HashMap<Integer, Float> lists = this.sciTable.get(i);
			if(lists != null)
			{
				float v1 = incrRemovesci(oldc, vi, i);
				lists.put(oldc, v1);
				float v2 = incrAddsci(newc, vi, i);
				lists.put(newc, v2);
			}
			
		}
	}
	
	private void updatesclusterTable(int oldc, int newc, int vi)
	{
		float v1 = incrRemovesc(oldc, vi);
		this.sclusterTable.put(oldc, v1);
		float v2 =  incrAddsc(newc, vi);
		this.sclusterTable.put(newc, v2);
	}
	
	@Override
	public void iterate() {
		iterate++;
	}
	
	@Override
	public ClusterWritable resetiState() {
		return new ClusterWritable(0, 0, -1.0);
	}
	

	@Override
	public void initStateTable(
			OutputPKVBuffer<DoubleWritable, ClusterWritable> stateTable) {
		//init it
		/*
		List<Integer> keys = new ArrayList<Integer>(nodes.keySet());
		
		for(int i : keys)
		{
			System.out.println("init entry " + i);
			stateTable.init(new IntWritable(i), new DoubleWritable(-1.0), new DoubleWritable(-1.0));
		}*/
	}

	@Override
	public DoubleWritable decidePriority(IntWritable key, ClusterWritable arg0, boolean iorc) {
			double value = arg0.getAddvalue();
				return new DoubleWritable(value);
	}

	@Override
	public void configure(JobConf job) {		
		 int taskid = Util.getTaskId(job);
		 this.loadNodeToMem(job, taskid);
		 this.loadGraphToMem(job);
		 this.initsciTable(job);
		 this.initsclusterTable(job);
	}

	@Override
	public void updateState(IntWritable key, Iterator<ClusterWritable> values,
			OutputPKVBuffer<DoubleWritable, ClusterWritable> buffer, Reporter report)
			throws IOException {
	
		reduce++;	
		report.setStatus(String.valueOf(reduce));
								
		ClusterWritable clusterinfo = values.next();	
	
		System.out.println("updator receive " + clusterinfo);
		
		int node = clusterinfo.getNodeid();

		int cluster = clusterinfo.getClusterid();
		
		int oldc = -1;
		
		if(this.nodes.get(node) != null)
			oldc = this.nodes.get(node);
		
		if(oldc != cluster && oldc != -1)
		{
			System.out.println("move " + node + " from cluster" + oldc + " to cluster" + cluster );
			updatesclusterTable(oldc, cluster, node);
			updatesciTable(oldc, cluster, node);
			int itemnum1 = this.clusters.get(oldc);
			this.clusters.put(oldc, itemnum1-1);
			int itemnum2 = this.clusters.get(cluster);
			this.clusters.put(cluster, itemnum2+1);
			this.nodes.put(node,cluster);
			System.out.println("update sci and sc tables");
		}
		
			List<Integer> keys = new ArrayList<Integer>(nodes.keySet());
		
			double max = 0;
			int newc = 0;
			oldc = 0;
			double newdelta = 0;
			double oldvalue = 0;
			
			//outbuffer.incrKey();
			//outbuffer.incrKey();
			PriorityRecord<DoubleWritable, ClusterWritable> pkvRecord;
			for(int i : keys)
			{
				oldc = this.nodes.get(i);
				newc = oldc;
				oldvalue = deltas(oldc,i);
				//System.out.println("deltas(oldc,i) = " + oldvalue);
				max = 0;
				
				List<Integer> ckeys = new ArrayList<Integer>(clusters.keySet()); 
				
				for(int c : ckeys)
				{
					newdelta = deltas(c,i) - oldvalue;
					if(Double.compare(newdelta,max) > 0)
					{
						max = newdelta;
						newc = c;
					}
				}
				
				//System.out.println("put " + clusterinfo + " in stateTable");
				
				IntWritable iwi = new IntWritable(i);
				
				if(buffer.stateTable.containsKey(iwi)){
					buffer.stateTable.get(iwi).getiState().setNodeid(node);
					buffer.stateTable.get(iwi).getiState().setClusterid(newc);
					buffer.stateTable.get(iwi).getiState().setAddvalue(max);
					buffer.stateTable.get(iwi).getcState().setNodeid(node);
					buffer.stateTable.get(iwi).getcState().setClusterid(newc);
					buffer.stateTable.get(iwi).getcState().setAddvalue(max);
					buffer.stateTable.get(iwi).getPriority().set(max);
				}
				else
				{
					ClusterWritable cw = new ClusterWritable(node, newc, max);
					pkvRecord = new PriorityRecord<DoubleWritable, ClusterWritable>(
							new DoubleWritable(max), cw, cw);
					buffer.stateTable.put(iwi, pkvRecord);
					//buffer.nTableKeys++;
					//System.out.println(i + " call incrKey()");
				}
				//if(buffer.stateTable.containsKey(iwi)){
				//	double dd = buffer.stateTable.get(iwi).getiState().get();
				//	System.out.println("istate is " + dd);
				//}
			}	
	}
	
	/*
	@Override
	public void close() {
		
		System.out.println("send data to map");
		
		synchronized(outbuffer.stateTable){
			List<Integer> keys = new ArrayList<Integer>(nodes.keySet());
		
			double max = 0;
			int newc = 0;
			int oldc = 0;
			double newdelta = 0;
			double oldvalue = 0;
			
			outbuffer.incrKey();
			outbuffer.incrKey();
			
			for(int i : keys)
			{
				oldc = this.nodes.get(i);
				newc = oldc;
				oldc = this.nodes.get(i);
				newc = this.nodes.get(i);
				oldvalue = deltas(oldc,i);
				//System.out.println("deltas(oldc,i) = " + oldvalue);
				max = 0;
				
				List<Integer> ckeys = new ArrayList<Integer>(clusters.keySet()); 
				
				for(int c : ckeys)
				{
					newdelta = deltas(c,i) - oldvalue;
					if(Double.compare(newdelta,max) > 0)
					{
						max = newdelta;
						newc = c;
					}
				}
				double clusterinfo = i + (newc*1.0)/10000;
				outbuffer.stateTable.get(i).getiState().set(clusterinfo);
				outbuffer.stateTable.get(i).getcState().set(max);
				outbuffer.stateTable.get(i).getPriority().set(max);
			}	
		}	
	}
	*/
}
