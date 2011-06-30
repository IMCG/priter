package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Updator;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;

public class HittingTimeUpdator extends MapReduceBase implements Updator<DoubleWritable, DoubleWritable> {
	
	private int reduce = 0;
	private int iterate = 0;
	private int startnode = 0;
	private int totalnode = 1100000;
	private String subGraphsDir;
	
	//graph in local memory
	private double Weights[] = new double[totalnode];

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
			double weight = 0.0;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					String node = line.substring(0, index);
					weight = 0.0;
					
					String linkstring = line.substring(index+1);
					StringTokenizer st = new StringTokenizer(linkstring);
					while(st.hasMoreTokens()){
						String link = st.nextToken();
						String item[] = link.split(",");
						weight += Double.parseDouble(item[1]);
					}
					Weights[Integer.parseInt(node)] = weight;
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
	    totalnode = job.getInt("priter.graph.nodes", 1100000);
	    int taskid = Util.getTaskId(job);
		this.loadGraphToMem(job, taskid);
	}
	
	@Override
	public void iterate() {
		iterate++;
	}
	
	@Override
	public DoubleWritable resetiState() {
		return new DoubleWritable(0.0);
	}
	

	@Override
	public void initStateTable(
			OutputPKVBuffer<DoubleWritable, DoubleWritable> stateTable) {
		stateTable.init(new IntWritable(startnode), new DoubleWritable(0.0), new DoubleWritable(0.0));
	}

	@Override
	public DoubleWritable decidePriority(IntWritable key, DoubleWritable arg0, boolean iorc) {
		if(iorc)
			return new DoubleWritable(arg0.get());
		else
			return new DoubleWritable(-arg0.get());
	}


	@Override
	public void updateState(IntWritable key, Iterator<DoubleWritable> values,
			OutputPKVBuffer<DoubleWritable, DoubleWritable> buffer, Reporter report)
			throws IOException {
		reduce++;	
		report.setStatus(String.valueOf(reduce));
		
		
		double delta = 0.0;
		while(values.hasNext()){				
			delta += values.next().get();	
			//System.out.println(key + "get " + delta);
		}
		//System.out.println(key + "get " + delta);
		
		PriorityRecord<DoubleWritable, DoubleWritable> pkvRecord;	
		if(buffer.stateTable.containsKey(key)){
			String node = key.toString();
			int nodeid = Integer.parseInt(node);
			if(nodeid == startnode)
			{
				buffer.stateTable.get(key).getiState().set(0.0);
				buffer.stateTable.get(key).getcState().set(0.0);
				buffer.stateTable.get(key).getPriority().set(0.0);
			}
			else
			{
				pkvRecord = buffer.stateTable.get(key);
				double iState = pkvRecord.getiState().get() + delta;
				double cState = pkvRecord.getcState().get() + delta;
				buffer.stateTable.get(key).getiState().set(iState);
				buffer.stateTable.get(key).getcState().set(cState);
				buffer.stateTable.get(key).getPriority().set(iState*Weights[nodeid]);
				//System.out.println(node + "\t" + Weights[nodeid]);
			}
			
		}else{
			pkvRecord = new PriorityRecord<DoubleWritable, DoubleWritable>(
					new DoubleWritable(delta+1.0), new DoubleWritable(delta+1.0), new DoubleWritable(delta+1.0));
			buffer.stateTable.put(new IntWritable(key.get()), pkvRecord);
			//System.out.println("update " + key + " with " + pkvRecord);

		}
	}

	@Override
	public FloatWritable obj() {
		// TODO Auto-generated method stub
		return null;
	}
}
