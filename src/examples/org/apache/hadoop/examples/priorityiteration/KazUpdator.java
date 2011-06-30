package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Updator;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;



public class KazUpdator extends MapReduceBase implements
		Updator<FloatWritable, FloatWritable> {
	
	private int workload = 0;
	private int iterate = 0;
	private String subGraphsDir;
	private HashMap<Integer, Integer> degreeMap = new HashMap<Integer, Integer>();

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
					String[] links = linkstring.split(" ");
					int degree = links.length;

					this.degreeMap.put(Integer.parseInt(node), degree);
				}
				
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void configure(JobConf job) {	

		loadGraphToMem(job, Util.getTaskId(job));
		System.out.println("load graph");
	}

	@Override
	public void iterate() {
		iterate++;
	}

	@Override
	public void initStateTable(
			OutputPKVBuffer<FloatWritable, FloatWritable> stateTable) {
	}

	@Override
	public FloatWritable resetiState() {
		return new FloatWritable(0);
	}
	
	@Override
	public FloatWritable decidePriority(IntWritable key, FloatWritable arg0, boolean iorc) {
		if(iorc){
			if(this.degreeMap.get(key.get()) != null){
				return new FloatWritable(arg0.get()*this.degreeMap.get(key.get()));
			}else{
				return new FloatWritable(arg0.get());
			}
			
		}else{
			return new FloatWritable(arg0.get());
		}
		
	}

	@Override
	public void updateState(IntWritable key, Iterator<FloatWritable> values,
			OutputPKVBuffer<FloatWritable, FloatWritable> buffer, Reporter report)
			throws IOException {
		workload++;		
		report.setStatus(String.valueOf(workload));
		
		float delta = 0;
		while(values.hasNext()){				
			delta += values.next().get();	
			//System.out.println("input " + key + "\t" + delta);
		}

		PriorityRecord<FloatWritable, FloatWritable> pkvRecord;	
		if(buffer.stateTable.containsKey(key)){
			pkvRecord = buffer.stateTable.get(key);

			float iState = pkvRecord.getiState().get() + delta;
			float cState = pkvRecord.getcState().get() + delta;
			buffer.stateTable.get(key).getiState().set(iState);
			buffer.stateTable.get(key).getcState().set(cState);
			buffer.stateTable.get(key).getPriority().set(iState*this.degreeMap.get(key.get()));
		}else{
			pkvRecord = new PriorityRecord<FloatWritable, FloatWritable>(
					new FloatWritable(delta), new FloatWritable(delta), new FloatWritable(delta));
			buffer.stateTable.put(new IntWritable(key.get()), pkvRecord);
		}
	}

	@Override
	public FloatWritable obj() {
		// TODO Auto-generated method stub
		return null;
	}


}
