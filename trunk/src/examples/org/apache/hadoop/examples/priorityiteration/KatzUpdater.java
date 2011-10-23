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
import org.apache.hadoop.mapred.Updater;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;



public class KatzUpdater extends MapReduceBase implements
		Updater<FloatWritable, FloatWritable> {
	
	private int workload = 0;
	private int iterate = 0;
	private String subGraphsDir;
	private HashMap<Integer, Integer> degreeMap = new HashMap<Integer, Integer>();

	private synchronized void readDegree(JobConf conf, int n){
		subGraphsDir = conf.get(MainDriver.SUBGRAPH_DIR);
		Path remote_link = new Path(subGraphsDir + "/part" + n);
		
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(conf);
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
			e.printStackTrace();
		}
	}
	
	@Override
	public void configure(JobConf job) {	
		readDegree(job, Util.getTaskId(job));
	}

	@Override
	public void iterate() {
		iterate++;
		System.out.println("iteration " + iterate + " total parsed " + workload);
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
	public FloatWritable decidePriority(IntWritable key, FloatWritable iState) {
		if(this.degreeMap.get(key.get()) != null){
			return new FloatWritable(iState.get()*this.degreeMap.get(key.get()));
		}else{
			return new FloatWritable(iState.get());
		}
	}
	
	@Override
	public FloatWritable decideTopK(IntWritable key, FloatWritable cState) {
		return new FloatWritable(cState.get());
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
		}

		synchronized(buffer.stateTable){
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
	}
}
