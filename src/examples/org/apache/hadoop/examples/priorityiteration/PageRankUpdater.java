package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.PrIterBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Updater;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;


public class PageRankUpdater extends PrIterBase implements
		Updater<FloatWritable, FloatWritable> {
	
	private JobConf job;
	private int workload = 0;
	private int iterate = 0;
	private float initvalue;

	@Override
	public void configure(JobConf job) {	
		this.job = job;
	}

	@Override
	public void iterate() {
		iterate++;
		System.out.println("iteration " + iterate + " total parsed " + workload);
	}

	@Override
	public void initStateTable(
			OutputPKVBuffer<FloatWritable, FloatWritable> stateTable) {
		String subGraphsDir = job.get(MainDriver.SUBGRAPH_DIR);
		int taskid = Util.getTaskId(job);
		Path subgraph = new Path(subGraphsDir + "/part" + taskid);
		
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(job);
			FSDataInputStream in = hdfs.open(subgraph);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					int node = Integer.parseInt(line.substring(0, index));
					stateTable.init(new IntWritable(node), new FloatWritable(0), new FloatWritable(initvalue));
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public FloatWritable resetiState() {
		return new FloatWritable(0);
	}
	
	@Override
	public FloatWritable decidePriority(IntWritable key, FloatWritable iState) {
		return new FloatWritable(iState.get());
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
				buffer.stateTable.get(key).getPriority().set(iState);
			}else{
				pkvRecord = new PriorityRecord<FloatWritable, FloatWritable>(
						new FloatWritable(delta), new FloatWritable(delta), new FloatWritable(delta));
				buffer.stateTable.put(new IntWritable(key.get()), pkvRecord);
			}
		}
	}
}
