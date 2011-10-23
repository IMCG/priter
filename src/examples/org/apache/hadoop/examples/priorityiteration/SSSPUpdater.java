package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Updater;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;


public class SSSPUpdater extends MapReduceBase implements
		Updater<IntWritable, IntWritable> {
	private int workload = 0;
	private int iterate = 0;
	
	@Override
	public void iterate() {
		iterate++;
		System.out.println("iteration " + iterate + " total parsed " + workload);
	}
	
	@Override
	public IntWritable resetiState() {
		return new IntWritable(Integer.MAX_VALUE);
	}
	

	@Override
	public void initStateTable(
			OutputPKVBuffer<IntWritable, IntWritable> stateTable) {
	}

	@Override
	public IntWritable decidePriority(IntWritable key, IntWritable iState) {
		return new IntWritable(-iState.get());
	}

	@Override
	public IntWritable decideTopK(IntWritable key, IntWritable cState) {
		return new IntWritable(-cState.get());
	}

	@Override
	public void updateState(IntWritable key, Iterator<IntWritable> values,
			OutputPKVBuffer<IntWritable, IntWritable> buffer, Reporter report)
			throws IOException {
		workload++;	
		report.setStatus(String.valueOf(workload));
		
		int min_len = values.next().get();
		synchronized(buffer.stateTable){
			PriorityRecord<IntWritable, IntWritable> pkvRecord;	
			if(buffer.stateTable.containsKey(key)){
				pkvRecord = buffer.stateTable.get(key);
				int cState = pkvRecord.getcState().get();
				if(min_len < cState){
					buffer.stateTable.get(key).getiState().set(min_len);
					buffer.stateTable.get(key).getcState().set(min_len);
					buffer.stateTable.get(key).getPriority().set(-min_len);
				}
			}else{
				pkvRecord = new PriorityRecord<IntWritable, IntWritable>(
						new IntWritable(-min_len), new IntWritable(min_len), new IntWritable(min_len));
				buffer.stateTable.put(new IntWritable(key.get()), pkvRecord);
			}
		}
	}
}
