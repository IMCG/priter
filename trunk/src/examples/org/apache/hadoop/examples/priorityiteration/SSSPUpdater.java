package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.PrIterBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Updater;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;


public class SSSPUpdater extends PrIterBase implements
		Updater<IntWritable, FloatWritable, FloatWritable> {
	private int workload = 0;
	private int iterate = 0;
	
	@Override
	public void iterate() {
		iterate++;
		System.out.println("iteration " + iterate + " total parsed " + workload);
	}
	
	@Override
	public FloatWritable resetiState() {
		return new FloatWritable(Float.MAX_VALUE);
	}
	

	@Override
	public void initStateTable(
			OutputPKVBuffer<IntWritable, FloatWritable, FloatWritable> stateTable) {
	}

	@Override
	public FloatWritable decidePriority(IntWritable key, FloatWritable iState) {
		return new FloatWritable(-iState.get());
	}

	@Override
	public FloatWritable decideTopK(IntWritable key, FloatWritable cState) {
		return new FloatWritable(-cState.get());
	}

	@Override
	public void updateState(IntWritable key, Iterator<FloatWritable> values,
			OutputPKVBuffer<IntWritable, FloatWritable, FloatWritable> buffer, Reporter report)
			throws IOException {
		workload++;	
		report.setStatus(String.valueOf(workload));
		
		float min_len = values.next().get();
		synchronized(buffer.stateTable){
			PriorityRecord<FloatWritable, FloatWritable> pkvRecord;	
			if(buffer.stateTable.containsKey(key)){
				pkvRecord = buffer.stateTable.get(key);
				float cState = pkvRecord.getcState().get();
				if(min_len < cState){
					buffer.stateTable.get(key).getiState().set(min_len);
					buffer.stateTable.get(key).getcState().set(min_len);
					buffer.stateTable.get(key).getPriority().set(-min_len);
				}
			}else{
				pkvRecord = new PriorityRecord<FloatWritable, FloatWritable>(
						new FloatWritable(-min_len), new FloatWritable(min_len), new FloatWritable(min_len));
				buffer.stateTable.put(new IntWritable(key.get()), pkvRecord);
			}
		}
	}
}
