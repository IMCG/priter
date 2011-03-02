package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;


public class BSearchReduce extends MapReduceBase implements
		IterativeReducer<IntWritable, IntWritable, IntWritable, IntWritable, IntWritable> {
	private int reduce = 0;
	private int iterate = 0;
	private int startnode;
	
	public void configure(JobConf job) {
		startnode = job.getInt(MainDriver.START_NODE, 0);
	}
	
	@Override
	public void reduce(IntWritable arg0, Iterator<IntWritable> arg1,
			OutputCollector<IntWritable, IntWritable> arg2, Reporter arg3) throws IOException {
	}

	@Override
	public void iterate() {
		iterate++;
	}

	@Override
	public IntWritable setDefaultKey() {
		return new IntWritable(1);
	}
	
	@Override
	public IntWritable setDefaultiState() {
		return new IntWritable(Integer.MAX_VALUE);
	}

	@Override
	public IntWritable setDefaultcState(IntWritable key) {
		if(key.get() == startnode){
			return new IntWritable(0);
		}
		return new IntWritable(Integer.MAX_VALUE);
	}
	

	@Override
	public void initStateTable(
			OutputPKVBuffer<IntWritable, IntWritable> stateTable) {
	}

	@Override
	public IntWritable decidePriority(IntWritable key, IntWritable arg0, boolean iorc) {
		return new IntWritable(-arg0.get());
	}


	@Override
	public void updateState(IntWritable key, Iterator<IntWritable> values,
			OutputPKVBuffer<IntWritable, IntWritable> buffer, Reporter report)
			throws IOException {
		reduce++;	
		report.setStatus(String.valueOf(reduce));
		
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
