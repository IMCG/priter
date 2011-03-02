package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;
import org.apache.hadoop.io.IntWritable;

public class ConnectComponentReduce extends MapReduceBase implements
		IterativeReducer<IntWritable, IntWritable, IntWritable, IntWritable, IntWritable> {

	private int workload = 0;
	private int iter = 0;
	
	@Override
	public void initStateTable(OutputPKVBuffer<IntWritable, IntWritable> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void iterate() {
		iter++;
		System.out.println("iteration " + iter + " total parsed " + workload);
	}

	@Override
	public IntWritable setDefaultKey() {
		return new IntWritable(1);
	}

	@Override
	public IntWritable setDefaultcState(IntWritable arg0) {
		return new IntWritable(Integer.MIN_VALUE);
	}

	@Override
	public IntWritable setDefaultiState() {
		return new IntWritable(Integer.MIN_VALUE);
	}

	@Override
	public IntWritable decidePriority(IntWritable arg0, IntWritable arg1, boolean iorc) {
		return new IntWritable(arg1.get());
	}

	@Override
	public void updateState(IntWritable key, Iterator<IntWritable> values,
			OutputPKVBuffer<IntWritable, IntWritable> buffer, Reporter report)
			throws IOException {
		workload++;		
		report.setStatus(String.valueOf(workload));
		
		int max_id = values.next().get();

		PriorityRecord<IntWritable, IntWritable> pkvRecord;	
		if(buffer.stateTable.containsKey(key)){
			pkvRecord = buffer.stateTable.get(key);

			int cState = pkvRecord.getcState().get();
			if(max_id > cState){
				buffer.stateTable.get(key).getiState().set(max_id);
				buffer.stateTable.get(key).getcState().set(max_id);
				buffer.stateTable.get(key).getPriority().set(max_id);
			}
		}else{
			pkvRecord = new PriorityRecord<IntWritable, IntWritable>(
					new IntWritable(max_id), new IntWritable(max_id), new IntWritable(max_id));
			buffer.stateTable.put(new IntWritable(key.get()), pkvRecord);
		}
	}

	@Override
	public void reduce(IntWritable arg0, Iterator<IntWritable> arg1,
			OutputCollector<IntWritable, IntWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

}
