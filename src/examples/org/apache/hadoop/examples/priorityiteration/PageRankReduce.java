package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;

public class PageRankReduce extends MapReduceBase implements
		IterativeReducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable, DoubleWritable> {
	
	private JobConf job;
	private int nPages;
	private int workload = 0;
	private int iterate = 0;

	@Override
	public void configure(JobConf job) {		
		nPages = job.getInt(MainDriver.PG_TOTAL_PAGES, 0);	
		this.job = job;
	}

	@Override
	public void iterate() {
		iterate++;
	}

	@Override
	public void initStateTable(
			OutputPKVBuffer<DoubleWritable, DoubleWritable> stateTable) {
		int n = Util.getTaskId(job);
		int ttnum = Util.getTTNum(job);
		for(int i=n; i<nPages; i=i+ttnum){
			if(i<100){
				stateTable.init(new IntWritable(i), new DoubleWritable(0.0), new DoubleWritable(0.2));
			}
		}
	}

	@Override
	public IntWritable setDefaultKey() {
		return new IntWritable(0);
	}

	@Override
	public DoubleWritable setDefaultiState() {
		return new DoubleWritable(0.0);
	}
	
	@Override
	public DoubleWritable setDefaultcState(IntWritable key) {
		if(key.get() < 100){
			return new DoubleWritable(0.2);
		}else{
			return new DoubleWritable(0.0);
		}	
	}

	@Override
	public DoubleWritable setPriority(IntWritable key, DoubleWritable iState, boolean iorc) {
		return new DoubleWritable(iState.get());
	}

	@Override
	public void reduce(IntWritable key, Iterator<DoubleWritable> values,
			OutputCollector<IntWritable, DoubleWritable> output,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateState(IntWritable key, Iterator<DoubleWritable> values,
			OutputPKVBuffer<DoubleWritable, DoubleWritable> buffer, Reporter report)
			throws IOException {
		workload++;		
		double delta = 0.0;
		while(values.hasNext()){				
			delta += values.next().get();	
		}

		PriorityRecord<DoubleWritable, DoubleWritable> pkvRecord;	
		if(buffer.stateTable.containsKey(key)){
			pkvRecord = buffer.stateTable.get(key);
			double iState = pkvRecord.getiState().get() + delta;
			double cState = pkvRecord.getcState().get() + delta;
			buffer.stateTable.get(key).getiState().set(iState);
			buffer.stateTable.get(key).getcState().set(cState);
			buffer.stateTable.get(key).getPriority().set(iState);
		}else{
			pkvRecord = new PriorityRecord<DoubleWritable, DoubleWritable>(
					new DoubleWritable(delta), new DoubleWritable(delta), new DoubleWritable(delta));
			buffer.stateTable.put(key, pkvRecord);
		}
	}
}
