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

public class PageRankReduce extends MapReduceBase implements
		IterativeReducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	
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
	public void reduce(IntWritable key, Iterator<DoubleWritable> values,
			OutputPKVBuffer<IntWritable, DoubleWritable> output, Reporter report)
			throws IOException {
		workload++;	
		int page = key.get();		
		double delta = 0.0;
		while(values.hasNext()){				
			delta += values.next().get();	
			//System.out.println("input: " + key + " : " + delta);
		}

		IntWritable outKey = new IntWritable(page);
		DoubleWritable outVal = new DoubleWritable(delta);
		output.collect(outKey, outVal);

		//System.out.println("output: " + priority + " : " + outKey + " : " + outVal);
	}

	@Override
	public void reduce(IntWritable arg0, Iterator<DoubleWritable> arg1,
			OutputCollector<IntWritable, DoubleWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void iterate() {
		iterate++;
	}

	@Override
	public void initStateTable(
			OutputPKVBuffer<IntWritable, DoubleWritable> stateTable) {
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
	public void updateState(DoubleWritable iState, DoubleWritable cState,
			DoubleWritable value) {
		iState.set(iState.get() + value.get());
		cState.set(cState.get() + value.get());
	}

	@Override
	public int compare(DoubleWritable state1, DoubleWritable state2) {
		return state1.compareTo(state2);
	}
}
