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

public class BSearchReduce extends MapReduceBase implements
		IterativeReducer<IntWritable, IntWritable, IntWritable, IntWritable, IntWritable> {
	private JobConf job;
	private int reduce = 0;
	private int iterate = 0;
	private int nNodes = 0;
	
	public void configure(JobConf job) {
		this.job = job;
		nNodes = job.getInt(MainDriver.SP_TOTAL_NODES, 0);
	}
	
	//format node	f:len
	//       node	v:shortest_length
	@Override
	public void reduce(IntWritable key, Iterator<IntWritable> values,
			OutputPKVBuffer<IntWritable, IntWritable, IntWritable> output, Reporter report)
			throws IOException {
		reduce++;	
		//System.out.println("input: " + key);
		
		int min_len = Integer.MAX_VALUE;
		while(values.hasNext()){
			int len = values.next().get();
			if(len<min_len){
				min_len = len;
			}
		}
		
		output.collect(new IntWritable(key.get()), new IntWritable(min_len));
	}

	@Override
	public void reduce(IntWritable arg0, Iterator<IntWritable> arg1,
			OutputCollector<IntWritable, IntWritable> arg2, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		
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
	public IntWritable setPriority(IntWritable iState) {
		return new IntWritable(-iState.get());
	}

	@Override
	public void updateState(IntWritable iState, IntWritable cState, IntWritable value) {
		if(value.get() >= cState.get()){
			iState.set(Integer.MAX_VALUE);
		}else if(value.get() < cState.get()){
			iState.set(value.get());
			cState.set(value.get());
		}
	}

	@Override
	public void initStateTable(
			OutputPKVBuffer<IntWritable, IntWritable, IntWritable> stateTable) {
		int n = Util.getTaskId(job);
		int ttnum = Util.getTTNum(job);
		for(int i=n; i<nNodes; i=i+ttnum){
			stateTable.init(new IntWritable(i), new IntWritable(Integer.MAX_VALUE), new IntWritable(Integer.MAX_VALUE));
		}
	}
}
