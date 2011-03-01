package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;
import org.apache.hadoop.mapred.buffer.impl.StateTableIterator;


public class BSearchReduce extends MapReduceBase implements
		IterativeReducer<IntWritable, IntWritable, IntWritable, IntWritable, IntWritable> {
	private JobConf job;
	private int reduce = 0;
	private int iterate = 0;
	private int startnode;
	private int nNodes = 0;
	
	public void configure(JobConf job) {
		this.job = job;
		nNodes = job.getInt(MainDriver.SP_TOTAL_NODES, 0);
		startnode = job.getInt(MainDriver.SP_START_NODE, 0);
	}
	
	//format node	f:len
	//       node	v:shortest_length

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
		return new IntWritable(Integer.MAX_VALUE);
	}

	@Override
	public void initStateTable(
			OutputPKVBuffer<IntWritable, IntWritable> stateTable) {
		stateTable.init(new IntWritable(startnode), new IntWritable(0), new IntWritable(0));
	}

	@Override
	public IntWritable setPriority(IntWritable key, IntWritable iState, boolean iorc) {
		return new IntWritable(-iState.get());
	}

	@Override
	public void reduce(IntWritable key, Iterator<IntWritable> values,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateState(IntWritable key, Iterator<IntWritable> values,
			OutputPKVBuffer<IntWritable, IntWritable> buffer, Reporter report)
			throws IOException {
		reduce++;	
		
		int min_len = Integer.MAX_VALUE;
		while(values.hasNext()){
			int len = values.next().get();
			//System.out.println("input value: " + len);
			if(len<min_len){
				min_len = len;
			}
		}
		
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
			buffer.stateTable.put(key, pkvRecord);
		}
	}
}
