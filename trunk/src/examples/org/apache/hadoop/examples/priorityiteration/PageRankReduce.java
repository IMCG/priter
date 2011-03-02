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
	private int startPages;
	private int workload = 0;
	private int iterate = 0;
	private double initvalue;
	private int partitions;

	@Override
	public void configure(JobConf job) {		
		nPages = job.getInt("priter.graph.nodes", 0);	
		startPages = job.getInt(MainDriver.START_NODE, nPages);
		this.job = job;
		initvalue = PageRank.RETAINFAC * nPages / startPages;
		partitions = job.getInt("mapred.iterative.partitions", 0);
	}

	@Override
	public void iterate() {
		iterate++;
	}

	@Override
	public void initStateTable(
			OutputPKVBuffer<DoubleWritable, DoubleWritable> stateTable) {
		int n = Util.getTaskId(job);
		for(int i=n; i<nPages; i=i+partitions){
			if(i < startPages){
				stateTable.init(new IntWritable(i), new DoubleWritable(0.0), new DoubleWritable(initvalue));
			}else{
				stateTable.init(new IntWritable(i), new DoubleWritable(0.0), new DoubleWritable(0.0));
			}	
		}
	}

	@Override
	public DoubleWritable setDefaultiState() {
		return new DoubleWritable(0.0);
	}

	@Override
	public DoubleWritable setDefaultcState(IntWritable key) {
		if(key.get() < startPages){
			return new DoubleWritable(initvalue);
		}else{
			return new DoubleWritable(0.0);
		}	
	}
	
	@Override
	public DoubleWritable decidePriority(IntWritable key, DoubleWritable arg0, boolean iorc) {
		return new DoubleWritable(arg0.get());
	}

	@Override
	public void updateState(IntWritable key, Iterator<DoubleWritable> values,
			OutputPKVBuffer<DoubleWritable, DoubleWritable> buffer, Reporter report)
			throws IOException {
		workload++;		
		report.setStatus(String.valueOf(workload));
		
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
			buffer.stateTable.put(new IntWritable(key.get()), pkvRecord);
		}
	}


}
