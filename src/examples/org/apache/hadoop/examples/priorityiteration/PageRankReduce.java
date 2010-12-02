package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;
;

public class PageRankReduce extends MapReduceBase implements
		IterativeReducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	
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
			OutputPKVBuffer<DoubleWritable, IntWritable, DoubleWritable> output, Reporter report)
			throws IOException {
		workload++;
		
		int page = key.get();
		
		double delta = 0.0;
		while(values.hasNext()){				
			delta += values.next().get();	
			addition++;
			//System.out.println("input: " + key + " : " + delta);
		}
		
		if(ranks.containsKey(page)){
			double baseRank = ranks.get(page);			
			ranks.put(page, baseRank + delta);
			//System.out.println(page + "\t" + baseRank + "\t" + delta);
		}else{
			System.out.println("wrong");
		}

		DoubleWritable priority = new DoubleWritable(delta);
		/*
		DoubleWritable priority = null;
		if(!this.linkNum.containsKey(page)){
			System.out.println("no " + page + " found!");
		}else{
			if (this.linkNum.get(page)==0){
				priority = new DoubleWritable(Double.MIN_VALUE);
			}else{
				
			}	
		}
		*/
		IntWritable outKey = new IntWritable(page);
		DoubleWritable outVal = new DoubleWritable(delta);
		output.collect(priority, outKey, outVal);

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
			OutputPKVBuffer<DoubleWritable, IntWritable, DoubleWritable> stateTable) {
		int n = Util.getTaskId(job);
		int ttnum = Util.getTTNum(job);
		for(int i=n; i<nPages; i=i+ttnum){
			if(i<100){
				stateTable.init(new IntWritable(i), new DoubleWritable(0.0), new DoubleWritable(0.2));
			}else{
				stateTable.init(new IntWritable(i), new DoubleWritable(0.0), new DoubleWritable(0.0));
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
	public DoubleWritable setPriority(DoubleWritable iState) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateState(DoubleWritable iState, DoubleWritable cState,
			DoubleWritable value) {
		// TODO Auto-generated method stub
		
	}
}
