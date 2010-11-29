package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;




public class BSearchReduce extends MapReduceBase implements
		IterativeReducer<IntWritable, IntWritable, Text, IntWritable, Text> {
	
	private int topk = 0;
	private int thsize = 0;
	//private TreeSet<TopKRecord> heap = new TreeSet<TopKRecord>();
	private HashMap<Integer, Integer> heap = new HashMap<Integer, Integer>();
	private int expand_threshold = Integer.MIN_VALUE;
	private int iter = 0;
	private boolean heapFull = false;
	private int last_thresh = Integer.MAX_VALUE;
	private int snapshot_index = 0;
	private int max = Integer.MIN_VALUE;
	private int countf = 0;
	private int reduce = 0;
	private int compare = 0;
	private int iterate = 0;

	
	private class TopKRecord implements Comparable<TopKRecord> {
		public int node;
		public int length;

		public TopKRecord(int key, int sum) {
			this.node = key;
			this.length = sum;
		}
		

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof TopKRecord))
				return false;

			TopKRecord other = (TopKRecord) obj;
			if (other.node == this.node)
				return true;
			else
				return false;
		}

		@Override
		public int hashCode() {
			return this.node;
		}

		@Override
		public int compareTo(TopKRecord other) {
			if (this.length == other.length)
				return (this.node > other.node ? -1 : 1);

			return (this.length > other.length ? -1 : 1);
		}
	}
	
	private List<Integer> getSortedKey(){
		Date start = new Date();
		synchronized(this.heap){
			final Map<Integer, Integer> langForComp = this.heap;
			List<Integer> keys = new ArrayList<Integer>(this.heap.keySet());
			Collections.sort(keys, 
					new Comparator(){
						public int compare(Object left, Object right){
							int leftValue = langForComp.get((Integer)left);
							int rightValue = langForComp.get((Integer)right);
							return (leftValue > rightValue) ? 1 : -1;
						}
					});
			Date end = new Date();
			System.out.println("took time " + (end.getTime()-start.getTime()) + "ms");
			return keys;
		}	
	}
	
	public void configure(JobConf job) {
		topk = job.getInt(MainDriver.TOP_K, 0);
		thsize = topk/job.getInt("mapred.iterative.ttnum", -1);
	}
	
	//format node	f:len
	//       node	v:shortest_length
	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputPKVBuffer<IntWritable, IntWritable, Text> output, Reporter report)
			throws IOException {
		reduce++;	
		//System.out.println("input: " + key);
		synchronized(this.heap){
			boolean hasf = false;
			boolean fIsShorter = false;
			int len = -1;
			int node = key.get();
			int min_len = this.heap.containsKey(node) ? this.heap.get(node) : Integer.MAX_VALUE;
			
			while(values.hasNext()){		
				String value = values.next().toString();
				
				//System.out.println("input: " + key + " : " + value + " : " + min_len);
				
				int index = value.indexOf(":");
				if(index == -1){
					System.out.println("some thing wrong, no :");
					continue;
				}
				String indicator = value.substring(0, index);
							
				if(indicator.equals("f")){
					//hasf = true;
					len = Integer.parseInt(value.substring(index+1));
					if(len<min_len){
						min_len = len;
						fIsShorter = true;
						compare++;
					}
				}else{
					System.out.println("error!\n");
				}
			
			}
			
			/**
			 * v is value, which has the lowest priority, since we just 
			 * expand frontier, and frontier's priority is inversely proportional
			 * to the length, the shorter the length, the higher priority it has
			 */
			//if node distance is less than the stored distance, than expand it again
			if(fIsShorter){
				countf++;
				String out = "f:" + String.valueOf(min_len);
				output.collect(new IntWritable(-min_len), new IntWritable(key.get()), new Text(out));
				this.heap.put(node, min_len);	
				
				//System.out.println(key + " : " + out);
			}	
		}
	}



	@Override
	public void reduce(IntWritable arg0, Iterator<Text> arg1,
			OutputCollector<IntWritable, Text> arg2, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		
	}


	//format: priority	key 	value
	@Override
	public synchronized boolean snapshot(BufferedWriter writer) throws IOException{
		snapshot_index++;
		synchronized(this.heap){
			List<Integer> keys = getSortedKey();
	
			HashMap<Integer, Integer> temp = new HashMap<Integer, Integer>(topk);
			Iterator<Integer> itr =keys.iterator();
			int count = 0;
			int len = 0;
			int bound = 0;
			while(itr.hasNext() && count < topk){
				int k = itr.next();
				len = this.heap.get(k);
				
				writer.write(-len + "\t" + k + "\t" + len + "\n");
				temp.put(k, len);
				
				count++;
				if(count == thsize) bound = -len;
			}
			this.heap = temp;
			
			if(count >= thsize) this.expand_threshold = bound;
			
			System.out.println("snapshot index " + snapshot_index + 
					", heap size " + this.heap.size() + " keys size " + keys.size());
			System.out.println("snapshot index " + snapshot_index + " iterations " + iterate +
					" reduce is " + reduce + " compare is " + compare);
		}

		return true;
	}

	@Override
	public IntWritable setPriority(IntWritable key, Text value) {
		String valstr = value.toString();
		int index = valstr.indexOf(":");
		int len = Integer.parseInt(valstr.substring(index+1));
		
		return new IntWritable(-len);
	}

	@Override
	public int bucketTransfer(int[] steps) {
		iter++;
		/*
		if(iter<10){
			return 2;
		}else{
			return 1;
		}
		*/
		if(max < -30){
			return 3;
		}else{
			return 3;
		}
		
	}

	@Override
	public IntWritable[] bufSplit(IntWritable minP, IntWritable maxP,
			boolean init) {
		if(init == true){
			minP.set(Integer.MAX_VALUE);
			maxP.set(Integer.MIN_VALUE);
			return null;
		}
		
		max = maxP.get();
		int step = (maxP.get() - minP.get())/10;
		IntWritable[] steps = new IntWritable[10];
		for(int i=0; i<10; i++){
			steps[i] = new IntWritable(maxP.get() - step*(i+1));
		}
		return steps;
	}

	@Override
	public IntWritable bound(IntWritable min, IntWritable max) {
		iter++;
		return new IntWritable(-667);
		//return new IntWritable(expand_threshold);
		/*
		if(max.get() == 0.0){
			return new IntWritable(expand_threshold);
		}else{

			System.out.println("max is " + max.get() + " min is " + min.get() + " bound is " + this.expand_threshold);

			return new IntWritable(expand_threshold);
		}	
		*/
	}

	@Override
	public void combine(IntWritable pri1, Text value1, IntWritable pri2,
			Text value2, PriorityRecord<IntWritable, Text> output) {
		
		if(pri1.get() > pri2.get()){
			output.setPriority(pri1);
			output.setValue(new Text(value1.toString()));
		}else{
			output.setPriority(pri2);
			output.setValue(new Text(value2.toString()));
		}
	}

	@Override
	public void defaultKV(IntWritable key, Text value) {
		// TODO Auto-generated method stub
		key.set(1);
		value.set("nothing");
	}
	
	@Override
	public void iterate() {
		iterate++;
	}

	@Override
	public Text setDefaultiState() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IntWritable setPriority(Text iState) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Text updateState(Text oldState, Text value) {
		// TODO Auto-generated method stub
		return null;
	}
}
