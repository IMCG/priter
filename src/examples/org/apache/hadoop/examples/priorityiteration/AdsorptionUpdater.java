package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.PrIterBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Updater;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;


public class AdsorptionUpdater extends PrIterBase implements
		Updater<IntWritable, DoubleWritable, DoubleWritable> {
	
	private JobConf job;
	private int workload = 0;
	private int iterate = 0;
	private HashMap<Integer, Double> weightMap = new HashMap<Integer, Double>();

	@Override
	public void configure(JobConf job) {		
		this.job = job;
	}
	
	@Override
	public void iterate() {
		iterate++;
		System.out.println("iteration " + iterate + " total parsed " + workload);
	}

	@Override
	public void initStateTable(
			OutputPKVBuffer<IntWritable, DoubleWritable, DoubleWritable> stateTable) {
		String subGraphsDir = job.get(MainDriver.SUBGRAPH_DIR);
		int taskid = Util.getTaskId(job);
		Path remote_link = new Path(subGraphsDir + "/part-" + taskid);
		double initvalue = Adsorption.RETAINFAC * 10000;
		
		FileSystem hdfs = null;
		try{
			hdfs = FileSystem.get(job);
			FSDataInputStream in = hdfs.open(remote_link);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					int node = Integer.parseInt(line.substring(0, index));
					int index2 = line.indexOf(":");
					if(index2 != -1){
						String linkstring = line.substring(index2+1);
						double weight = 0.0;
						StringTokenizer st = new StringTokenizer(linkstring);
						while(st.hasMoreTokens()){
							String[] field = st.nextToken().split(",");
							weight += Double.parseDouble(field[1]);
						}
						this.weightMap.put(node, weight);	
						stateTable.init(new IntWritable(node), new DoubleWritable(0), new DoubleWritable(initvalue));
					}				
				}
			}
			
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public DoubleWritable resetiState() {
		return new DoubleWritable(0.0);
	}

	@Override
	public DoubleWritable decidePriority(IntWritable key, DoubleWritable iState) {
		Double weight = weightMap.get(key.get());
		if(weight == null){
			System.out.println(key + "\tnull");
			return new DoubleWritable(0.0);
		}else{
			return new DoubleWritable(iState.get() * weightMap.get(key.get()));
		}	
	}
	
	@Override
	public DoubleWritable decideTopK(IntWritable key, DoubleWritable cState) {
		Double weight = weightMap.get(key.get());
		if(weight == null){
			System.out.println(key + "\tnull");
			return new DoubleWritable(0.0);
		}else{
			return new DoubleWritable(cState.get());
		}	
	}

	@Override
	public void updateState(IntWritable key, Iterator<DoubleWritable> values,
			OutputPKVBuffer<IntWritable, DoubleWritable, DoubleWritable> buffer, Reporter report)
			throws IOException {
		workload++;		
		report.setStatus(String.valueOf(workload));
		
		double delta = 0.0;
		while(values.hasNext()){				
			delta += values.next().get();	
		}
		
		synchronized(buffer.stateTable){
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
}
