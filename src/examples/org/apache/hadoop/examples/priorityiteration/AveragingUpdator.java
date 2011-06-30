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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Updator;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;


public class AveragingUpdator extends MapReduceBase implements
		Updator<DoubleWritable, DoubleWritable> {
	
	private JobConf job;
	private int nNodes;
	private int startNodes;
	private double initvalue;
	private int workload = 0;
	private int iterate = 0;
	private String subGraphsDir;
	private HashMap<Integer, Double> weightMap = new HashMap<Integer, Double>();

	@Override
	public void configure(JobConf job) {		
		nNodes = job.getInt("priter.graph.nodes", 0);	
		startNodes = job.getInt(MainDriver.START_NODE, nNodes);
		initvalue = Averaging.RETAINFAC * nNodes / startNodes;
		this.job = job;
		int taskid = Util.getTaskId(job);
		try {
			loadGraphToMem(job, taskid);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private synchronized void loadGraphToMem(JobConf conf, int n) throws IOException{
		FileSystem hdfs = null;
		hdfs = FileSystem.get(conf);
		assert(hdfs != null);
		
		subGraphsDir = conf.get(MainDriver.SUBGRAPH_DIR);

		Path remote_link = new Path(subGraphsDir + "/part-" + n);

		FSDataInputStream in = hdfs.open(remote_link);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		
		String line;
		while((line = reader.readLine()) != null){
			int index = line.indexOf("\t");
			if(index != -1){
				String node = line.substring(0, index);
				int index2 = line.indexOf(":");
				if(index2 != -1){
					String linkstring = line.substring(index2+1);
					double weight = 0.0;
					StringTokenizer st = new StringTokenizer(linkstring);
					while(st.hasMoreTokens()){
						String[] field = st.nextToken().split(",");
						weight += Double.parseDouble(field[1]);
					}
					this.weightMap.put(Integer.parseInt(node), weight);		
					//ystem.out.println(node + "\t" + weight);
				}				
			}
		}
		
		reader.close();
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
		for(int i=n; i<nNodes; i=i+ttnum){
			if(i<startNodes){
				stateTable.init(new IntWritable(i), new DoubleWritable(0.0), new DoubleWritable(initvalue));
			}
		}
	}

	@Override
	public DoubleWritable resetiState() {
		return new DoubleWritable(0.0);
	}

	@Override
	public DoubleWritable decidePriority(IntWritable key, DoubleWritable arg0, boolean iorc) {
		//System.out.println(key.get() + "\t" + arg0.get());
		Double weight = weightMap.get(key.get());
		if(weight == null){
			System.out.println(key + "\tnull");
			return new DoubleWritable(0.0);
		}else{
			if(iorc){
				return new DoubleWritable(arg0.get() * weightMap.get(key.get()));
			}else{
				return new DoubleWritable(arg0.get());
			}
		}
		
	}

	@Override
	public void updateState(IntWritable key, Iterator<DoubleWritable> values,
			OutputPKVBuffer<DoubleWritable, DoubleWritable> buffer, Reporter arg3)
			throws IOException {
		workload++;		
		arg3.setStatus(String.valueOf(workload));
		double delta = 0.0;
		while(values.hasNext()){				
			delta += values.next().get();	
			//System.out.println("input: " + key + " : " + delta);
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

	@Override
	public FloatWritable obj() {
		// TODO Auto-generated method stub
		return null;
	}
}
