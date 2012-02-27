package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileBasedUpdater;
import org.apache.hadoop.mapred.IFile.PriorityQueueWriter;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.PrIterBase;
import org.apache.hadoop.mapred.Reporter;


public class PageRankFileUpdater extends PrIterBase implements
		FileBasedUpdater<IntWritable, FloatWritable, FloatWritable, IntArrayWritable> {
	
	private JobConf job;
	private int workload = 0;
	private int iterate = 0;

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
	public FloatWritable resetiState() {
		return new FloatWritable(0);
	}
	
	@Override
	public FloatWritable decidePriority(IntWritable key, FloatWritable iState) {
		return new FloatWritable(iState.get());
	}
	
	@Override
	public FloatWritable decideTopK(IntWritable key, FloatWritable cState) {
		return new FloatWritable(cState.get());
	}

	@Override
	public long initFiles(
			Writer<IntWritable, FloatWritable> istateWriter,
			Writer<IntWritable, FloatWritable> cstateWriter,
			Writer<IntWritable, IntArrayWritable> staticWriter,
			PriorityQueueWriter<IntWritable, FloatWritable, IntArrayWritable> priorityqueueWriter) {
		
		String subGraphsDir = job.get(MainDriver.SUBGRAPH_DIR);
		int taskid = Util.getTaskId(job);
		Path subgraph = new Path(subGraphsDir + "/part" + taskid);
		
		long records = 0;
		FileSystem hdfs = null;
	    try {
			hdfs = FileSystem.get(job);
			FSDataInputStream in = hdfs.open(subgraph);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
			String line;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					int node = Integer.parseInt(line.substring(0, index));
					String linkstring = line.substring(index+1);
					ArrayList<Integer> links = new ArrayList<Integer>();
					StringTokenizer st = new StringTokenizer(linkstring);
					while(st.hasMoreTokens()){
						links.add(Integer.parseInt(st.nextToken()));
					}
					
					IntWritable[] links2 = new IntWritable[links.size()];
					
					//System.out.print(node + ":" + links2.length + "\t");
					
					for(int i=0; i<links.size(); i++){
						links2[i] = new IntWritable(links.get(i));
						//System.out.print(links2[i] + " ");
					}
					//System.out.println();
					
					IntArrayWritable links3 = new IntArrayWritable();
					links3.set(links2);
						
					istateWriter.append(new IntWritable(node), new FloatWritable(0));
					cstateWriter.append(new IntWritable(node), new FloatWritable(PageRank.RETAINFAC));
					staticWriter.append(new IntWritable(node), links3);
					priorityqueueWriter.append(new IntWritable(node), new FloatWritable(PageRank.RETAINFAC), links3);
					
					records++;
				}
			}

			System.out.println(istateWriter.getRawLength());
			System.out.println(cstateWriter.getRawLength());
			System.out.println(staticWriter.getRawLength());
			System.out.println(priorityqueueWriter.getRawLength());
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
	    return records;
	}

	@Override
	public FloatWritable accumulate(FloatWritable iState, FloatWritable cState) {
		return new FloatWritable(iState.get() + cState.get());
	}

	@Override
	public void updateState(IntWritable key, Iterator<FloatWritable> values,
			OutputCollector<IntWritable, FloatWritable> output,
			Reporter reporter) throws IOException {
		workload++;		
		reporter.setStatus(String.valueOf(workload));
		
		float delta = 0;
		while(values.hasNext()){	
			float v = values.next().get();
			delta += v;
		}
		
		output.collect(key, new FloatWritable(delta));
	}
}
