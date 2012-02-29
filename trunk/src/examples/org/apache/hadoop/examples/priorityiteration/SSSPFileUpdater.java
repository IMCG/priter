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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileBasedUpdater;
import org.apache.hadoop.mapred.IFile.PriorityQueueWriter;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.PrIterBase;
import org.apache.hadoop.mapred.Reporter;


public class SSSPFileUpdater extends PrIterBase implements
		FileBasedUpdater<IntWritable, FloatWritable, FloatWritable, LinkArrayWritable> {
	private int workload = 0;
	private int iterate = 0;
  private int startnode;
  private int partitions;
  private JobConf job;
	
  
  private class Link{
		int node;
		float weight;
		
		public Link(int n, float w){
			node = n;
			weight = w;
		}
	}
    
  @Override
	public void configure(JobConf job) {	
		this.job = job;
    this.startnode = job.getInt(MainDriver.START_NODE, 0);
    this.partitions = job.getInt("priter.graph.partitions", -1);
	}
    
	@Override
	public void iterate() {
		iterate++;
		System.out.println("iteration " + iterate + " total parsed " + workload);
	}
	
	@Override
	public FloatWritable resetiState() {
		return new FloatWritable(Float.MAX_VALUE);
	}
	

    
  @Override
  public long initFiles(Writer<IntWritable, FloatWritable> istateWriter, 
                        Writer<IntWritable, FloatWritable> cstateWriter, 
                        Writer<IntWritable, LinkArrayWritable> staticWriter, 
                        PriorityQueueWriter<IntWritable, FloatWritable, LinkArrayWritable> priorityqueueWriter) {
		
		String subGraphsDir = job.get(MainDriver.SUBGRAPH_DIR);
		int taskid = Util.getTaskId(job);
		Path subgraph = new Path(subGraphsDir + "/part" + taskid);
		
		long records = 0;
		FileSystem hdfs = null;
    float max = Float.MAX_VALUE;
	  try {
			hdfs = FileSystem.get(job);
			FSDataInputStream in = hdfs.open(subgraph);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			
      boolean hasStart = false;
			String line;
			while((line = reader.readLine()) != null){
				int index = line.indexOf("\t");
				if(index != -1){
					int node = Integer.parseInt(line.substring(0, index));
					String linkstring = line.substring(index+1);
					ArrayList<Link> links = new ArrayList<Link>();
					StringTokenizer st = new StringTokenizer(linkstring);
					while(st.hasMoreTokens()){
						String link = st.nextToken();
						String item[] = link.split(",");
						Link l = new Link(Integer.parseInt(item[0]), Float.parseFloat(item[1]));
						links.add(l);
					}
					
					LinkWritable[] links2 = new LinkWritable[links.size()];

					for(int i=0; i<links.size(); i++){
						links2[i] = new LinkWritable(links.get(i).node, links.get(i).weight);
					}
          
					LinkArrayWritable links3 = new LinkArrayWritable();
					links3.set(links2);
						
          if(node == startnode){
            istateWriter.append(new IntWritable(node), new FloatWritable(0));
            cstateWriter.append(new IntWritable(node), new FloatWritable(0));
            staticWriter.append(new IntWritable(node), links3);
            priorityqueueWriter.append(new IntWritable(node), new FloatWritable(0), links3);
            
            hasStart = true;
          }else{
            istateWriter.append(new IntWritable(node), new FloatWritable(max));
            cstateWriter.append(new IntWritable(node), new FloatWritable(max));
            staticWriter.append(new IntWritable(node), links3);
          }

					records++;
				}
			}
      
      //to avoid no priority queue exception
      if(!hasStart){
        LinkWritable[] links2 = new LinkWritable[partitions];

        for(int i=0; i<partitions; i++){
          //no use, map will skip it
          links2[i] = new LinkWritable(i, 1000);
        }
        LinkArrayWritable links3 = new LinkArrayWritable();
        links3.set(links2);
        priorityqueueWriter.append(new IntWritable(-1), new FloatWritable(0), links3);
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
	public FloatWritable decidePriority(IntWritable key, FloatWritable iState) {
		return new FloatWritable(-iState.get());
	}

	@Override
	public FloatWritable decideTopK(IntWritable key, FloatWritable cState) {
		return new FloatWritable(-cState.get());
	}


  @Override
  public FloatWritable updatecState(FloatWritable iState, FloatWritable cState) {
    float shorter = Math.min(iState.get(), cState.get());
    return new FloatWritable(shorter);
  }

  @Override
  public void updateiState(IntWritable key, Iterator<FloatWritable> values, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter) throws IOException {
    workload++;		
		reporter.setStatus(String.valueOf(workload));
		
    System.out.println("input: " + key);
    
		float distance = Float.MAX_VALUE;
		while(values.hasNext()){	
			float v = values.next().get();
      if(v < distance){
        distance = v;
      }
		}
		
		output.collect(key, new FloatWritable(distance));
  }
}
