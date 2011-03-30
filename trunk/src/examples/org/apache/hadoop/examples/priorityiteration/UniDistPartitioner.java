package org.apache.hadoop.examples.priorityiteration;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;


public class UniDistPartitioner<K2, V2> implements Partitioner<K2, V2> {
	
	  /** Use {@link Object#hashCode()} to partition. */
	  public int getPartition(K2 key, V2 value,
	                          int numReduceTasks) {
		  return Integer.parseInt(key.toString()) % numReduceTasks;
	  }

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}
}
