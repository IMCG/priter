package org.apache.hadoop.examples.priorityiteration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.lib.HashPartitioner;

public class UniDistIntPartitioner<V> extends HashPartitioner<IntWritable, V> {
	  /** Use {@link Object#hashCode()} to partition. */
	  public int getPartition(IntWritable key, V value,
	                          int numReduceTasks) {
	    return key.get() % numReduceTasks;
	  }
}
