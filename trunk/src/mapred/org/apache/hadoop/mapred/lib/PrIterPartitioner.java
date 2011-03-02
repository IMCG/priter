package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.io.IntWritable;

public class PrIterPartitioner<V> extends HashPartitioner<IntWritable, V> {
	public int getPartition(IntWritable key, V value, int numReduceTasks) {
		  return key.get() % numReduceTasks;
	}
}
