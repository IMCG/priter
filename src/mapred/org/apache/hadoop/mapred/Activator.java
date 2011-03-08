package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;

public interface Activator<P extends WritableComparable, V> extends JobConfigurable {
	/*
	 * for activate node, that is map
	 */
	
	void initStarter(InputPKVBuffer<V> starter) throws IOException;
	
	void activate(IntWritable nodeid, V value, OutputCollector<IntWritable, V> output, Reporter reporter) throws IOException;
	
	void iterate();
}
