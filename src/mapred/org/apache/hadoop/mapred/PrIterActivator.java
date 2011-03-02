package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;

public interface PrIterActivator<P extends WritableComparable, V> extends JobConfigurable {
	/*
	 * for activate node, that is map
	 */
	
	void initPKVBuffer(InputPKVBuffer<IntWritable, V> starter) throws IOException;
	
	void iterate();
}
