package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;

public interface Updator<P extends WritableComparable, V> extends JobConfigurable {
	/*
	 * for update node state, that is reduce
	 */
	
	void initStateTable(OutputPKVBuffer<P, V> stateTable);
	V resetiState();
	P decidePriority(IntWritable key, V iState, boolean iornot);
	void updateState(IntWritable key, Iterator<V> values, OutputPKVBuffer<P, V> stateTable, Reporter reporter) throws IOException;	
	
	void iterate();
	P obj();
}
