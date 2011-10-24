package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;

public interface Updater<K, P extends WritableComparable, V> extends JobConfigurable {
	/*
	 * for update node state, that is reduce
	 */
	
	void initStateTable(OutputPKVBuffer<K, P, V> stateTable);
	V resetiState();
	P decidePriority(K key, V iState);
	P decideTopK(K key, V cState);
	void updateState(K key, Iterator<V> values, OutputPKVBuffer<K, P, V> stateTable, Reporter reporter) throws IOException;	
	
	void iterate();
}
