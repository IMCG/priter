package org.apache.hadoop.mapred;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;

public interface IterativeReducer<K2, V2, K3, V3,  P extends WritableComparable> 
	extends Reducer<K2, V2, K3, V3> {
	
	/**
	 * generate snapshot, and also judge if we should stop
	 * @param writer
	 * @param records
	 * @return stop signal, true: going on, false: stop
	 */	
	void initStateTable(OutputPKVBuffer<P, V3> stateTable);
	IntWritable setDefaultKey();
	V3 setDefaultiState();
	V3 setDefaultcState(IntWritable k);
	P decidePriority(IntWritable key, V3 iState, boolean iornot);
	void updateState(K2 key, Iterator<V2> values, OutputPKVBuffer<P, V3> stateTable, Reporter reporter) throws IOException;	
	
	void iterate();
}
