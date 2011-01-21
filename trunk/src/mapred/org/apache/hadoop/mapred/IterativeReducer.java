package org.apache.hadoop.mapred;


import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;
import org.apache.hadoop.mapred.buffer.impl.StateTableIterator;

public interface IterativeReducer<K2, V2, K3 extends Writable, V3 extends WritableComparable, P extends WritableComparable> extends Reducer<K2,V2,K3,V3> {
	
	/**
	 * generate snapshot, and also judge if we should stop
	 * @param writer
	 * @param records
	 * @return stop signal, true: going on, false: stop
	 */	
	void initStateTable(OutputPKVBuffer<P, K3, V3> stateTable);
	K3 setDefaultKey();
	V3 setDefaultiState();
	V3 setDefaultcState(K3 k);
	P setPriority(K3 key, V3 iState);
	void updateState(V3 iState, V3 cState, V3 value);	
	
	void reduce(K2 key, Iterator<V2> values,
			OutputPKVBuffer<P, K3, V3> output, Reporter reporter)
    				throws IOException;
	
	void iterate();
	V3 setThreshold(Map<K3, PriorityRecord<P, V3>> stateTable, long processTime, long overheadTime);
	boolean stopCheck(StateTableIterator<K3, V3> stateTable);
}
