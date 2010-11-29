package org.apache.hadoop.mapred;


import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;

public interface IterativeReducer<P extends Writable, K2, V2, K3 extends Writable, V3 extends Writable> extends Reducer<K2, V2, K3, V3> {
	
	/**
	 * generate snapshot, and also judge if we should stop
	 * @param writer
	 * @param records
	 * @return stop signal, true: going on, false: stop
	 */
	boolean snapshot(BufferedWriter writer) throws IOException;
	
	//get priority function
	P setPriority(V3 iState);
	V3 setDefaultiState();
	V3 updateState(V3 oldState, V3 value);
	void defaultKV(K3 key, V3 value);
	
	void reduce(K2 key, Iterator<V2> values,
			OutputPKVBuffer<P, K3, V3> output, Reporter reporter)
    				throws IOException;
	
	void iterate();
}
