package org.apache.hadoop.mapred;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;

public interface IterativeReducer<P extends Writable, K2, V2, K3 extends Writable, V3 extends Writable> extends Reducer<K2, V2, K3, V3> {
	
	/**
	 * generate snapshot, and also judge if we should stop
	 * @param writer
	 * @param records
	 * @return stop signal, true: going on, false: stop
	 */	
	void initStateTable(OutputPKVBuffer<P, K3, V3> stateTable);
	P setPriority(V3 iState);
	K3 setDefaultKey();
	V3 setDefaultiState();
	void updateState(V3 iState, V3 cState, V3 value);	
	
	void reduce(K2 key, Iterator<V2> values,
			OutputPKVBuffer<P, K3, V3> output, Reporter reporter)
    				throws IOException;
	
	void iterate();
}
