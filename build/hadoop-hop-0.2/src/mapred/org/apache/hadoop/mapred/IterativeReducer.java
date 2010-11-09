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
	
	P[] bufSplit(P min, P max, boolean init);
	
	int bucketTransfer(int[] bucketSize);
	
	void defaultKV(K3 k, V3 v);
	
	P bound(P min, P max);
	
	void combine(P priority1, V3 value1, P priority2, V3 value2, PriorityRecord<P, V3> output);
	
	//get priority function
	P setPriority(K3 key, V3 value);
	
	void reduce(K2 key, Iterator<V2> values,
			OutputPKVBuffer<P, K3, V3> output, Reporter reporter)
    				throws IOException;
	
	void iterate();
}
