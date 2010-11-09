package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;

public interface IterativeMapper<P, K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2> {
	/**
	 * for loading the initial vector to priority-key-value buffer, user should
	 * use pkvBuffer.collect(priority, K, V) to initialize the priorityKVBuffer
	 * @param pkvBuffer
	 * @throws IOException
	 */
	void initPKVBuffer(InputPKVBuffer<K1, V1> pkvBuffer) throws IOException;
	void iterate();
}
