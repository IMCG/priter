package org.apache.hadoop.mapred.buffer.impl;

import org.apache.hadoop.io.Writable;

public interface StateTableIterator<K extends Writable, V extends Writable>  {

	void performTerminationCheck();
	K getKey();
	V getiState();
	V getcState();
	boolean next();
}
