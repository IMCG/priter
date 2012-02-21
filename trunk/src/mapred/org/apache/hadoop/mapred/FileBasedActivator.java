package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public interface FileBasedActivator<K, P extends WritableComparable, V extends WritableComparable, D> extends JobConfigurable {

	void activate(K nodeid, V value, D data, OutputCollector<K, V> output, Reporter reporter) throws IOException;
	
	void iterate();
}
