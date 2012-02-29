package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Valueable;
import org.apache.hadoop.mapred.IFile.PriorityQueueWriter;
import org.apache.hadoop.mapred.IFile.Writer;

public interface FileBasedUpdater<K, P extends Valueable, V extends Valueable, D> extends JobConfigurable {
	/*
	 * for update node state, that is reduce
	 */

	long initFiles(Writer<K, V> istateWriter, 
							Writer<K, V> cstateWriter, 
							Writer<K, D> staticWriter, 
							PriorityQueueWriter<K, V, D> priorityqueueWriter);
	V resetiState();
	P decidePriority(K key, V iState);
	P decideTopK(K key, V cState);	
	V updatecState(V iState, V cState);
	void updateiState(K key, Iterator<V> values, OutputCollector<K, V> output, Reporter reporter) throws IOException;	
	
	void iterate();
}
