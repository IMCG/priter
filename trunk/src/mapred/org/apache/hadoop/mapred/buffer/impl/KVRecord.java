package org.apache.hadoop.mapred.buffer.impl;

public class KVRecord<K extends Object, V extends Object> {
	public K k;
	public V v;
	
	public KVRecord(K inK, V inV){
		this.k = inK;
		this.v = inV;
	}
}
