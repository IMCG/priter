package org.apache.hadoop.mapred.buffer.impl;

import org.apache.hadoop.io.WritableComparable;

public class KVRecord<K extends Object, V extends WritableComparable> implements Comparable<KVRecord<K, V>>{
	public K k;
	public V v;
	
	public KVRecord(K inK, V inV){
		this.k = inK;
		this.v = inV;
	}
	
	@Override
	public String toString(){
		return k + "\t" + v;
	}

	@Override
	public int compareTo(KVRecord<K, V> o) {
		return this.v.compareTo(o.v);
	}


}
