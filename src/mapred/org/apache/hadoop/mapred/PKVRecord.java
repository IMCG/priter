package org.apache.hadoop.mapred;

import org.apache.hadoop.io.WritableComparable;

public class PKVRecord<P extends WritableComparable, K extends Object, V extends Object> implements Comparable<PKVRecord<P, K, V>>{
	public P p;
	public K k;
	public V v;
	
	public PKVRecord(P inP, K inK, V inV){
		this.p = inP;
		this.k = inK;
		this.v = inV;
	}
	
	@Override
	public String toString(){
		return p + "\t" + k + "\t" + v;
	}

	@Override
	public int compareTo(PKVRecord<P, K, V> o) {
		// TODO Auto-generated method stub
		return this.p.compareTo(o.p);
	}


}