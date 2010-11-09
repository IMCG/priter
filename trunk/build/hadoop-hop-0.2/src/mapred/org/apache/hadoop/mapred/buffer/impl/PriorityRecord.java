package org.apache.hadoop.mapred.buffer.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PriorityRecord<P extends Object, V extends Object>
				implements Writable, Comparable<PriorityRecord<P, V>> {
	private P priority;
	private V value;
		
	public PriorityRecord() {
		super();
	}
	
	public PriorityRecord(P p, V v) {
		super();
		
		priority = p;
		value = v;
	}

	public PriorityRecord(PriorityRecord<P, V> record) {
		super();
		
		priority = record.priority;
		value = record.value;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof PriorityRecord))
			return false;

		PriorityRecord other = (PriorityRecord) obj;
		if (other.priority.equals(this.priority))
			return true;
		else
			return false;
	}

	@Override
	public int hashCode() {
		return this.value.hashCode();
	}
	
	@Override
	public int compareTo(PriorityRecord<P, V> other) {
		
		return ((WritableComparable)this.priority).compareTo(other.priority);
	}
	
	public V getValue() {
		return this.value;
	}
	
	public P getPriority() {
		return this.priority;
	}
	
	public void setValue(V v) {
		this.value = v;
	}
	
	public void setPriority(P p) {
		this.priority = p;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		((Writable) this.priority).readFields(in);
		((Writable) this.value).readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		((Writable)this.priority).write(out);
		((Writable)this.value).write(out);
	}

	@Override
	public String toString(){
		return new String(priority + "\t" + value);
	}
}
