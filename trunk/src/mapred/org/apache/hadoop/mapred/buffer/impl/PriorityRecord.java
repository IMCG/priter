package org.apache.hadoop.mapred.buffer.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PriorityRecord<P extends Object, V extends Object>
				implements Writable, Comparable<PriorityRecord<P, V>> {
	private P priority;
	private V iState;
	private V cState;
		
	public PriorityRecord() {
		super();
	}
	
	public PriorityRecord(P p, V iniState, V incState) {
		super();
		
		priority = p;
		iState = iniState;
		cState = incState;
	}

	public PriorityRecord(PriorityRecord<P, V> record) {
		super();
		
		priority = record.priority;
		iState = record.iState;
		cState = record.cState;
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
		return this.iState.hashCode();
	}
	
	@Override
	public int compareTo(PriorityRecord<P, V> other) {
		
		return ((WritableComparable)this.priority).compareTo(other.priority);
	}
	
	public V getiState() {
		return this.iState;
	}
	
	public V getcState() {
		return this.cState;
	}
	
	public P getPriority() {
		return this.priority;
	}
	
	public void setiState(V iniState) {
		this.iState = iniState;
	}
	
	public void setcState(V incState) {
		this.cState = incState;
	}
	
	public void setPriority(P p) {
		this.priority = p;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		((Writable) this.priority).readFields(in);
		((Writable) this.iState).readFields(in);
		((Writable) this.cState).readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		((Writable)this.priority).write(out);
		((Writable)this.iState).write(out);
		((Writable)this.cState).write(out);
	}

	@Override
	public String toString(){
		return new String(priority + "\t" + iState + "\t" + cState);
	}
}
