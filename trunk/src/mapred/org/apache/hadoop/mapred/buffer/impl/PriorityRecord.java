package org.apache.hadoop.mapred.buffer.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Valueable;
import org.apache.hadoop.io.Writable;

public class PriorityRecord<P extends Valueable, V extends Valueable>
				implements Writable, Comparable<PriorityRecord<P, V>> {
	private P priority;
	private V iState;
	private V cState;
		
	public PriorityRecord() {
		super();
	}
	
	public PriorityRecord(P inpriority, V iniState, V incState) {
		super();
		priority = inpriority;
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
	public int hashCode() {
		return this.priority.hashCode();
	}
	
	public P getPriority(){
		return this.priority;
	}
	
	public V getiState() {
		return this.iState;
	}
	
	public V getcState() {
		return this.cState;
	}
	
	public void setPriority(P inpriority) {
		this.priority = inpriority;
	}
	
	public void setiState(V iniState) {
		this.iState = iniState;
	}
	
	public void setcState(V incState) {
		this.cState = incState;
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

	@Override
	public int compareTo(PriorityRecord<P, V> o) {
		return this.priority.compareTo(o.priority);
	}
}
