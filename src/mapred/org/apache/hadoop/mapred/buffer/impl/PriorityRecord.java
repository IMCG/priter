package org.apache.hadoop.mapred.buffer.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PriorityRecord<V extends Object>
				implements Writable {
	private V iState;
	private V cState;
		
	public PriorityRecord() {
		super();
	}
	
	public PriorityRecord(V iniState, V incState) {
		super();
		iState = iniState;
		cState = incState;
	}

	public PriorityRecord(PriorityRecord<V> record) {
		super();
		iState = record.iState;
		cState = record.cState;
	}

	@Override
	public int hashCode() {
		return this.iState.hashCode();
	}
	
	public V getiState() {
		return this.iState;
	}
	
	public V getcState() {
		return this.cState;
	}
	
	public void setiState(V iniState) {
		this.iState = iniState;
	}
	
	public void setcState(V incState) {
		this.cState = incState;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		((Writable) this.iState).readFields(in);
		((Writable) this.cState).readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		((Writable)this.iState).write(out);
		((Writable)this.cState).write(out);
	}

	@Override
	public String toString(){
		return new String(iState + "\t" + cState);
	}
}
