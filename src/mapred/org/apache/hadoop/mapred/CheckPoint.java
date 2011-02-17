package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CheckPoint implements Writable {

	private int iter = 0;
	private int snapshot = 0;
	
	public CheckPoint(){}
	
	public CheckPoint(int iter, int snapshot){
		this.iter = iter;
		this.snapshot = snapshot;
	}
	
	public int getIter(){
		return iter;
	}
	
	public int getSnapshot() {
		return snapshot;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.iter = in.readInt();
		this.snapshot = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.iter);
		out.writeInt(this.snapshot);
	}

	@Override
	public String toString(){
		return "iter " + this.iter + " snapshot " + this.snapshot;
	}
}
