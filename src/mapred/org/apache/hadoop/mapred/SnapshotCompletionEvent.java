package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class SnapshotCompletionEvent implements Writable {

	int iteration = 0;
	int tasktrackerIndex = 0;
	//String outputPath = new String();
	JobID jobID = new JobID();
	boolean stop = false;
	
	public SnapshotCompletionEvent() {};
	
	public SnapshotCompletionEvent(int iter, int index, JobID jobid, boolean stop) {
		this.iteration = iter;
		this.tasktrackerIndex = index;
		//this.outputPath = output;
		this.jobID = jobid;
		this.stop = stop;
	}
	
	public int getIteration() {
		return this.iteration;
	}
	
	public int getTaskIndex() {
		return this.tasktrackerIndex;
	}
	
	public JobID getJobID() {
		return this.jobID;
	}
	
	public boolean getStop() {
		return this.stop;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.iteration = in.readInt();
		this.tasktrackerIndex = in.readInt();
		this.jobID.readFields(in);
		this.stop = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.iteration);
		out.writeInt(this.tasktrackerIndex);
		this.jobID.write(out);
		out.writeBoolean(this.stop);
	}

	@Override
	public String toString() {
		return new String("iteration is " + this.iteration + " : tasktracker index is " + this.tasktrackerIndex);
	}
}
