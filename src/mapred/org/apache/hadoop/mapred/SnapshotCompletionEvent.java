package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class SnapshotCompletionEvent implements Writable {

	int snaphostIndex = 0;
	int iterIndex = 0;
	int tasktrackerIndex = 0;
	boolean update = true;
	//String outputPath = new String();
	JobID jobID = new JobID();
	
	public SnapshotCompletionEvent() {};
	
	public SnapshotCompletionEvent(int snapshotindex, int iterindex, int index, boolean update, JobID jobid) {
		this.snaphostIndex = snapshotindex;
		this.iterIndex = iterindex;
		this.tasktrackerIndex = index;
		this.update = update;
		//this.outputPath = output;
		this.jobID = jobid;
	}
	
	public int getSnapshotIndex() {
		return this.snaphostIndex;
	}
	
	public int getIterIndex(){
		return this.iterIndex;
	}
	
	public int getTaskIndex() {
		return this.tasktrackerIndex;
	}
	public boolean getUpdate(){
		return this.update;
	}
	public JobID getJobID() {
		return this.jobID;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.snaphostIndex = in.readInt();
		this.iterIndex = in.readInt();
		this.tasktrackerIndex = in.readInt();
		this.update = in.readBoolean();
		this.jobID.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.snaphostIndex);
		out.writeInt(this.iterIndex);
		out.writeInt(this.tasktrackerIndex);
		out.writeBoolean(this.update);
		this.jobID.write(out);
	}

	@Override
	public String toString() {
		return new String("snapshot " + this.snaphostIndex + " iter " + this.iterIndex + " : tasktracker index is " + this.tasktrackerIndex);
	}
}