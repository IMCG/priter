package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class SnapshotCompletionEvent implements Writable {

	int snaphostIndex = 0;
	int iterIndex = 0;
	int tasktrackerIndex = 0;
	long total_updates = 0;
	double local_progress = 0;
	JobID jobID = new JobID();
	
	public SnapshotCompletionEvent() {};
	
	public SnapshotCompletionEvent(int snapshotindex, int iterindex, int index, long updates, double totalF2, JobID jobid) {
		this.snaphostIndex = snapshotindex;
		this.iterIndex = iterindex;
		this.tasktrackerIndex = index;
		this.total_updates = updates;
		this.local_progress = totalF2;
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
	
	public long getPartialUpdates(){
		return this.total_updates;
	}
  
  	public double getLocalProgress(){
		return this.local_progress;
	}
  
	public JobID getJobID() {
		return this.jobID;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.snaphostIndex = in.readInt();
		this.iterIndex = in.readInt();
		this.tasktrackerIndex = in.readInt();
		this.total_updates = in.readLong();
		this.local_progress = in.readDouble();
		this.jobID.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.snaphostIndex);
		out.writeInt(this.iterIndex);
		out.writeInt(this.tasktrackerIndex);
		out.writeLong(this.total_updates);
		out.writeDouble(this.local_progress);
		this.jobID.write(out);
	}

	@Override
	public String toString() {
		return new String("snapshot " + this.snaphostIndex + " iter " + this.iterIndex + " : tasktracker index is " + this.tasktrackerIndex);
	}
}
