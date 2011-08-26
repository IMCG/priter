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
  long total_updates = 0;
  double total_curr = 0;
	float obj = 0;
	//String outputPath = new String();
	JobID jobID = new JobID();
	
	public SnapshotCompletionEvent() {};
	
	public SnapshotCompletionEvent(int snapshotindex, int iterindex, int index, boolean update, long updates, double totalF2, float obj, JobID jobid) {
		this.snaphostIndex = snapshotindex;
		this.iterIndex = iterindex;
		this.tasktrackerIndex = index;
		this.update = update;
    this.total_updates = updates;
    this.total_curr = totalF2;
		this.obj = obj;
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
  public long getPartialUpdates(){
		return this.total_updates;
	}
  public double getPartialF2(){
		return this.total_curr;
	}
	public float getObj(){
		return this.obj;
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
    this.total_updates = in.readLong();
    this.total_curr = in.readDouble();
		this.obj = in.readFloat();
		this.jobID.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.snaphostIndex);
		out.writeInt(this.iterIndex);
		out.writeInt(this.tasktrackerIndex);
		out.writeBoolean(this.update);
    out.writeLong(this.total_updates);
    out.writeDouble(this.total_curr);
		out.writeFloat(this.obj);
		this.jobID.write(out);
	}

	@Override
	public String toString() {
		return new String("snapshot " + this.snaphostIndex + " iter " + this.iterIndex + " : tasktracker index is " + this.tasktrackerIndex);
	}
}
