package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IterationCompletionEvent implements Writable {

	int iterationNum;
	int taskid;
	JobID jobID;
	
	public IterationCompletionEvent(int iterNum, int id, JobID job){
		iterationNum = iterNum;
		taskid = id;
		jobID = job;
	}
	
	public int getIteration(){
		return iterationNum;
	}
	
	public int gettaskID(){
		return taskid;
	}
	
	public JobID getJob(){
		return jobID;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.iterationNum = in.readInt();
		this.taskid = in.readInt();
		this.jobID.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.iterationNum);
		out.writeInt(this.taskid);
		this.jobID.write(out);
	}

}
