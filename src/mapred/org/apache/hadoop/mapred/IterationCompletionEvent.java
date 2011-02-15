package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IterationCompletionEvent implements Writable {

	int iterationNum = 0;
	int taskid = 0;
	int checkpoint = 0;
	JobID jobID = new JobID();
	
	public IterationCompletionEvent() {};
	
	public IterationCompletionEvent(int iterNum, int id, int inIter, JobID job){
		iterationNum = iterNum;
		taskid = id;
		checkpoint = inIter;
		jobID = job;
	}
	
	public int getIteration(){
		return iterationNum;
	}
	
	public int gettaskID(){
		return taskid;
	}
	
	public int getCheckPoint(){
		return checkpoint;
	}
	
	public JobID getJob(){
		return jobID;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.iterationNum = in.readInt();
		this.taskid = in.readInt();
		this.checkpoint = in.readInt();
		this.jobID.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.iterationNum);
		out.writeInt(this.taskid);
		out.writeInt(this.checkpoint);
		this.jobID.write(out);
	}

}
