package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RollbackEvent implements Writable {

	TaskAttemptID taskid;
	
	public RollbackEvent(){
		
	}
	
	public RollbackEvent(TaskAttemptID taskid){
		this.taskid = taskid;
	}
	
	public TaskAttemptID getTaskID(){
		return this.taskid;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.taskid.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.taskid.write(out);
	}

}
