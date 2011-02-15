package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RedoTaskAction extends TaskTrackerAction {
	TaskAttemptID taskId;
	int checkpoint;
	  
	  public RedoTaskAction() {
	    super(ActionType.REDO_TASK);
	  }
	  
	  public RedoTaskAction(TaskAttemptID taskId, int checkpoint) {
	    super(ActionType.REDO_TASK);
	    this.taskId = taskId;
	    this.checkpoint = checkpoint;
	  }

	  public TaskAttemptID getTaskID() {
	    return taskId;
	  }
	  
	  public int getCheckPoint(){
		  return checkpoint;
	  }
	  
	  @Override
	  public void write(DataOutput out) throws IOException {
		  out.writeInt(checkpoint);
		  taskId.write(out);
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
		  checkpoint = in.readInt();
		  taskId = TaskAttemptID.read(in);
	  }
}
