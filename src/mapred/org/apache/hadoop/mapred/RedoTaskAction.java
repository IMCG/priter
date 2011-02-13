package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RedoTaskAction extends TaskTrackerAction {
	  int taskId;
	  
	  public RedoTaskAction() {
	    super(ActionType.REDO_TASK);
	  }
	  
	  public RedoTaskAction(int taskId) {
	    super(ActionType.REDO_TASK);
	    this.taskId = taskId;
	  }

	  public int getTaskID() {
	    return taskId;
	  }
	  
	  @Override
	  public void write(DataOutput out) throws IOException {
	    out.writeInt(taskId);
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    taskId = in.readInt();
	  }
}
