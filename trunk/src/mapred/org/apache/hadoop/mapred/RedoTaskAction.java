package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RedoTaskAction extends TaskTrackerAction {
	TaskAttemptID taskId;
	int checkpointIter = 0;
	int checkpointSnapshot = 0;
	  
	  public RedoTaskAction() {
	    super(ActionType.REDO_TASK);
	  }
	  
	  public RedoTaskAction(TaskAttemptID taskId, int iter, int snapshot) {
	    super(ActionType.REDO_TASK);
	    this.taskId = taskId;
	    this.checkpointIter = iter;
	    this.checkpointSnapshot = snapshot;
	  }

	  public TaskAttemptID getTaskID() {
	    return taskId;
	  }
	  
	  public int getIterCheckPoint(){
		  return checkpointIter;
	  }
	  
	  public int getSnapshotCheckpoint(){
		  return checkpointSnapshot;
	  }
	  
	  @Override
	  public void write(DataOutput out) throws IOException {
		  out.writeInt(checkpointIter);
		  out.writeInt(checkpointSnapshot);
		  taskId.write(out);
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
		  checkpointIter = in.readInt();
		  checkpointSnapshot = in.readInt();
		  taskId = TaskAttemptID.read(in);
	  }
}
