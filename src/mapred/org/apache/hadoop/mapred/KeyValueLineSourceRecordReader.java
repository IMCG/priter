package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class KeyValueLineSourceRecordReader implements RecordReader<Text, Text> {
	  private final LineRecordReader lineRecordReader;

	  private byte separator = (byte) '\t';

	  private LongWritable dummyKey;

	  private Text innerValue;
	  
	  private String sourceFile;
	  

	  public Class getKeyClass() { return Text.class; }
	  
	  public Text createKey() {
	    return new Text();
	  }
	  
	  public Text createValue() {
	    return new Text();
	  }

	  public KeyValueLineSourceRecordReader(Configuration job, FileSplit split)
	    throws IOException {
	    
	    lineRecordReader = new LineRecordReader(job, split);
	    dummyKey = lineRecordReader.createKey();
	    innerValue = lineRecordReader.createValue();
	    String sepStr = job.get("key.value.separator.in.input.line", "\t");
	    this.separator = (byte) sepStr.charAt(0);
	    this.sourceFile = split.getPath().getParent().getName() + ":";
	  }
	  
	  public static int findSeparator(byte[] utf, int start, int length, byte sep) {
	    for (int i = start; i < (start + length); i++) {
	      if (utf[i] == sep) {
	        return i;
	      }
	    }
	    return -1;
	  }

	  /** Read key/value pair in a line. */
	  public synchronized boolean next(Text key, Text value)
	    throws IOException {
	    Text tKey = key;
	    Text tValue = value;
	    byte[] line = null;
	    int lineLen = -1;
	    if (lineRecordReader.next(dummyKey, innerValue)) {
	      line = innerValue.getBytes();
	      lineLen = innerValue.getLength();
	    } else {
	      return false;
	    }
	    if (line == null)
	      return false;
	    int pos = findSeparator(line, 0, lineLen, this.separator);
	    if (pos == -1) {
	      tKey.set(line, 0, lineLen);
	      tValue.set("");
	    } else {
	      int keyLen = pos;
	      byte[] keyBytes = new byte[keyLen];
	      System.arraycopy(line, 0, keyBytes, 0, keyLen);
	      int valLen = lineLen - keyLen - 1;
	      int labelLen = this.sourceFile.getBytes().length;
	      byte[] valBytes = new byte[valLen + labelLen];
	      System.arraycopy(this.sourceFile.getBytes(), 0, valBytes, 0, labelLen);
	      System.arraycopy(line, pos + 1, valBytes, labelLen, valLen);
	      tKey.set(keyBytes);
	      tValue.set(valBytes);
	    }
	    return true;
	  }
	  
	  //for reading priority key value pairs
	  public synchronized boolean next(Text pri, Text key, Text value)
		  	throws IOException {
		  Text tPri = pri;
		  Text tKey = key;
		  Text tValue = value;
		  
		  byte[] line = null;
		  int lineLen = -1;
		  if (lineRecordReader.next(dummyKey, innerValue)) {
		    line = innerValue.getBytes();
		    lineLen = innerValue.getLength();
		  } else {
		    return false;
		  }
		  if (line == null)
		    return false;
		  
		  System.out.println(line + "\t" + lineLen);
		  int pos1 = findSeparator(line, 0, lineLen, this.separator);
		  int pos2 = findSeparator(line, pos1+1, lineLen, this.separator);
		  if((pos1 == -1) || (pos2 == -1)) {
			  tPri.set("");
			  tKey.set("");
			  tValue.set("");
		  } else {
			  int priorityLen = pos1;
			  byte[] priorityBytes = new byte[priorityLen];
			  System.arraycopy(line, 0, priorityBytes, 0, priorityLen);
			  int keyLen = pos2 - pos1 - 1;
			  byte[] keyBytes = new byte[keyLen];
			  System.arraycopy(line, pos1+1, keyBytes, 0, keyLen);
			  int valLen = lineLen - keyLen - priorityLen - 2;
			  byte[] valBytes = new byte[valLen];
			  System.arraycopy(line, pos2 + 1, valBytes, 0, valLen);
			  /*
			  System.out.println(priorityBytes + "\t" + priorityLen);
			  priority.reset(line, 0, priorityLen);
			  System.out.println(priority.getPosition() + "\t" + priority.getLength() + "\t" + priority.getData());
			  */
			  tPri.set(priorityBytes);
			  tKey.set(keyBytes);
			  tValue.set(valBytes);
		  }
		  return true;
		}
	  
	  public float getProgress() {
	    return lineRecordReader.getProgress();
	  }
	  
	  public synchronized long getPos() throws IOException {
	    return lineRecordReader.getPos();
	  }

	  public synchronized void close() throws IOException { 
	    lineRecordReader.close();
	  }
}
