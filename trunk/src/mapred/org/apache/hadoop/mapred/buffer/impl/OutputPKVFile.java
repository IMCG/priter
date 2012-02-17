package org.apache.hadoop.mapred.buffer.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Valueable;
import org.apache.hadoop.mapred.FileBasedUpdater;
import org.apache.hadoop.mapred.FileHandle;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.buffer.BufferUmbilicalProtocol;
import org.apache.hadoop.util.Progress;

public class OutputPKVFile<K extends Object, P extends Valueable, V extends Valueable, D extends Object> 
			implements OutputCollector<K, V> {
	private static final Log LOG = LogFactory.getLog(OutputPKVFile.class.getName());
	
	private FileHandle outputHandle;
	private TaskAttemptID taskAttemptID;
	private IFile.Writer<K, V> writer;
	private FileBasedUpdater<K, P, V, D> updater;
	private HashMap<K, P> samples;
	private int SAMPLESIZE;
	private boolean bPortion = false;
	private boolean bLength = false;
	private double queueportion = 0.2;
	private int queuelen = 0;
	private int counter = 0;
	private int checkInterval;
	private int totalRecords;
	
	public OutputPKVFile(BufferUmbilicalProtocol umbilical, Task task, JobConf job, 
			Reporter reporter, Progress progress, 
			Class<K> keyClass, Class<P> priClass, Class<V> valClass, 
			FileBasedUpdater<K, P, V, D> updater) throws IOException{
		
		FileSystem hdfs = FileSystem.get(job);
		outputHandle = new FileHandle(task.getJobID());
    	outputHandle.setConf(job);
    	taskAttemptID = task.getTaskID();
		Path intermediatefile = outputHandle.getiStateFile4Write(taskAttemptID);
		this.writer = new IFile.Writer<K, V>(job, hdfs, intermediatefile, keyClass, valClass, null, null);
		this.updater = updater;
		
		SAMPLESIZE = job.getInt("priter.job.samplesize", 1000);
		checkInterval = totalRecords / SAMPLESIZE - 1;
		samples = new HashMap<K, P>();
		
		if(job.getFloat("priter.queue.portion", -1) != -1){
			this.bPortion = true;
			this.queueportion = job.getFloat("priter.queue.portion", 1);
		}else if(job.getInt("priter.queue.length", -1) != -1){
			this.bLength = true;
			this.queuelen = job.getInt("priter.queue.length", 10000);
		}
	}
	
	@Override
	public void collect(K key, V value) throws IOException {
		writer.append(key, value);
		if(counter % checkInterval == 0){
			P pri = updater.decidePriority(key, value);
			samples.put(key, pri);
		}
	}
	
	public P getPriorityThreshold(){
		//retrieve the priority threshold at the same time
		
		final Map<K, P> langForSort = samples;
		List<K> randomkeys = new ArrayList<K>(samples.keySet());
		Collections.sort(randomkeys, 
				new Comparator(){
					public int compare(Object left, Object right){
						P leftrecord = langForSort.get(left);
						P rightrecord = langForSort.get(right);
						return -leftrecord.compareTo(rightrecord);
					}
				});
		
		if(this.bPortion){
			int actualqueuelen = (int) (this.totalRecords * this.queueportion);
			
			int cutindex = (int) this.queueportion * SAMPLESIZE;
			P threshold = samples.get((randomkeys.get(cutindex-1>=0?cutindex-1:0)));
			
			LOG.info("queuelen " + actualqueuelen + " eliglbe records " + totalRecords + 
					" cut index " + cutindex + " threshold is " + threshold);
			return threshold;
		} else{
			int cutindex = queuelen * SAMPLESIZE / totalRecords;
			P threshold = samples.get((randomkeys.get(cutindex-1>=0?cutindex-1:0)));
			
			LOG.info("queuelen " + queuelen + " eliglbe records " + totalRecords + 
					" cut index " + cutindex + " threshold is " + threshold);
			return threshold;
		}	
	}
	
	public void close() throws IOException{
		writer.close();
		
		//rename the istatefile_for_write to istatefile_for_read
		Path file4write = outputHandle.getiStateFile4Write(taskAttemptID);
		Path file4read = outputHandle.getiStateFile4Read(taskAttemptID);
		File toBeRenamed = new File(file4write.getName());
		File newfile = new File(file4read.getName());
		if(!toBeRenamed.renameTo(newfile)){
			LOG.error("error renaming file " + toBeRenamed + " to " + newfile);
		}
	}

}
