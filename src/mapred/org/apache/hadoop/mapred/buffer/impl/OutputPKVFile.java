package org.apache.hadoop.mapred.buffer.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
	
	class KPRecord<K, P>{
		K key;
		P pri;
		
		public KPRecord(K k, P p){
			key = k;
			pri = p;
		}
	}
	
	private JobConf job;
	private FileHandle outputHandle;
	private FileSystem localFs;
	private TaskAttemptID taskAttemptID;
	private Path intermediatefile;
	private Class<K> keyClass;
	private Class<V> valClass;
	private IFile.Writer<K, V> writer;
	private FileBasedUpdater<K, P, V, D> updater;
	private ArrayList<KPRecord<K, P>> samples;
	private int SAMPLESIZE;
	private boolean bPortion = false;
	private boolean bLength = false;
	private double queueportion = 0.2;
	private int queuelen = 0;
	private int counter = 0;
	private long checkInterval;
	private long totalRecords;
	
	public OutputPKVFile(BufferUmbilicalProtocol umbilical, Task task, JobConf job, 
			Reporter reporter, Progress progress, 
			Class<K> keyClass, Class<P> priClass, Class<V> valClass, 
			FileBasedUpdater<K, P, V, D> updater, long records) throws IOException{
		
		this.job = job;
		this.localFs = FileSystem.getLocal(job);
		this.outputHandle = new FileHandle(task.getJobID());
    	this.outputHandle.setConf(job);
    	this.taskAttemptID = task.getTaskID();
    	this.keyClass = keyClass;
    	this.valClass = valClass;
		this.intermediatefile = outputHandle.getiStateFileIntermediate(taskAttemptID);
		this.writer = new IFile.Writer<K, V>(job, localFs, intermediatefile, keyClass, valClass, null, null);
		this.updater = updater;
		
		totalRecords = records;
		SAMPLESIZE = job.getInt("priter.job.samplesize", 1000);
		checkInterval = totalRecords / SAMPLESIZE + 1;
		samples = new ArrayList<KPRecord<K, P>>(totalRecords > SAMPLESIZE ? SAMPLESIZE : (int)totalRecords);
		
    LOG.info("checkinterval is " + checkInterval);
		if(job.getFloat("priter.queue.portion", -1) != -1){
			this.bPortion = true;
			this.queueportion = job.getFloat("priter.queue.portion", 1);
		}else if(job.getInt("priter.queue.length", -1) != -1){
			this.bLength = true;
			this.queuelen = job.getInt("priter.queue.length", 10000);
		}
	}
	
	public void initIntermediateFile() throws IOException{
		this.writer = new IFile.Writer<K, V>(job, localFs, intermediatefile, keyClass, valClass, null, null);
	}
	
	@Override
	public void collect(K key, V value) throws IOException {
		writer.append(key, value);
		if(++counter % checkInterval == 0){
			P pri = updater.decidePriority(key, value);
			samples.add(new KPRecord<K, P>(key, pri));
		}
	}
	
	public P getPriorityThreshold(){
		//retrieve the priority threshold at the same time
		Collections.sort(samples, 
				new Comparator(){
					public int compare(Object left, Object right){
						P leftrecord = ((KPRecord<K, P>)left).pri;
						P rightrecord = ((KPRecord<K, P>)right).pri;
						return -leftrecord.compareTo(rightrecord);
					}
				});
		
		if(counter > Long.MAX_VALUE / 2) counter = 0;
		if(this.bPortion){
			int actualqueuelen = (int) (this.totalRecords * this.queueportion);
			
			int cutindex = (int) (queueportion * samples.size());
			P threshold = samples.get(cutindex-1>=0?cutindex-1:0).pri;
			
			LOG.info("1queuelen " + actualqueuelen + " eliglbe records " + totalRecords + " sample size " + samples.size()
					+ " cut index " + cutindex + " threshold is " + threshold);
			samples.clear();
			return threshold;
		} else{
			int cutindex = (int) (queuelen * samples.size() / totalRecords);
			P threshold = samples.get(cutindex-1>=0?cutindex-1:0).pri;

      
			LOG.info("2queuelen " + queuelen + " eliglbe records " + totalRecords + 
					" cut index " + cutindex + " threshold is " + threshold);
			samples.clear();
			return threshold;
		}	
	}
	
	public void close() throws IOException{
		writer.close();
		/*
		 * renaming files results in crc checksum problems, I change method
		//rename the istatefile_for_write to istatefile_for_read
		Path file4write = outputHandle.getiStateFile4Write(taskAttemptID);
		Path file4read = outputHandle.getiStateFile4Read(taskAttemptID);
		File toBeRenamed = new File(file4write.getParent()+"/"+file4write.getName());
		File newfile = new File(file4read.getParent()+"/"+file4read.getName());
		LOG.info("renaming file " + toBeRenamed + " to file " + newfile);
		if(!toBeRenamed.renameTo(newfile)){
			LOG.error("error renaming file " + toBeRenamed + " to " + newfile);
		}
		*/
	}

}
