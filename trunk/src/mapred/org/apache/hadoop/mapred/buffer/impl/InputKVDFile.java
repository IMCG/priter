package org.apache.hadoop.mapred.buffer.impl;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.FileBasedActivator;
import org.apache.hadoop.mapred.FileHandle;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.InputCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.IFile.PriorityQueueReader;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapred.buffer.OutputFile.Header;

public class InputKVDFile<K extends Object, P extends WritableComparable, V extends WritableComparable, D extends Object> implements InputCollector<K, V>{

	private static final Log LOG = LogFactory.getLog(InputPKVBuffer.class.getName());
	public int iteration = 0;
	private JobConf job;
	private FileSystem hdfs;
	private final FileSystem localFs;
	private FileHandle outputHandle = null;
	private TaskAttemptID taskAttemptID;
	private Class<K> keyClass;
	private Class<V> valClass;
	private Class<D> dataClass;
	private Deserializer<K> keyDeserializer;	
	private Deserializer<V> valDeserializer;	
	private Deserializer<D> dataDeserializer;
	private K key;
	private V val;
	private D data;
	private Path priQueueFile;
	private Path signalFile;
	private JOutputBuffer<K, V> buffer;
	private Reporter reporter;
	private long lastlength = -1;
	
	private FileBasedActivator<K, P, V, D> activator;
	
	public InputKVDFile(Task task, JobConf job, Reporter reporter, JOutputBuffer<K, V> buffer,
			Class<K> keyClass, Class<V> valClass, Class<D> dataClass, 
			FileBasedActivator<K, P, V, D> activator) throws IOException{	
		
		LOG.info("InputKVDFile is created for task " + task.getTaskID());
		
		this.job = job;
		this.hdfs = FileSystem.get(job);
		this.localFs = FileSystem.getLocal(job);
    	this.outputHandle = new FileHandle(task.getJobID());
    	this.outputHandle.setConf(job);
    	this.taskAttemptID = task.getTaskID();
    	
		this.keyClass = keyClass;
		this.valClass = valClass;
		this.dataClass = dataClass;
		SerializationFactory serializationFactory = new SerializationFactory(job);
	    this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
	    this.valDeserializer = serializationFactory.getDeserializer(valClass);
	    this.dataDeserializer = serializationFactory.getDeserializer(dataClass);
	    
		this.priQueueFile = outputHandle.getPriorityQueueFile(taskAttemptID);
		this.signalFile = outputHandle.getSignalFile(taskAttemptID);
		this.activator = activator;
		this.buffer = buffer;
		this.reporter = reporter;
	}
	
	@Override
	public boolean read(DataInputStream istream, Header header)
			throws IOException {
		if(this.iteration <= ((OutputFile.PKVBufferHeader)header).iteration()){
			this.iteration = ((OutputFile.PKVBufferHeader)header).iteration();

			IFile.Reader reader = new IFile.Reader(job, istream, header.compressed(), null, null);
			DataInputBuffer key = new DataInputBuffer();
			DataInputBuffer value = new DataInputBuffer();
			
			//while (reader.next(key, value)) {}
			return true;
		}else{
			LOG.error("current iteration " + this.iteration + " but receive iteration " + ((OutputFile.PKVBufferHeader)header).iteration());
			return false;
		}
		
	}

	public long process() throws IOException{
		if(!localFs.exists(priQueueFile)) {
			throw new IOException("priorityqueue file does not exist! " + priQueueFile);
		}
		long count = 0;
		
		PriorityQueueReader<K, V, D> priqueue_reader = new PriorityQueueReader<K, V, D>(job, localFs, priQueueFile, null, null);
		
		DataInputBuffer keyIn = new DataInputBuffer();
		DataInputBuffer valueIn = new DataInputBuffer();
		DataInputBuffer staticIn = new DataInputBuffer();
		while(priqueue_reader.next(keyIn, valueIn, staticIn)){
			keyDeserializer.open(keyIn);
			valDeserializer.open(valueIn);
			dataDeserializer.open(staticIn);
			key = keyDeserializer.deserialize(key);
			val = valDeserializer.deserialize(val);
			data = dataDeserializer.deserialize(data);
			
			activator.activate(key, val, data, buffer, reporter);
			
			reporter.incrCounter(Counter.MAP_INPUT_RECORDS, 1);
			count++;
		}
		
		priqueue_reader.close();
		localFs.delete(priQueueFile, false);
		return count;
	}
	
	public boolean isReady() throws IOException{
		//ready when priority queue file has been initialized
		
		if(localFs.exists(signalFile)){
			return true;
		}else{
			return false;
		}
		
		/*
		if(localFs.exists(priQueueFile)){
			long length = localFs.getLength(priQueueFile);
			
			LOG.info("length " + length);
			if(length != lastlength){
				lastlength = length;
				return false;
			}else{
				return true;
			}
		}else{
			return false;
		}
		*/
	}
	
	@Override
	public ValuesIterator<K, V> valuesIterator() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void free() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		
	}

}
