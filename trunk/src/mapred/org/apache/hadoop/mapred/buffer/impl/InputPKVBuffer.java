package org.apache.hadoop.mapred.buffer.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.InputCollector;
import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.buffer.BufferUmbilicalProtocol;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

public class InputPKVBuffer<K extends Object, V extends Object> implements
		InputCollector<K, V> {

	private static final Log LOG = LogFactory.getLog(InputPKVBuffer.class.getName());
	
	private boolean stopSignal = false;	
	private Task task;
	private TaskAttemptID taskAttemptID;
	private int iteration = 0;
	private K savedKey;
	private V savedValue;			//for get K, V pair
	private OutputFile.Header savedHeader = null;
	
	private Deserializer keyDeserializer;	
	private Deserializer valDeserializer;
	
	private JobConf job;
	
	private boolean bSort;
	private Queue<KVRecord<K, V>> recordsQueue = null;
		
	//private IterativeReducer iterReducer = null;
	
	@SuppressWarnings("unchecked")
	public InputPKVBuffer(BufferUmbilicalProtocol umbilical, Task task, JobConf job, 
			Reporter reporter, Progress progress, 
			Class<K> keyClass, Class<V> valClass){	
		
		LOG.info("InputPKVBuffer is created for task " + task.getTaskID());
		this.task = task;
		this.taskAttemptID = task.getTaskID();
		this.iteration = 0;
		
		this.job = job;
		
		SerializationFactory serializationFactory = new SerializationFactory(job);
	    this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
	    this.valDeserializer = serializationFactory.getDeserializer(valClass);
	    
	    this.recordsQueue = new LinkedList<KVRecord<K, V>>();
	  

	    //here we create 2 reducer instance, two configure() methods are invoked, and 2 process
	    //will access the reducer, may result in some problem	    
	    //this.iterReducer = (IterativeReducer)ReflectionUtils.newInstance(job.getReducerClass(), job);
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void free() {
		this.recordsQueue.clear();
		this.recordsQueue = null;
	}

	public synchronized boolean isStop() {
		return this.stopSignal;
	}
	
	//should be called in user-defined IterativeMapper.initPKVBuffer()
	public synchronized void init(K key, V value) throws IOException {
		synchronized(this.recordsQueue){
			KVRecord<K, V> rec = new KVRecord<K, V>(key, value);
			this.recordsQueue.add(rec);
		}
	}
	/*
	@SuppressWarnings("unchecked")
	private void squeezeBuffer(){		
		Map<K, PriorityRecord<P, K, V>> tempMap = new HashMap<K, PriorityRecord<P, K, V>>();
		
		while(!this.recordsQueue.isEmpty()){
			PriorityRecord<P, K, V> record = this.recordsQueue.poll();
			
			if(tempMap.containsKey(record.getKey())){
				PriorityRecord<P, K, V> pkvRecord = tempMap.get(record.getKey());
				V valAfterCombine = 
					(V) this.iterReducer.combine(record.getValue(), pkvRecord.getValue());
				P pri = (P)this.iterReducer.setPriority(record.getKey(), valAfterCombine);
				
				PriorityRecord<P, K, V> newRecord = new PriorityRecord<P, K, V>(pri, record.getKey(), valAfterCombine);
				tempMap.put(record.getKey(), newRecord);
			}else{
				tempMap.put(record.getKey(), record);
			}		
		}
		
		//after combination, push the recording to priority queue again
		Collection<PriorityRecord<P, K, V>> entries = tempMap.values(); 
		this.recordsQueue.addAll(entries);
		
		tempMap = null;
	}
	*/
	@SuppressWarnings("unchecked")
	@Override
	public synchronized boolean read(DataInputStream istream, OutputFile.Header header)
			throws IOException {	
		if(this.iteration <= ((OutputFile.PKVBufferHeader)header).iteration()){
			//LOG.info("queue size: " + this.recordsQueue.size());
			synchronized(this.recordsQueue){
				/*
				//wait for recordsqueue is empty, which means no last records left
				while(!this.recordsQueue.isEmpty()){
					try {
						//LOG.info(this.recordsQueue.size() + "\t" + this.recordsQueue.isEmpty());
						//LOG.info("records queue is not empty, so wait");
						
						synchronized(this.task){
							this.task.notifyAll();
						}
						this.recordsQueue.wait();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				*/
				this.iteration = ((OutputFile.PKVBufferHeader)header).iteration();
				
				IFile.Reader reader = new IFile.Reader(job, istream, header.compressed(), null, null);
				DataInputBuffer key = new DataInputBuffer();
				DataInputBuffer value = new DataInputBuffer();
				
				//if priority queue is not empty, we should do replace
				boolean replace = false;
				if(!this.recordsQueue.isEmpty()){
					replace = true;
				}
				
				while (reader.next(key, value)) {
					keyDeserializer.open(key);
					valDeserializer.open(value);
					Object keyObject = null;
					Object valObject = null;
					keyObject = keyDeserializer.deserialize(keyObject);
					valObject = valDeserializer.deserialize(valObject);
					
					//LOG.info(" read some priority KV pair: " + keyObject + " : " + valObject + " with iteration of " + this.iteration);
					
					if(replace){
						/*
						//looking for and remove someone
						Iterator<PriorityRecord<P, K, V>> itr = this.recordsQueue.iterator();
						PriorityRecord<P, K, V> newRecord = null;
						PriorityRecord<P, K, V> oldRecord = null;
						while(itr.hasNext()){
							PriorityRecord<P, K, V> record = itr.next();
							if(record.getKey().equals(keyObject)){
								//LOG.info("duplicated record found" + keyObject);
								oldRecord = record;
								V valAfterCombine = 
									(V) this.iterReducer.combine(record.getValue(), valObject);
								P pri = (P)this.iterReducer.setPriority(record.getKey(), valAfterCombine);
								newRecord = new PriorityRecord<P, K, V>(pri, record.getKey(), valAfterCombine);
							
								break;
							}					
						}
						
						if(newRecord != null){
							this.recordsQueue.remove(oldRecord);
							this.recordsQueue.add(newRecord);
						}
						else{
							this.recordsQueue.add(new PriorityRecord<P, K, V>((P)priorityObject, (K)keyObject, (V)valObject));
						}
						*/
						this.recordsQueue.add(new KVRecord<K, V>((K)keyObject, (V)valObject));
					}else{
						this.recordsQueue.add(new KVRecord<K, V>((K)keyObject, (V)valObject));
					}				
					
					//LOG.info("queue size is " + this.recordsQueue.size());
					/*
					//if queue size too large, then we squeeze it by keys
					if(this.recordsQueue.size() > 10000){
						LOG.info("queue is too large, we squeeze it");
						squeezeBuffer();
					}
					*/
				}
				
				boolean nouse = istream.readBoolean();
				this.notifyAll();			//notify MapTask's pkvBuffer.wait()
				
				return true;
			}

		}
		
		return false;
	}

	public OutputFile.Header getRecentHeader() {
		return this.savedHeader;
	}
	
	@Override
	public ValuesIterator<K, V> valuesIterator() throws IOException {
		return null;
	}

	public synchronized boolean next() {
		//LOG.info("doing next, for map . size is " + this.recordsQueue.size());
		synchronized(this.recordsQueue){
			KVRecord<K, V> record = this.recordsQueue.poll();
			if(record == null){
				this.recordsQueue.clear();
				this.recordsQueue.notifyAll();
				return false;
			}
			else{
				this.savedKey = record.k;
				this.savedValue = record.v;
				//LOG.info(" doing next, for map : " + record.getPriority() + " : " + this.savedKey +" : " + this.savedValue);
				return true;
			}
		}
	}
	
	public K getTopKey() {
		return this.savedKey;
	}
	
	public V getTopValue() {
		return this.savedValue;
	}
}
