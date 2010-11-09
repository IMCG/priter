package org.apache.hadoop.mapred.buffer.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.FileHandle;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.buffer.BufferUmbilicalProtocol;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapred.buffer.OutputFile.Header;
import org.apache.hadoop.util.Progress;


public class OutputPKVBuffer<P extends Writable, K extends Writable, V extends Writable> 
		implements OutputCollector<K, V> {
	
	
	//**************************************
	private static final Log LOG = LogFactory.getLog(OutputPKVBuffer.class.getName());
	
	private static enum PRI_TYPE{SORT, BUCKET, THRESHOLD};
	private PRI_TYPE priType;
	
	private TaskAttemptID taskAttemptID;
	private int iteration = 0;
	private int topk;
	private Class<P> priorityClass;
	private Class<K> keyClass;	
	private Class<V> valClass;
	public boolean start = false;
	
	private final FileSystem localFs;
	private FileHandle outputHandle = null;
	private JobConf job = null;
	
	//private boolean bSort;
	Serializer<P> prioritySerializer;
    Serializer<K> keySerializer;
    Serializer<V> valueSerializer;
    DataOutputBuffer buffer = new DataOutputBuffer();
    
    private IterativeReducer iterReducer = null;

	//main part of this class, storing priority-key-value records
	private Map<K, PriorityRecord<P, V>> HoldMap = new HashMap<K, PriorityRecord<P, V>>();
	private ArrayList<KVRecord<K, V>> records = new ArrayList<KVRecord<K, V>>();
	
	//for buffer control
	//for buckets method
	private P minP;
	private P maxP;
	private P initMinP;
	private P initMaxP;
	private P[] steps;
	private ArrayList[] buckets = new ArrayList[10];
	
	//for threshold;
	private P thresholdP;
	
	private int iter;
	public int total_map = 0;
	public int total_reduce = 0;
	
	//for emitsize determination
	public long sort_time = 0;
	public long iter_previous_time = 0;
	public double iter_time = 0;
	public int emitSize;
	public int actualEmit = 0;
	public static int WAIT_ITER = 2;
	private float wearfactor;
	
	public OutputPKVBuffer(BufferUmbilicalProtocol umbilical, Task task, JobConf job, 
			Reporter reporter, Progress progress, 
		       Class<P> priorityClass, Class<K> keyClass, Class<V> valClass, 
		       		IterativeReducer iterReducer) throws IOException{	
		
		LOG.info("OutputPKVBuffer is reset for task " + task.getTaskID());

		this.taskAttemptID = task.getTaskID();
		this.priorityClass = priorityClass;
		this.keyClass = keyClass;
		this.valClass = valClass;
		
		this.job = job;
		this.localFs = FileSystem.getLocal(job);
		this.outputHandle = new FileHandle(taskAttemptID.getJobID());
		this.outputHandle.setConf(job);
		//this.bSort = this.job.getBoolean("mapred.iterative.sort", true);
		this.topk = this.job.getInt("mapred.iterative.topk", 0);
		this.emitSize = job.getInt("mapred.iterative.reduce.emitsize", 100);
		this.wearfactor = job.getFloat("mapred.iterative.output.wearfactor", (float)0.1);
		int type = job.getInt("mapred.iterative.priority.type", 2);
		if(type == 0){
			this.priType = PRI_TYPE.SORT;
		}else if(type == 1){
			this.priType = PRI_TYPE.BUCKET;
		}else if(type == 2){
			this.priType = PRI_TYPE.THRESHOLD;
		}
		this.iterReducer = iterReducer;
				
		LOG.info(priorityClass);
		minP = (P)WritableFactories.newInstance(priorityClass, job);
		maxP = (P)WritableFactories.newInstance(priorityClass, job);
		iterReducer.bufSplit(minP, maxP, true);
		initMinP = minP;
		initMaxP = maxP;
		thresholdP = (P)this.iterReducer.bound(minP, maxP);
		//10 buckets storing kv pairs, desend order
		for(int i=0; i<buckets.length; i++){
			buckets[i] = new ArrayList<K>();
		}
		iter = 0;
		
		Date start = new Date();
	}

	public Header header() {
		return new OutputFile.PKVBufferHeader(this.taskAttemptID, this.iteration);
	}
	
	public TaskAttemptID getTaskAttemptID() {
		return this.taskAttemptID;
	}

	private synchronized ArrayList<KVRecord<K, V>> getThresholdRecords() {
		synchronized(this.HoldMap){	
			ArrayList<KVRecord<K, V>> toprecords = new ArrayList<KVRecord<K, V>>();
			
			Iterator<K> hold_itr = new ArrayList<K>(this.HoldMap.keySet()).iterator();
			int count = 0;
			PriorityRecord<P, V> rec = null;
			while(hold_itr.hasNext()){
				K key = hold_itr.next();
				rec = this.HoldMap.get(key);
				
				//LOG.info(rec + "\t" + thresholdP);
				if(((WritableComparable)rec.getPriority()).compareTo(thresholdP) >= 0){
					toprecords.add(new KVRecord<K, V>(key, rec.getValue()));
					this.HoldMap.remove(key);
					count++;
				}
			}
				
			return toprecords;
		}
	}
	
	@SuppressWarnings("unchecked")
	private synchronized ArrayList<KVRecord<K, V>> getBucketRecords() {
		synchronized(this.HoldMap){		
			steps = (P[])this.iterReducer.bufSplit(minP, maxP, false);
						
			for(int i=0; i<10; i++){
				buckets[i].clear();
			}
			
			Iterator<K> hold_itr = new ArrayList<K>(this.HoldMap.keySet()).iterator();
			while(hold_itr.hasNext()){
				K key = hold_itr.next();
				P pri = this.HoldMap.get(key).getPriority();
				
				if(((WritableComparable)pri).compareTo(steps[0]) >= 0){
					buckets[0].add(key);
				}else if(((WritableComparable)pri).compareTo(steps[1]) >= 0){
					buckets[1].add(key);
				}else if(((WritableComparable)pri).compareTo(steps[2]) >= 0){
					buckets[2].add(key);
				}else if(((WritableComparable)pri).compareTo(steps[3]) >= 0){
					buckets[3].add(key);
				}else if(((WritableComparable)pri).compareTo(steps[4]) >= 0){
					buckets[4].add(key);
				}else if(((WritableComparable)pri).compareTo(steps[5]) >= 0){
					buckets[5].add(key);
				}else if(((WritableComparable)pri).compareTo(steps[6]) >= 0){
					buckets[6].add(key);
				}else if(((WritableComparable)pri).compareTo(steps[7]) >= 0){
					buckets[7].add(key);
				}else if(((WritableComparable)pri).compareTo(steps[8]) >= 0){
					buckets[8].add(key);
				}else{
					buckets[9].add(key);
				}
			}
		
			LOG.info("iter " + (iter++) + " buckets status, min " + minP + " max " + maxP + 
					" bucket[0]=" + buckets[0].size() +
					" bucket[1]=" + buckets[1].size() +
					" bucket[2]=" + buckets[2].size() +
					" bucket[3]=" + buckets[3].size() +
					" bucket[4]=" + buckets[4].size() +
					" bucket[5]=" + buckets[5].size() +
					" bucket[6]=" + buckets[6].size() +
					" bucket[7]=" + buckets[7].size() +
					" bucket[8]=" + buckets[8].size() +
					" bucket[9]=" + buckets[9].size());

			int[] bucketSizes = new int[10];
			for(int i=0; i<10; i++){
				bucketSizes[i] = buckets[i].size();
			}
			int bucketIndex = this.iterReducer.bucketTransfer(bucketSizes);
			
			ArrayList<KVRecord<K, V>> records = new ArrayList<KVRecord<K, V>>();
			int count = 0;
				
			List<K> keys = new ArrayList<K>();
			for(int index=0; index<bucketIndex; index++){
				keys.addAll(buckets[index]);
			}
						
			Iterator<K> sort_itr =keys.iterator();
			PriorityRecord<P, V> record = null;
			while(sort_itr.hasNext()){
				Object k = sort_itr.next();
				record = HoldMap.get(k);
				
				records.add(new KVRecord(k, record.getValue()));				
				
				this.HoldMap.remove(k);	
				count++;
			}
			
			LOG.info("expend " + count + " k-v pairs");
			
			maxP = (bucketIndex > 0) ? steps[bucketIndex-1] : steps[0];

			return records;
			
		}
	}
	
	private synchronized ArrayList<KVRecord<K, V>> getSortRecords() {
		synchronized(this.HoldMap){	
			List<K> keys = new ArrayList<K>(this.HoldMap.keySet());
			
			Date start_sort_date = new Date();
			long start_sort = start_sort_date.getTime();
			double iter_time_per_node = (double)(start_sort - iter_previous_time - 500) / actualEmit;
			iter_time = (iter_time_per_node <= 0) ? 0.2 : iter_time_per_node;
			
			//LOG.info("heap size : " + this.recordsMap.size());
			final Map<K, PriorityRecord<P, V>> langForSort = HoldMap;
			Collections.sort(keys, 
					new Comparator(){
						public int compare(Object left, Object right){
							K leftKey = (K)left;
							K rightKey = (K)right;
		 
							PriorityRecord<P, V> leftValue = (PriorityRecord<P, V>)langForSort.get(leftKey);
							PriorityRecord<P, V> rightValue = (PriorityRecord<P, V>)langForSort.get(rightKey);
							return -leftValue.compareTo(rightValue);
						}
					});
			Date end_sort_date = new Date();
			long end_sort = end_sort_date.getTime();
			
			sort_time = end_sort - start_sort + 500;
			
			if (iter > WAIT_ITER){
				emitSize = (int) ((double)(sort_time * wearfactor) / iter_time);
			}
			if (emitSize == 0){
				emitSize = 1;
			}
			LOG.info("iteration " + iter + " outputqueuesize " + emitSize + " wearfactor " + wearfactor 
					+ " overhead: " + sort_time + 
					" on " + keys.size() + " nodes, iterationtime " + (start_sort - iter_previous_time) 
					+ " and " + iter_time + " per key");

			iter_previous_time = end_sort;
			
			ArrayList<KVRecord<K, V>> records = new ArrayList<KVRecord<K, V>>();
			actualEmit = 0;
						
			Iterator<K> sort_itr =keys.iterator();
			PriorityRecord<P, V> record = null;
			while(sort_itr.hasNext() && actualEmit<this.emitSize){
				Object k = sort_itr.next();
				record = HoldMap.get(k);				
				records.add(new KVRecord(k, record.getValue()));								
				this.HoldMap.remove(k);	
				actualEmit++;
			}
			
			LOG.info("iteration " + iter + " expend " + actualEmit + " k-v pairs");
			iter++;
			return records;
		}
	}
	
	public synchronized void collect(P priority, K key, V value) throws IOException {
		//LOG.info("some key value pair in: " + priority + " : " + key + " : " + value);
		
		if (priority.getClass() != priorityClass) {
			throw new IOException("Type mismatch in priority from map: expected "
					+ priorityClass.getName() + ", recieved "
					+ priority.getClass().getName());
		}
		if (key.getClass() != keyClass) {
			throw new IOException("Type mismatch in key from map: expected "
					+ keyClass.getName() + ", recieved "
					+ key.getClass().getName());
		}
		if (value.getClass() != valClass) {
			throw new IOException("Type mismatch in value from map: expected "
					+ valClass.getName() + ", recieved "
					+ value.getClass().getName());
		}
				
		start = true;
		synchronized(this.HoldMap){		
			PriorityRecord<P, V> newpkvRecord = new PriorityRecord<P, V>();
			P pri = priority;

			//do combination
			boolean merge = false;
			if(this.HoldMap.containsKey(key)){
				merge = true;
				PriorityRecord<P, V> pkvRecord = this.HoldMap.get(key);
				this.iterReducer.combine(priority, value, 
						pkvRecord.getPriority(), pkvRecord.getValue(),
						newpkvRecord);	
				pri = newpkvRecord.getPriority();
				/*
				LOG.info("priority1 " + priority + " value1 " + value + 
						"\tpriority2 " + pkvRecord.getPriority() + " value2 " + pkvRecord.getValue() + 
						"\tfinal priority " + newpkvRecord.getPriority() + " final value " + newpkvRecord.getValue());
				*/
			}else{
				newpkvRecord.setPriority(priority);
				newpkvRecord.setValue(value);
			}
			
			if(this.priType == PRI_TYPE.THRESHOLD){
				//LOG.info("compare " + pri + "\t" +  thresholdP);
				if(((WritableComparable)pri).compareTo(thresholdP) > 0){
					this.records.add(new KVRecord<K, V>(key, newpkvRecord.getValue()));
					if(merge){
						this.HoldMap.remove(key);
					}
				}else{
					this.HoldMap.put(key, newpkvRecord);
				}
				
				if(((WritableComparable)pri).compareTo(maxP) > 0){
					maxP = pri;
				}else if(((WritableComparable)pri).compareTo(minP) < 0){
					minP = pri;
				}	
			}else if(this.priType == PRI_TYPE.SORT){
				this.HoldMap.put(key, newpkvRecord);
			}else if(this.priType == PRI_TYPE.BUCKET){
	
			}
		}
		total_reduce++;
	}
	
	@Override
	public synchronized void collect(K key, V value) throws IOException {
		//LOG.info("some key value pair in: " + key + " : " + value);
		
		//PriorityRecord<K, V> rec = new PriorityRecord<K, V>(Integer.MIN_VALUE, key, value);
		//this.recordsMap.put(key, rec);
	}
	
	/**
	 * this need to be improved. we should emit the top records by doing spill only when
	 * we got some new higher priority KVs, if no updated new higher priority KVs, then we
	 * don't need to emit. If we have a lot of new higher KVs, we can emit some part of higher
	 * ones. while if we have a small number of new higher KVs, we emit them all.
	 * @param stop
	 * @return
	 * @throws IOException
	 */
	public synchronized OutputFile spillTops() throws IOException {

		Path filename = null;
		Path indexFilename = null;
		try{
			filename = outputHandle.getSpillFileForWrite(this.taskAttemptID, this.iteration, -1);
			indexFilename = outputHandle.getSpillIndexFileForWrite(
					this.taskAttemptID, this.iteration, 24);
		}catch(IOException e){
			e.printStackTrace();
		}
		
		if (localFs.exists(filename)) {
			throw new IOException("PartitionBuffer::sortAndSpill -- spill file exists! " + filename);
		}

		FSDataOutputStream out = null;
		FSDataOutputStream indexOut = null;
		IFile.Writer<K, V> writer = null;
		
		try{		
			out = localFs.create(filename, false);
			indexOut = localFs.create(indexFilename, false);
	
			if (out == null ) throw new IOException("Unable to create spill file " + filename);
			
			writer = new IFile.Writer<K, V>(job, out, keyClass, valClass, null, null);
			
			ArrayList<KVRecord<K, V>> entries = null;
			
			if(this.priType == PRI_TYPE.THRESHOLD){
				if(iteration % 10 == 0){
					LOG.info("hold buffer reshape");
					entries = getThresholdRecords();			
				}
				
				//determine threshold for next round
				thresholdP = (P)this.iterReducer.bound(minP, maxP);
				LOG.info("mapP is " + maxP + " threshold is " + thresholdP);
				maxP = thresholdP;

			}else if(this.priType == PRI_TYPE.BUCKET){
				entries = getBucketRecords();
			}else if(this.priType == PRI_TYPE.SORT){
				Date current = new Date();
				iter_time = (current.getTime() - iter_previous_time) / emitSize;
				entries = getSortRecords();
			}
	
			if(entries != null) records.addAll(entries);
			int count = 0;
					
			if(records.size() == 0){
				LOG.info("no records to send");
				K k = (K)WritableFactories.newInstance(keyClass, job);
				V v = (V)WritableFactories.newInstance(valClass, job);
				this.iterReducer.defaultKV(k, v);
				writer.append(k, v);
				/*
				if (null != writer) {
					writer.close();
					writer = null;
				}
				
				if (out != null){
					out.close();
					out = null;
				}
				if (indexOut != null) {
					indexOut.close();
					indexOut = null;
				}
				 
				localFs.delete(filename, true);
				localFs.delete(indexFilename, true);
				return null;
				*/
			}else{
				//LOG.info("emit size: " + emitRecSize);
				//int writedRecords = 0;
				for(KVRecord<K, V> entry : records){		
					writer.append(entry.k, entry.v);
					//writedRecords++;
					//LOG.info("send records: " + entry.k + " : " + entry.v);
					entry = null;
					count++;
				}
				records.clear();
				total_map += count;
			}		
			writer.close();
			
			LOG.info("iteration " + this.iteration + " expand " + count + " k-v pairs, " +
					"total maps " + total_map + " total collected " + total_reduce);
			writeIndexRecord(indexOut, out, 0, writer);
			writer = null;
		} catch(IOException e){
			e.printStackTrace();
		}finally {
			if (null != writer) {
				writer.close();
				writer = null;
			}
			
			if (out != null){
				out.close();
				out = null;
			}
			if (indexOut != null) {
				indexOut.close();
				indexOut = null;
			}
			
			//LOG.info("generated a spill file " + filename);
			//LOG.info("generated a spill index file " + indexFilename);
		}
		
		this.iteration++;
		
		int partitions = job.getBoolean("mapred.iterative.mapsync", false) ? job.getInt("mapred.iterative.ttnum", 1) : 1;
		return new OutputFile(this.taskAttemptID, this.iteration, filename, indexFilename, partitions);
		
	}
	
	private void writeIndexRecord(FSDataOutputStream indexOut,
			FSDataOutputStream out, long start,
			IFile.Writer<K, V> writer)
	throws IOException {
		//when we write the offset/decompressed-length/compressed-length to
		//the final index file, we write longs for both compressed and
		//decompressed lengths. This helps us to reliably seek directly to
		//the offset/length for a partition when we start serving the
		//byte-ranges to the reduces. We probably waste some space in the
		//file by doing this as opposed to writing VLong but it helps us later on.
		// index record: <offset, raw-length, compressed-length>
		//StringBuffer sb = new StringBuffer();
		indexOut.writeLong(start);
		indexOut.writeLong(writer.getRawLength());
		long segmentLength = out.getPos() - start;
		indexOut.writeLong(segmentLength);
		
		LOG.info("index record <offset, raw-length, compressed-length>: " + 
				start + ", " + writer.getRawLength() + ", " + segmentLength);
	}
	
	@Override
	public String toString() {
		return new String(this.taskAttemptID + " priority buffer(" + this.iteration + ")");
	}
	
	public int size() {
		synchronized(this.HoldMap){
			return this.HoldMap.size();
		}		
	}
	/*
	public void bufferStat() {
		Iterator<K> hold_itr = new ArrayList<K>(this.HoldMap.keySet()).iterator();
		minP = initMinP;
		maxP = initMaxP;
		while(hold_itr.hasNext()){
			K key = hold_itr.next();
			P pri = this.HoldMap.get(key).getPriority();
			
			if(((WritableComparable)pri).compareTo(minP) < 0){
				minP = pri;
			}
			if(((WritableComparable)pri).compareTo(maxP) > 0){
				maxP = pri;
			}
			
		}
	}
	*/
}
