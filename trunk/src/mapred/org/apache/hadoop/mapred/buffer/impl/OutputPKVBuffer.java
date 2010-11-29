package org.apache.hadoop.mapred.buffer.impl;

import java.io.BufferedWriter;
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
	
	private TaskAttemptID taskAttemptID;
	private int iteration = 0;
	//private Class<P> priorityClass;
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
    private V defaultiState;
    
    private IterativeReducer iterReducer = null;

	//main part of this class, storing priority-key-value records
	private Map<K, PriorityRecord<P, V>> stateTable = new HashMap<K, PriorityRecord<P, V>>();
	private ArrayList<KVRecord<K, V>> priorityQueue = new ArrayList<KVRecord<K, V>>();
	
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
	private int topk;
	
	public OutputPKVBuffer(BufferUmbilicalProtocol umbilical, Task task, JobConf job, 
			Reporter reporter, Progress progress, 
		       Class<P> priorityClass, Class<K> keyClass, Class<V> valClass, 
		       		IterativeReducer iterReducer) throws IOException{	
		
		LOG.info("OutputPKVBuffer is reset for task " + task.getTaskID());

		this.taskAttemptID = task.getTaskID();
		//this.priorityClass = priorityClass;
		this.keyClass = keyClass;
		this.valClass = valClass;
		
		this.job = job;
		this.localFs = FileSystem.getLocal(job);
		this.outputHandle = new FileHandle(taskAttemptID.getJobID());
		this.outputHandle.setConf(job);
		this.emitSize = job.getInt("mapred.iterative.reduce.emitsize", 100);
		this.wearfactor = job.getFloat("mapred.iterative.output.wearfactor", (float)10);
		this.topk = job.getInt("mapred.iterative.topk", 1000);
		this.iterReducer = iterReducer;		
		this.defaultiState = (V)iterReducer.setDefaultiState();
		
		Date start = new Date();
	}

	public Header header() {
		return new OutputFile.PKVBufferHeader(this.taskAttemptID, this.iteration);
	}
	
	public TaskAttemptID getTaskAttemptID() {
		return this.taskAttemptID;
	}

	private synchronized ArrayList<KVRecord<K, V>> getSortRecords() {
		synchronized(this.stateTable){	
			List<K> keys = new ArrayList<K>(this.stateTable.keySet());
			
			Date start_sort_date = new Date();
			long start_sort = start_sort_date.getTime();
			double iter_time_per_node = (double)(start_sort - iter_previous_time - 500) / actualEmit;
			iter_time = (iter_time_per_node <= 0) ? 0.2 : iter_time_per_node;
			
			//LOG.info("heap size : " + this.recordsMap.size());
			final Map<K, PriorityRecord<P, V>> langForSort = stateTable;
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
			
			if (iteration > WAIT_ITER){
				emitSize = (int) ((double)(sort_time * wearfactor) / iter_time);
			}
			if (emitSize == 0){
				emitSize = 1;
			}
			LOG.info("iteration " + iteration + " outputqueuesize " + emitSize + " wearfactor " + wearfactor 
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
				record = stateTable.get(k);				
				records.add(new KVRecord(k, record.getiState()));
				this.stateTable.get(k).setiState(defaultiState);
				actualEmit++;
			}
			
			LOG.info("iteration " + iteration + " expend " + actualEmit + " k-v pairs");
			return records;
		}
	}
	
	public synchronized void collect(K key, V value) throws IOException {
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
		synchronized(this.stateTable){		
			P pri;

			if(this.stateTable.containsKey(key)){
				PriorityRecord<P, V> pkvRecord = this.stateTable.get(key);
				V newiState = (V)iterReducer.updateState(pkvRecord.getiState(), value);
				V newcState = (V)iterReducer.updateState(pkvRecord.getcState(), value);
				pri = (P)iterReducer.setPriority(newiState);
				pkvRecord.setPriority(pri);
				pkvRecord.setiState(newiState);
				pkvRecord.setcState(newcState);
			}else{
				pri = (P)iterReducer.setPriority(value);
				PriorityRecord<P, V> newpkvRecord = new PriorityRecord<P, V>(pri, value, value);
				this.stateTable.put(key, newpkvRecord);
			}
		}
		total_reduce++;
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
			
			Date current = new Date();
			iter_time = (current.getTime() - iter_previous_time) / emitSize;
			entries = getSortRecords();
			
	
			if(entries != null) this.priorityQueue.addAll(entries);
			int count = 0;
					
			if(priorityQueue.size() == 0){
				LOG.info("no records to send");
				K k = (K)WritableFactories.newInstance(keyClass, job);
				V v = (V)WritableFactories.newInstance(valClass, job);
				this.iterReducer.defaultKV(k, v);
				writer.append(k, v);
			}else{
				for(KVRecord<K, V> entry : priorityQueue){		
					writer.append(entry.k, entry.v);
					//LOG.info("send records: " + entry.k + " : " + entry.v);
					entry = null;
					count++;
				}
				priorityQueue.clear();
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
		
		//int partitions = job.getBoolean("mapred.iterative.mapsync", false) ? job.getInt("mapred.iterative.ttnum", 1) : 1;
		int partitions = 1;
		return new OutputFile(this.taskAttemptID, this.iteration, filename, indexFilename, partitions);
		
	}
	
	private void writeIndexRecord(FSDataOutputStream indexOut,
			FSDataOutputStream out, long start,
			IFile.Writer<K, V> writer)
	throws IOException {
		indexOut.writeLong(start);
		indexOut.writeLong(writer.getRawLength());
		long segmentLength = out.getPos() - start;
		indexOut.writeLong(segmentLength);
		
		LOG.info("index record <offset, raw-length, compressed-length>: " + 
				start + ", " + writer.getRawLength() + ", " + segmentLength);
	}
	
	public void snapshot(BufferedWriter writer, int snapshot_index) {

		synchronized(this.stateTable){
			final Map<K, PriorityRecord<P, V>> langForComp = this.stateTable;
			List<K> keys = new ArrayList<K>(this.stateTable.keySet());
			Collections.sort(keys, 
					new Comparator(){
						public int compare(Object left, Object right){
							P leftpriority = (P)iterReducer.setPriority(langForComp.get((K)left).getcState());
							P rightpriority = (P)iterReducer.setPriority(langForComp.get((K)right).getcState());

							return ((WritableComparable)leftpriority).compareTo(rightpriority);
						}
					});

			Iterator<K> itr =keys.iterator();
			PriorityRecord<P, V> record = null;
			int count = 0;
			int len = 0;
			while(itr.hasNext() && count < topk){
				K k = itr.next();
				record = stateTable.get(k);	
				
				if(record.getcState().equals(defaultcState)) break;
				
				writer.write(record.getPriority() + "\t" + k + "\t" + record.getcState() + "\n");
				
				count++;
			}

			System.out.println("snapshot index " + snapshot_index + " iterations " + iterate +
					" reduce is " + reduce + " compare is " + compare);
		}
	}
	
	@Override
	public String toString() {
		return new String(this.taskAttemptID + " priority buffer(" + this.iteration + ")");
	}
	
	public int size() {
		synchronized(this.stateTable){
			return this.stateTable.size();
		}		
	}
}
