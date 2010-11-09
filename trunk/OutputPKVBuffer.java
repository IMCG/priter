package org.apache.hadoop.mapred.buffer.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
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


public class OutputPKVBuffer<P extends Object, K extends Object, V extends Object> 
		implements OutputCollector<K, V> {
	
	//**************************************
	private static final Log LOG = LogFactory.getLog(OutputPKVBuffer.class.getName());
	
	private TaskAttemptID taskAttemptID;
	private int iteration = 0;
	private Class<P> priorityClass;
	private Class<K> keyClass;	
	private Class<V> valClass;
	
	private final FileSystem localFs;
	private FileHandle outputHandle = null;
	private JobConf job = null;
	private int emitSize;
	private boolean bSort;
	Serializer<P> prioritySerializer;
    Serializer<K> keySerializer;
    Serializer<V> valueSerializer;
    DataOutputBuffer buffer = new DataOutputBuffer();
    
    private IterativeReducer iterReducer = null;
    
    //save sent status
    //private Map<K, Boolean> sentMap = new HashMap<K, Boolean>();

	//main part of this class, storing priority-key-value records
	private Map<K, PriorityRecord<P, V>> recordsMap = null;
	//private PriorityQueue<PriorityRecord<K, V>> topRecords = null;
	
	public OutputPKVBuffer(BufferUmbilicalProtocol umbilical, Task task, JobConf job, 
			Reporter reporter, Progress progress, 
		       Class<P> priorityClass, Class<K> keyClass, Class<V> valClass, 
		       		IterativeReducer iterReducer) throws IOException{	
		
		LOG.info("OutputPKVBuffer is reset for task " + task.getTaskID());

		this.taskAttemptID = task.getTaskID();
		this.priorityClass = priorityClass;
		this.keyClass = keyClass;
		this.valClass = valClass;
		this.recordsMap = new HashMap<K, PriorityRecord<P, V>>();	
		//this.topRecords = new PriorityQueue<PriorityRecord<K, V>>();
		
		this.job = job;
		this.localFs = FileSystem.getLocal(job);
		this.outputHandle = new FileHandle(taskAttemptID.getJobID());
		this.outputHandle.setConf(job);
		this.bSort = this.job.getBoolean("mapred.iterative.sort", true);
		this.emitSize = job.getInt("mapred.iterative.reduce.emitsize", 100);
		this.iterReducer = iterReducer;
	}

	public Header header() {
		return new OutputFile.PKVBufferHeader(this.taskAttemptID, this.iteration);
	}
	
	public TaskAttemptID getTaskAttemptID() {
		return this.taskAttemptID;
	}

	@SuppressWarnings("unchecked")
	private synchronized ArrayList<KVRecord<K, V>> getRecords() {
		
		synchronized(this.recordsMap){
			List<K> keys = new ArrayList<K>(this.recordsMap.keySet());
			int size = 0;
			
			if(bSort){
				//sort on emitsize dataset
			
				//LOG.info("heap size : " + this.recordsMap.size());
				final Map<K, PriorityRecord<P, V>> langForComp = this.recordsMap;
				Collections.sort(keys, 
						new Comparator(){
							public int compare(Object left, Object right){
								K leftKey = (K)left;
								K rightKey = (K)right;
			 
								PriorityRecord<P, V> leftValue = (PriorityRecord<P, V>)langForComp.get(leftKey);
								PriorityRecord<P, V> rightValue = (PriorityRecord<P, V>)langForComp.get(rightKey);
								return -leftValue.compareTo(rightValue);
							}
						});
				size = this.emitSize;
			}else{
				size = Integer.MAX_VALUE;
			}
			
			ArrayList<KVRecord<K, V>> records = new ArrayList<KVRecord<K, V>>();
			int count = 0;
				
			Iterator<K> itr =keys.iterator();

			while(itr.hasNext() && count<size){
				Object k = itr.next();
				PriorityRecord<P, V> record = this.recordsMap.get(k);
				
				//check if it is large enough to transfer
				if(this.iterReducer.transfer(k, record.getValue(), record.getPriority())){				
					records.add(new KVRecord(k, record.getValue()));				
				}else{
					break;
				}
				
				this.recordsMap.remove(k);	
				count++;
			}
			LOG.info("expend frontier number as " + count);
			/*
			//only after collecting enough records, then send
			if(count < size){
				return null;
			}
			*/
			return records;
		}
	}
	
	public int size() {
		return this.recordsMap.size();
	}
	
	public synchronized void collect(P priority, K key, V value) throws IOException {
		//LOG.info("some key value pair in: " + priority + " : " + key + " : " + value);
		synchronized(this.recordsMap){
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
				
			PriorityRecord<P, V> rec;
			//do combination
			if(this.recordsMap.containsKey(key)){
				PriorityRecord<P, V> pkvRecord = this.recordsMap.get(key);
				V valAfterCombine = (V) this.iterReducer.combine(priority, value, pkvRecord.getPriority(), pkvRecord.getValue());
				P pri = (P)this.iterReducer.setPriority(key, valAfterCombine);
				rec = new PriorityRecord<P, V>(pri, valAfterCombine);
				this.recordsMap.remove(key);
			}else{
				rec = new PriorityRecord<P, V>(priority, value);
			}
				
			//we do not need to create a new instance of K and put it in pkvBuffer,
			//because the user already new it	
			
			this.recordsMap.put(key, rec);
		}
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
		synchronized(this.recordsMap){
			if(this.recordsMap.isEmpty()){
				return null;
			}
			
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
				
				//for spill, just want not sent yet records
				ArrayList<KVRecord<K, V>> entries = getRecords();
				
				if(entries.size() == 0){
					//Even though no records written, there are still 6 bytes will be written 
					//so if no records written, delete the file.
					if (null != writer) {
						writer.close();
						writer = null;
					}
					
					if (out != null) {
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
				}
				
				//LOG.info("emit size: " + emitRecSize);
				//int writedRecords = 0;
				for(KVRecord<K, V> entry : entries){		
					writer.append(entry.k, entry.v);
					//writedRecords++;
					//LOG.info("send records: " + entry.k + " : " + entry.v);
					entry = null;
				}

				writer.close();
				
				LOG.info("pkvBuffer file length " + out.getPos());
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
}
