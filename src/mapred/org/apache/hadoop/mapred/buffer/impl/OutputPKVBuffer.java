package org.apache.hadoop.mapred.buffer.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
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


public class OutputPKVBuffer<P extends WritableComparable, V extends WritableComparable> 
		implements OutputCollector<IntWritable, V>{
	
	public static final int SAMPLESIZE = 1000;
	public static final int PARTTOPKRANG = 4;
	//**************************************
	private static final Log LOG = LogFactory.getLog(OutputPKVBuffer.class.getName());

	private JobConf job = null;
	private TaskAttemptID taskAttemptID;
	private final FileSystem localFs;
	private FileSystem hdfs;
	private FileHandle outputHandle = null;
	public String exeQueueFile;
	public String stateTableFile;
	private int dumpFrequency;
	private boolean ftSupport;
	private String topkDir = null;

    private IterativeReducer iterReducer = null;
	public Map<IntWritable, PriorityRecord<P, V>> stateTable = new HashMap<IntWritable, PriorityRecord<P, V>>();
	private ArrayList<KVRecord<IntWritable, V>> priorityQueue = new ArrayList<KVRecord<IntWritable, V>>();
	private IntWritable defaultKey;
    private V defaultiState;
    
	private Iterator<IntWritable> iteratedKeys;
	private IntWritable iterKey;
	private V iteriState;
	private V itercState;
	
	private Class<P> priClass;
	private Class<V> valClass;
    //Serializer<K> keySerializer;
    //Serializer<V> valueSerializer;
    //DataOutputBuffer buffer = new DataOutputBuffer();
      
	private int topk;
	private int queuelen = 0;
	
	private int iteration = 0;
	public int total_map = 0;
	public int total_reduce = 0;
	public boolean start = false;
	
	//for expanding size determination
	public long timeComp;
	public long timeSync;
	
	public int actualEmit = 0;
	public static int WAIT_ITER = 0;
	
	//for termination check


	public OutputPKVBuffer(BufferUmbilicalProtocol umbilical, Task task, JobConf job, 
			Reporter reporter, Progress progress, Class<P> priClass, Class<V> valClass, 
		       		IterativeReducer iterReducer) throws IOException{	
		
		LOG.info("OutputPKVBuffer is reset for task " + task.getTaskID());

		this.job = job;
		this.taskAttemptID = task.getTaskID();
		this.localFs = FileSystem.getLocal(job);
		this.hdfs = FileSystem.get(job);
		this.outputHandle = new FileHandle(taskAttemptID.getJobID());
		this.outputHandle.setConf(job);
		this.iterReducer = iterReducer;		
		this.defaultKey = iterReducer.setDefaultKey();
		this.defaultiState = (V)iterReducer.setDefaultiState();
		
		this.ftSupport = job.getBoolean("mapred.iterative.ftsupport", true);
		this.exeQueueFile = job.get("mapred.output.dir") + "/_ExeQueueTemp/" + 
							taskAttemptID.getTaskID().getId() + "-exequeue";
		this.stateTableFile = job.get("mapred.output.dir") + "/_StateTableTempDir/"
								+ taskAttemptID.getTaskID().getId() + "-statetable";
		this.dumpFrequency = job.getInt("mapred.dump.frequency", 20);

		this.topkDir = job.get("mapred.output.dir") + "/" + this.taskAttemptID.getTaskID().getId();
		this.valClass = valClass;

		int partitions = job.getInt("mapred.iterative.partitions", 0);
		int totalkeys = job.getInt("mapred.iterative.totalkeys", -1) / partitions;
		this.queuelen = (int) (totalkeys * job.getFloat("mapred.iterative.alpha", 1));
		
		this.topk = job.getInt("mapred.iterative.topk", 1000);
		this.topk = this.topk * PARTTOPKRANG / partitions;
		
		this.iterReducer.initStateTable(this);
	}

	public Header header() {
		return new OutputFile.PKVBufferHeader(this.taskAttemptID, this.iteration);
	}
	
	public TaskAttemptID getTaskAttemptID() {
		return this.taskAttemptID;
	}

	public void init(IntWritable key, V iState, V cState){
		P pri = (P) iterReducer.setPriority(key, iState);
		PriorityRecord<P, V> newpkvRecord = new PriorityRecord<P, V>(pri, iState, cState);
		this.stateTable.put(key, newpkvRecord);
	}
	
	private synchronized ArrayList<KVRecord<IntWritable, V>> getTopRecords() {
		synchronized(this.stateTable){	
			/*
			if(actualEmit == 0){
				actualEmit = job.getInt("mapred.iterative.startkeys", 0) / this.ttnum;
				actualEmit = (actualEmit == 0) ? 1 : actualEmit;
			}
			*/
			actualEmit = 0;
			ArrayList<KVRecord<IntWritable, V>> records = new ArrayList<KVRecord<IntWritable, V>>();
			
			if((stateTable.size() <= queuelen) || (stateTable.size() <= SAMPLESIZE)){
				for(IntWritable k : stateTable.keySet()){		
					V v = stateTable.get(k).getiState();
					records.add(new KVRecord<IntWritable, V>(k, v));
					V iState = (V)iterReducer.setDefaultiState();
					this.stateTable.get(k).setiState(iState);
					P pri = (P) iterReducer.setPriority(k, iState);
					this.stateTable.get(k).setPriority(pri);
					actualEmit++;	
				}
				LOG.info("iteration " + iteration + " expend " + actualEmit + " k-v pairs");
			}else{
				Random rand = new Random();
				List<IntWritable> randomkeys = new ArrayList<IntWritable>(SAMPLESIZE);
				List<IntWritable> keys = new ArrayList<IntWritable>(stateTable.keySet());
				
				for(int j=0; j<SAMPLESIZE; j++){
					IntWritable randnode = keys.get(rand.nextInt(stateTable.size()));
					randomkeys.add(randnode);
				}
				
				final Map<IntWritable, PriorityRecord<P, V>> langForSort = stateTable;
				Collections.sort(randomkeys, 
						new Comparator(){
							public int compare(Object left, Object right){
								PriorityRecord<P, V> leftrecord = langForSort.get(left);
								PriorityRecord<P, V> rightrecord = langForSort.get(right);
								return -leftrecord.compareTo(rightrecord);
							}
						});
				
				int cutindex = queuelen * SAMPLESIZE / this.stateTable.size();
				P threshold = stateTable.get(randomkeys.get(cutindex)).getPriority();
				LOG.info("queuelen " + queuelen + " table size " + stateTable.size() + 
						" cut index " + cutindex);
				
				for(IntWritable k : stateTable.keySet()){		
					V v = stateTable.get(k).getiState();
					P pri = stateTable.get(k).getPriority();
					if(pri.compareTo(threshold) > 0){
						records.add(new KVRecord<IntWritable, V>(k, v));
						V iState = (V)iterReducer.setDefaultiState();
						this.stateTable.get(k).setiState(iState);
						P p = (P) iterReducer.setPriority(k, iState);
						this.stateTable.get(k).setPriority(p);
						actualEmit++;
					}	
					//LOG.info(k + "\t" + pri + "\t" + v);
				}
				LOG.info("iteration " + iteration + " expend " + actualEmit + " k-v pairs" + " threshold is " + threshold);
			}
			return records;
		}
	}
	/*
	public synchronized void collect(IntWritable key, V value) throws IOException {
		if (key.getClass() != IntWritable.class) {
			throw new IOException("Type mismatch in key from map: expected IntWritable"  
					+ ", recieved "
					+ key.getClass().getName());
		}
		if (value.getClass() != valClass) {
			throw new IOException("Type mismatch in value from map: expected "
					+ valClass.getName() + ", recieved "
					+ value.getClass().getName());
		}
				
		start = true;
		PriorityRecord<P, V> pkvRecord;
		synchronized(this.stateTable){		
			if(this.stateTable.containsKey(key)){
				pkvRecord = this.stateTable.get(key);
				iterReducer.updateState(pkvRecord.getiState(), pkvRecord.getcState(), value);	
				//LOG.info("updated existed key: " + key + " istate: " + pkvRecord.getiState() + " cstate: " + pkvRecord.getcState() + " value: " + value);
			}else{
				//LOG.error("no such key " + key);
				V iState = (V)iterReducer.setDefaultiState();
				V cState = (V)iterReducer.setDefaultcState(key);
				P pri = (P) iterReducer.setPriority(key, iState);
				pkvRecord = new PriorityRecord<P, V>(pri, iState, cState);
				iterReducer.updateState(pkvRecord.getiState(), pkvRecord.getcState(), value);
				this.stateTable.put(key, pkvRecord);
				//LOG.info("updated not existed key: " + key + " istate: " + pkvRecord.getiState() + " cstate: " + pkvRecord.getcState() + " value: " + value);
			}
			
			P pri = (P) iterReducer.setPriority(key, pkvRecord.getiState());
			this.stateTable.get(key).setPriority(pri);
		}
		total_reduce++;
	}
	*/
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
		start = true;
		
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
		IFile.Writer<IntWritable, V> writer = null;
		
		try{		
			out = localFs.create(filename, false);
			indexOut = localFs.create(indexFilename, false);
	
			if (out == null ) throw new IOException("Unable to create spill file " + filename);
			
			synchronized(this.stateTable){
				ArrayList<KVRecord<IntWritable, V>> entries = getTopRecords();
					
				writer = new IFile.Writer<IntWritable, V>(job, out, IntWritable.class, valClass, null, null);

				if(entries != null) this.priorityQueue.addAll(entries);
				int count = 0;
						
				if(priorityQueue.size() == 0){
					LOG.info("no records to send");
					writer.append(this.defaultKey, this.defaultiState);
				}else{
					for(KVRecord<IntWritable, V> entry : priorityQueue){		
						writer.append(entry.k, entry.v);
						//LOG.info("send records: " + entry.k + " : " + entry.v);
						entry = null;
						count++;
					}
					
					total_map += count;
				}		
				writer.close();
				
				LOG.info("iteration " + this.iteration + " expand " + count + " k-v pairs, " +
						"total maps " + total_map + " total collected " + total_reduce);
				writeIndexRecord(indexOut, out, 0, writer);
				writer = null;
				
				this.iteration++;
				//periodically dump statetable and execution queue
				if(ftSupport && (iteration % dumpFrequency == 0)){
					dumpExeQueue();
					dumpStateTable();
				}
				priorityQueue.clear();
			}
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
				
		int partitions = 1;
		return new OutputFile(this.taskAttemptID, this.iteration, filename, indexFilename, partitions);
		
	}
	
	private void writeIndexRecord(FSDataOutputStream indexOut,
			FSDataOutputStream out, long start,
			IFile.Writer<IntWritable, V> writer)
	throws IOException {
		indexOut.writeLong(start);
		indexOut.writeLong(writer.getRawLength());
		long segmentLength = out.getPos() - start;
		indexOut.writeLong(segmentLength);
		
		LOG.info("index record <offset, raw-length, compressed-length>: " + 
				start + ", " + writer.getRawLength() + ", " + segmentLength);
	}
	
	private void dumpExeQueue() throws IOException {
		FSDataOutputStream ostream = hdfs.create(new Path(exeQueueFile), true);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(ostream));
		
		int count = 0;
		for(KVRecord<IntWritable, V> entry : priorityQueue) {
		    writer.write(entry.k + "\t" + entry.v + "\n");
		    count++;
		}
		writer.close();
		ostream.close();
		
		LOG.info("dumped ExecutionQueue with " + count + " records");
	}
	
	private void dumpStateTable() throws IOException {
		FSDataOutputStream ostream = hdfs.create(new Path(stateTableFile), true);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(ostream));
		
		int count = 0;
		Set<Map.Entry<IntWritable, PriorityRecord<P, V>>> entries = this.stateTable.entrySet();
		for(Map.Entry<IntWritable, PriorityRecord<P, V>> entry : entries) {
		    IntWritable key = entry.getKey();
		    PriorityRecord<P, V> record = entry.getValue();
		    writer.write(key + "\t" + record + "\n");
		    count++;
		}
		writer.close();
		ostream.close();
		
		LOG.info("dumped StateTable with " + count + " records");
	}
	
	public int loadStateTable() throws IOException {
		FSDataInputStream istream = hdfs.open(new Path(stateTableFile));
		BufferedReader reader = new BufferedReader(new InputStreamReader(istream));
		
		int count = 0;
		while(reader.ready()){
			String line = reader.readLine();
			String[] field = line.split("\t", 4);
			
			IntWritable key = new IntWritable(Integer.parseInt(field[0]));
			if(valClass == IntWritable.class){
				IntWritable pri = new IntWritable(Integer.parseInt(field[1]));
				IntWritable iState = new IntWritable(Integer.parseInt(field[2]));
				IntWritable cState = new IntWritable(Integer.parseInt(field[3]));
				stateTable.put(key, new PriorityRecord(pri, iState, cState));
			}else if(valClass == DoubleWritable.class){
				DoubleWritable pri = new DoubleWritable(Double.parseDouble((field[1])));
				DoubleWritable iState = new DoubleWritable(Double.parseDouble((field[2])));
				DoubleWritable cState = new DoubleWritable(Double.parseDouble((field[3])));
				stateTable.put(key, new PriorityRecord(pri, iState, cState));
			}else if(valClass == FloatWritable.class){
				FloatWritable pri = new FloatWritable(Float.parseFloat((field[1])));
				FloatWritable iState = new FloatWritable(Float.parseFloat((field[2])));
				FloatWritable cState = new FloatWritable(Float.parseFloat((field[3])));
				stateTable.put(key, new PriorityRecord(pri, iState, cState));
			}else{
				throw new IOException(valClass + " not type matched");
			}
			count++;
		}
		
		reader.close();
		return count;
	}
	
	public void snapshot(int index) throws IOException {
		Path topkFile = new Path(topkDir + "/topKsnapshot-" + index);
		IFile.Writer<IntWritable, V> writer = new IFile.Writer<IntWritable, V>(job, hdfs, topkFile, IntWritable.class, valClass, null, null);
		//FSDataOutputStream ostream = hdfs.create(new Path(topkDir + "/topKsnapshot-" + index), true);
		//BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(ostream));

		synchronized(this.stateTable){		
			if(stateTable.size() <= topk){
				for(IntWritable k : stateTable.keySet()){		
					writer.append(k, stateTable.get(k).getcState());	
					//writer.write(k + "\t" + stateTable.get(k).getcState());
				}
			}else{
				Random rand = new Random();
				List<IntWritable> randomkeys = new ArrayList<IntWritable>(SAMPLESIZE);
				List<IntWritable> keys = new ArrayList<IntWritable>(stateTable.keySet());
				
				for(int j=0; j<SAMPLESIZE; j++){
					IntWritable randnode = keys.get(rand.nextInt(stateTable.size()));
					randomkeys.add(randnode);
				}
				
				final Map<IntWritable, PriorityRecord<P, V>> langForSort = stateTable;
				Collections.sort(randomkeys, 
						new Comparator(){
							public int compare(Object left, Object right){
								V leftrecord = langForSort.get(left).getcState();
								V rightrecord = langForSort.get(right).getcState();
								P leftpriority = (P) iterReducer.setPriority((IntWritable)left, leftrecord);
								P rightpriority = (P) iterReducer.setPriority((IntWritable)right, rightrecord);
								return -leftpriority.compareTo(rightpriority);
							}
						});
				
				int cutindex = this.topk * SAMPLESIZE / this.stateTable.size();
				P threshold = stateTable.get(randomkeys.get(cutindex)).getPriority();
		
				for(IntWritable k : stateTable.keySet()){		
					V v = stateTable.get(k).getcState();
					P pri = (P) iterReducer.setPriority(k, v);
					if(pri.compareTo(threshold) > 0){
						writer.append(k, stateTable.get(k).getcState());
						//writer.write(k + "\t" + stateTable.get(k).getcState() + "\n");
					}			
				}	
			}
		}	
		writer.close();
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

	@Override
	public void collect(IntWritable key, V value) throws IOException {
		// TODO Auto-generated method stub
		
	}
}
