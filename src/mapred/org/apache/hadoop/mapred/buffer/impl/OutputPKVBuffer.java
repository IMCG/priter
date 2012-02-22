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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Valueable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileHandle;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.ReduceTask;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.Updater;
import org.apache.hadoop.mapred.buffer.BufferUmbilicalProtocol;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapred.buffer.OutputFile.Header;
import org.apache.hadoop.util.Progress;


public class OutputPKVBuffer<K extends Object, P extends Valueable, V extends Valueable> 
		implements OutputCollector<K, V>{
	
	public int SAMPLESIZE;
	//**************************************
	private static final Log LOG = LogFactory.getLog(OutputPKVBuffer.class.getName());

	private JobConf job = null;
	private ReduceTask relatedTask;
	private TaskAttemptID taskAttemptID;
	private final FileSystem localFs;
	private FileSystem hdfs;
	private FileHandle outputHandle = null;
	
	private boolean bPriExec;
	public String exeQueueFile;
	public String stateTableFile;
	private int checkFreq;
	private boolean ftSupport;
	public int checkpointIter;
	public int checkpointSnapshot;
	
	private String topkDir = null;
	private int topk;

    private Updater<K, P, V> updater = null;
	public Map<K, PriorityRecord<P, V>> stateTable = new HashMap<K, PriorityRecord<P, V>>();
	private ArrayList<KVRecord<K, V>> priorityQueue = new ArrayList<KVRecord<K, V>>();
	private K defaultKey;
    private V defaultiState;
    private Class<K> keyClass;
	private Class<P> priClass;
	private Class<V> valClass;
      
	public int iteration = 0;
	public long total_map = 0;
	public int total_reduce = 0;
	public double progress = 0;
	public boolean start = false;
	private boolean bMemTrans = false;
	
	private int partitions = 0;
	private boolean bPortion = false;
	private boolean bLength = false;
	private boolean bUniLen = false;
	private double queueportion = 0.2;
	private int queuelen = 0;
	private int queuetop = -1;
	
	public static int WAIT_ITER = 0;
	
	public OutputPKVBuffer(BufferUmbilicalProtocol umbilical, Task task, JobConf job, 
				Reporter reporter, Progress progress, 
				Class<K> keyClass, Class<P> priClass, Class<V> valClass, 
				Updater<K, P, V> updater) throws IOException{	
		
		LOG.info("OutputPKVBuffer is reset for task " + task.getTaskID());

		this.job = job;
		this.relatedTask = (ReduceTask)task;
		this.taskAttemptID = task.getTaskID();
		this.localFs = FileSystem.getLocal(job);
		this.hdfs = FileSystem.get(job);
		this.outputHandle = new FileHandle(taskAttemptID.getJobID());
		this.outputHandle.setConf(job);
		this.updater = updater;		
		this.defaultKey = (K) (new IntWritable(0));
		this.defaultiState = (V)updater.resetiState();
		
		this.bPriExec = job.getBoolean("priter.job.priority", true);
		this.ftSupport = job.getBoolean("priter.checkpoint", true);
		this.exeQueueFile = job.get("mapred.output.dir") + "/_ExeQueueTemp/" + 
							taskAttemptID.getTaskID().getId() + "-exequeue";
		this.stateTableFile = job.get("mapred.output.dir") + "/_StateTableTempDir/"
								+ taskAttemptID.getTaskID().getId() + "-statetable";
		this.checkFreq = job.getInt("priter.checkpoint.frequency", 10);
		this.SAMPLESIZE = job.getInt("priter.job.samplesize", 1000);

		this.topkDir = job.get("mapred.output.dir") + "/" + this.taskAttemptID.getTaskID().getId();
		this.keyClass = keyClass;
		this.valClass = valClass;
		this.priClass = priClass;

		partitions = job.getInt("priter.graph.partitions", 0);
		if(partitions == 0){
			JobClient jobclient = new JobClient(job);
			ClusterStatus status = jobclient.getClusterStatus();
			partitions = 2 * status.getTaskTrackers();
		}
		
		if(job.getFloat("priter.queue.portion", -1) != -1){
			this.bPortion = true;
			this.queueportion = job.getFloat("priter.queue.portion", 1);
		}else if(job.getInt("priter.queue.length", -1) != -1){
			this.bLength = true;
			this.queuelen = job.getInt("priter.queue.length", 10000);
		}else if(job.getInt("priter.queue.uniqlength", -1) != -1){
			this.bUniLen = true;
			this.queuetop = job.getInt("priter.queue.uniqlength", -1);
		}
		
		this.topk = job.getInt("priter.snapshot.topk", 1000);
		this.topk = this.topk * job.getInt("priter.snapshot.topk.scale", 4) / partitions;
		
		this.bMemTrans = job.getBoolean("priter.transfer.mem", false);
		
		this.updater.initStateTable(this);
	}

	public Header header() {
		return new OutputFile.PKVBufferHeader(this.taskAttemptID, this.iteration);
	}
	
	public TaskAttemptID getTaskAttemptID() {
		return this.taskAttemptID;
	}
	
	public int getIteration(){
		return this.iteration;
	}

	public void init(K key, V iState, V cState){
		P pri = updater.decidePriority(key, iState);
		PriorityRecord<P, V> newpkvRecord = new PriorityRecord<P, V>(pri, iState, cState);
		this.stateTable.put(key, newpkvRecord);
	}
	
	private synchronized void getAllRecords(ArrayList<KVRecord<K, V>> records) {
		synchronized(this.stateTable){	
			int activations = 0;
			for(K k : stateTable.keySet()){		
				V v = stateTable.get(k).getiState();
	
				records.add(new KVRecord<K, V>(k, v));
				V iState = updater.resetiState();
				this.stateTable.get(k).setiState(iState);
				P p = updater.decidePriority(k, iState);
				this.stateTable.get(k).setPriority(p);
				activations++;
			}
			LOG.info("iteration " + iteration + " activate " + activations + " k-v pairs");
		}
	}
	private synchronized void getTopRecords(ArrayList<KVRecord<K, V>> records) {
		
		synchronized(this.stateTable){	
			int activations = 0;
			P threshold = updater.decidePriority((K)(new IntWritable(0)), updater.resetiState());
			
			//select eligible records to keys
			List<K> keys = new ArrayList<K>();
			for(K k : stateTable.keySet()){	
				//compare with the default value
				if(stateTable.get(k).getPriority().compareTo(threshold) > 0){
					keys.add(k);
				}
			}
			
			if(this.bPortion){
				int actualqueuelen = (int) (keys.size() * this.queueportion);
				//queulen extraction
				if(keys.size() <= SAMPLESIZE){
					for(K k : keys){		
						V v = stateTable.get(k).getiState();
						records.add(new KVRecord<K, V>(k, v));
						V iState = updater.resetiState();
						this.stateTable.get(k).setiState(iState);
						P p = updater.decidePriority(k, iState);
						this.stateTable.get(k).setPriority(p);
						activations++;	
					}
					LOG.info("iteration " + iteration + "queuelen is " + actualqueuelen + " expend " + activations + " k-v pairs");
				}else{
					Random rand = new Random();
					List<K> randomkeys = new ArrayList<K>(SAMPLESIZE);

					for(int j=0; j<SAMPLESIZE; j++){
						K randnode = keys.get(rand.nextInt(keys.size()));
						randomkeys.add(randnode);
					}
					
					final Map<K, PriorityRecord<P, V>> langForSort = stateTable;
					Collections.sort(randomkeys, 
							new Comparator(){
								public int compare(Object left, Object right){
									PriorityRecord<P, V> leftrecord = langForSort.get(left);
									PriorityRecord<P, V> rightrecord = langForSort.get(right);
									return -leftrecord.compareTo(rightrecord);
								}
							});
					
					//int cutindex = actualqueuelen * SAMPLESIZE / this.stateTable.size();
					int cutindex = actualqueuelen * SAMPLESIZE / keys.size();

					threshold = stateTable.get(randomkeys.get(cutindex-1>=0?cutindex-1:0)).getPriority();
					
					LOG.info("queuelen " + actualqueuelen + " eliglbe records " + keys.size() + 
							" cut index " + cutindex + " threshold is " + threshold);
					
					//to avoid 0 output, we emit all the records have the equal value of threshld
					if(cutindex==0 || stateTable.get(randomkeys.get(0)).getPriority() == threshold){
						for(K k : stateTable.keySet()){	
							V v = stateTable.get(k).getiState();
							P pri = stateTable.get(k).getPriority();
							
							if(pri.compareTo(threshold) >= 0){
								records.add(new KVRecord<K, V>(k, v));
								V iState = updater.resetiState();
								this.stateTable.get(k).setiState(iState);
								P p = updater.decidePriority(k, iState);
								this.stateTable.get(k).setPriority(p);
								activations++;
							}
						}
					}else{
						for(K k : keys){		
							V v = stateTable.get(k).getiState();
							P pri = stateTable.get(k).getPriority();
							
							if(pri.compareTo(threshold) > 0){
								records.add(new KVRecord<K, V>(k, v));
								V iState = updater.resetiState();
								this.stateTable.get(k).setiState(iState);
								P p = updater.decidePriority(k, iState);
								this.stateTable.get(k).setPriority(p);
								activations++;
							}
						}
					}

					LOG.info("iteration " + iteration + " expend " + activations + " k-v pairs" + " threshold is " + threshold);
				}
			}else if(this.bLength){
				int actualqueuelen = this.queuelen;
				/*
				//for asynchronous execution, determine the queue size based on how much portion of information received (from edges)
				if(job.getBoolean("priter.job.async.self", false) || job.getBoolean("priter.job.async.time", false)){
					actualqueuelen = (int) (queuelen * ((double)updatedEdges / totalEdges));
					LOG.info("actual queue len " + actualqueuelen + " queuelen " + queuelen + " updated edges " + updatedEdges);
					if(actualqueuelen > queuelen) actualqueuelen = queuelen;	
				}
				*/
				
				//queulen extraction
				if(keys.size() <= SAMPLESIZE){
					for(K k : keys){		
						V v = stateTable.get(k).getiState();			
						records.add(new KVRecord<K, V>(k, v));
						V iState = updater.resetiState();
						this.stateTable.get(k).setiState(iState);
						P p = updater.decidePriority(k, iState);
						this.stateTable.get(k).setPriority(p);
						activations++;	
					}
					LOG.info("iteration " + iteration + "queuelen is " + actualqueuelen + " expend " + activations + " k-v pairs");
				}else{
					Random rand = new Random();
					List<K> randomkeys = new ArrayList<K>(SAMPLESIZE);
					
					for(int j=0; j<SAMPLESIZE; j++){
						K randnode = keys.get(rand.nextInt(keys.size()));
						randomkeys.add(randnode);
					}
					
					final Map<K, PriorityRecord<P, V>> langForSort = stateTable;
					Collections.sort(randomkeys, 
							new Comparator(){
								public int compare(Object left, Object right){
									PriorityRecord<P, V> leftrecord = langForSort.get(left);
									PriorityRecord<P, V> rightrecord = langForSort.get(right);
									return -leftrecord.compareTo(rightrecord);
								}
							});
					
					int cutindex = actualqueuelen * SAMPLESIZE / keys.size();

					threshold = stateTable.get(randomkeys.get(cutindex)).getPriority();
					
					LOG.info("queuelen " + actualqueuelen + " eliglbe records " + keys.size() + 
							" cut index " + cutindex + " threshold is " + threshold);

					//to avoid 0 output, we emit all the records have the equal value of threshld
					if(cutindex==0 || stateTable.get(randomkeys.get(0)).getPriority() == threshold){
						for(K k : stateTable.keySet()){	
							V v = stateTable.get(k).getiState();
							P pri = stateTable.get(k).getPriority();
							
							if(pri.compareTo(threshold) >= 0){
								records.add(new KVRecord<K, V>(k, v));
								V iState = updater.resetiState();
								this.stateTable.get(k).setiState(iState);
								P p = updater.decidePriority(k, iState);
								this.stateTable.get(k).setPriority(p);
								activations++;
							}
						}
					}else{
						for(K k : keys){		
							V v = stateTable.get(k).getiState();
							P pri = stateTable.get(k).getPriority();
							
							if(pri.compareTo(threshold) > 0){
								records.add(new KVRecord<K, V>(k, v));
								V iState = updater.resetiState();
								this.stateTable.get(k).setiState(iState);
								P p = updater.decidePriority(k, iState);
								this.stateTable.get(k).setPriority(p);
								activations++;
							}
						}
					}
					LOG.info("iteration " + iteration + " expend " + activations + " k-v pairs" + " threshold is " + threshold);
				}
			}else if(this.bUniLen){
				//queue top extraction
				Random rand = new Random();
				TreeSet<P> randomPris = new TreeSet<P>(new Comparator(){
					@Override
					public int compare(Object left, Object right) {
						// TODO Auto-generated method stub
						return -((P)left).compareTo((P)right);
					}
				});

				for(int j=0; j<SAMPLESIZE; j++){
					P randpri = stateTable.get(keys.get(rand.nextInt(keys.size()))).getPriority();
					randomPris.add(randpri);
				}
				
				if(randomPris.size() <= queuetop){
					threshold = randomPris.pollLast();
				}else{
					for(int i=0; i<queuetop; i++){
						threshold = randomPris.pollFirst();
					}
				}

				LOG.info("queue top " + queuetop + " table size " + stateTable.size());
				
				for(K k : keys){		
					V v = stateTable.get(k).getiState();
					P pri = stateTable.get(k).getPriority();
					
					if(pri.compareTo(threshold) > 0){
						records.add(new KVRecord<K, V>(k, v));
						V iState = updater.resetiState();
						this.stateTable.get(k).setiState(iState);
						P p = updater.decidePriority(k, iState);
						this.stateTable.get(k).setPriority(p);
						activations++;
					}
				}
				LOG.info("iteration " + iteration + " expend " + activations + " k-v pairs" + " threshold is " + threshold);
			}
		}
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
		start = true;
		
		if(bMemTrans){
			//mem trans seems not work, i forgot whether I have implemented it or not
			OutputFile outfile;
			if(job.getBoolean("priter.job.mapsync", false)){
				outfile = new OutputFile(this.taskAttemptID, this.iteration, new Path("/tmp/t1"), new Path("/tmp/t1"), partitions);
			}else{
				outfile = new OutputFile(this.taskAttemptID, this.iteration, new Path("/tmp/t1"), new Path("/tmp/t1"), partitions);
			}
			
			synchronized(this.stateTable){
				synchronized(outfile.outputMemQueue){
					if(this.bPriExec){
						getTopRecords(outfile.outputMemQueue);
					}else{
						getAllRecords(outfile.outputMemQueue);
					}
					
					if(job.getBoolean("priter.job.mapsync", false)){
						for(int i=0; i<partitions; i++){
							outfile.outputMemQueue.add(new KVRecord(defaultKey, defaultiState));
						}
					}
					
					if(outfile.outputMemQueue.size() == 0){
						LOG.info("no records to send");
						outfile.outputMemQueue.add(new KVRecord(defaultKey, defaultiState));
					}else{
						total_map += outfile.outputMemQueue.size();
					}
					
					LOG.info("iteration " + this.iteration + " expand " + outfile.outputMemQueue.size() + " k-v pairs, " +
							"total maps " + total_map + " total collected " + total_reduce);

					//periodically dump statetable and execution queue
					if(ftSupport && iteration != 0 && (iteration % checkFreq == 0) && (checkpointIter < iteration)){
						dumpStateTable();
						checkpointIter = iteration;
						checkpointSnapshot = this.relatedTask.snapshotIndex;
					}
					
					this.iteration++;
				}
			}

			return outfile;
	
		}else{
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
				
				synchronized(this.stateTable){
		
					//ArrayList<KVRecord<K, V>> entries = null;
					if(this.bPriExec){
						getTopRecords(priorityQueue);
					}else{
						getAllRecords(priorityQueue);
					}
						
					writer = new IFile.Writer<K, V>(job, out, keyClass, valClass, null, null);

					//if(entries != null) this.priorityQueue.addAll(entries);
					int count = 0;
							
					if(priorityQueue.size() == 0){
						LOG.info("no records to send");
						writer.append(this.defaultKey, this.defaultiState);
					}else{
						for(KVRecord<K, V> entry : priorityQueue){		
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
					
					
					//periodically dump statetable and execution queue
					if(ftSupport && iteration != 0 && (iteration % checkFreq == 0) && (checkpointIter < iteration)){
						//dumpExeQueue();
						dumpStateTable();
						checkpointIter = iteration;
						checkpointSnapshot = this.relatedTask.snapshotIndex;
					}
					
					this.iteration++;
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
					
			int partition_num = job.getBoolean("priter.job.mapsync", false) ? this.partitions : 1;
			return new OutputFile(this.taskAttemptID, this.iteration, filename, indexFilename, partition_num);
		}
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
	
	private void dumpExeQueue() throws IOException {
		FSDataOutputStream ostream = hdfs.create(new Path(exeQueueFile), true);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(ostream));
		
		int count = 0;
		for(KVRecord<K, V> entry : priorityQueue) {
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
		
		long start = System.currentTimeMillis();
		int count = 0;
		Set<Map.Entry<K, PriorityRecord<P, V>>> entries = this.stateTable.entrySet();
		for(Map.Entry<K, PriorityRecord<P, V>> entry : entries) {
		    K key = entry.getKey();
		    PriorityRecord<P, V> record = entry.getValue();
		    writer.write(key + "\t" + record + "\n");
		    count++;
		}
		writer.close();
		ostream.close();
		long end = System.currentTimeMillis();
		
		LOG.info("dumped StateTable with " + count + " records takes " + (end-start) + " ms");
	}
	
	public int loadStateTable() throws IOException {
		FSDataInputStream istream = hdfs.open(new Path(stateTableFile));
		BufferedReader reader = new BufferedReader(new InputStreamReader(istream));
		
		int count = 0;
		while(reader.ready()){
			String line = reader.readLine();
			String[] field = line.split("\t", 4);
			
			K key = null;
			if(keyClass == IntWritable.class){
				key = (K) new IntWritable(Integer.parseInt(field[0]));
			}else if(keyClass == Text.class){
				key = (K) new Text(field[0]);
			}else if(keyClass == FloatWritable.class){
				key = (K) new FloatWritable(Float.parseFloat(field[0]));
			}else if(keyClass == DoubleWritable.class){
				key = (K) new DoubleWritable(Double.parseDouble(field[0]));
			}

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
		IFile.PriorityWriter<P, K, V> writer = new IFile.PriorityWriter<P, K, V>(job, hdfs, topkFile, priClass, keyClass, valClass, null, null);

		synchronized(this.stateTable){		
			if(stateTable.size() <= topk || stateTable.size() <= SAMPLESIZE){
				for(K k : stateTable.keySet()){	
					V v = stateTable.get(k).getcState();
					P pri = updater.decideTopK(k, v);
					writer.append(pri, k, stateTable.get(k).getcState());	
					progress += v.getV();
				}
			}else{
				Random rand = new Random();
				List<K> randomkeys = new ArrayList<K>(SAMPLESIZE);
				List<K> keys = new ArrayList<K>(stateTable.keySet());
				
				for(int j=0; j<SAMPLESIZE; j++){
					K randnode = keys.get(rand.nextInt(stateTable.size()));
					if(!randomkeys.contains(randnode)){
						randomkeys.add(randnode);
					}
				}
				
				final Map<K, PriorityRecord<P, V>> langForSort = stateTable;
				Collections.sort(randomkeys, 
						new Comparator(){
							public int compare(Object left, Object right){
								V leftrecord = langForSort.get(left).getcState();
								V rightrecord = langForSort.get(right).getcState();
								P leftpriority = updater.decideTopK((K)left, leftrecord);
								P rightpriority = updater.decideTopK((K)right, rightrecord);
								return -leftpriority.compareTo(rightpriority);
							}
						});
				
				int cutindex = this.topk * randomkeys.size() / this.stateTable.size()+1;
				P threshold = (P) updater.decideTopK(randomkeys.get(cutindex), stateTable.get(randomkeys.get(cutindex)).getcState());
		
				LOG.info("table size " + this.stateTable.size() + " cutindex " + cutindex + " threshold " + threshold);
				for(K k : stateTable.keySet()){		
					V v = stateTable.get(k).getcState();
					P pri = updater.decideTopK(k, v);
					if(pri.compareTo(threshold) > 0){
						writer.append(pri, k, stateTable.get(k).getcState());
					}
					progress += v.getV();
				}	
			}
		}	
		writer.close();
	}
	
	public double measureProgress(){
		double progress = 0;
		double defaultV = defaultiState.getV();
		for(K k : stateTable.keySet()){	
			V cstate = stateTable.get(k).getcState();
			if(cstate.getV() != defaultV) progress += cstate.getV();
		}
		return progress;
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
	public void collect(K key, V value) throws IOException {
		// TODO Auto-generated method stub
		
	}
}
