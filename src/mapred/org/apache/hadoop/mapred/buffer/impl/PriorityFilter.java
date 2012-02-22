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
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Valueable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileBasedUpdater;
import org.apache.hadoop.mapred.FileHandle;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.IFile.PriorityQueueWriter;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVFile.KPRecord;

public class PriorityFilter<K extends Object, P extends Valueable, V extends Valueable, D extends Object> {
	private static final Log LOG = LogFactory.getLog(OutputPKVFile.class.getName());
	
	class KPRecord<K, P>{
		K key;
		P pri;
		
		public KPRecord(K k, P p){
			key = k;
			pri = p;
		}
	}
	
	private JobConf conf;
	private FileHandle outputHandle = null;
	private TaskAttemptID taskAttemptID;
	private FileSystem hdfs;
	private final FileSystem localFs;
	private int iteration = 0;

    private Class<K> keyClass;
	private Class<P> priClass;
	private Class<V> valClass;
	private Class<D> dataClass;
	private K key;
	private V istate;
	private V cstate;
	private D data;
    
    public long localRecords = 0;
    public long activations = 0;
    public double progress = 0;
    public boolean init = false;
    private int partitions = 0;
    
    private int SAMPLESIZE;
    private ArrayList<KPRecord<K, P>> samples;
    private String topkDir = null;
    private P topk_threshold;
    private long counter = 0;
    private int topk;
    private long checkInterval;
    
    private Object synclock = new Object();
	
	private Deserializer<K> keyDeserializer;
	private Deserializer<V> istateDeserializer;
	private Deserializer<V> cstateDeserializer;
	private Deserializer<D> dataDeserializer;
	
	private Path iStateFileIntermediate;
	private Path cStateFileCurr;
	private Path staticFile;
	private Path iStateFile;
	private Path cStateFileNext;
	private Path priQueueFile;
	private Path signalFile;
	
	private FileBasedUpdater<K, P, V, D> updater;
	
	public PriorityFilter(Task task, JobConf job, 
			Class<K> keyClass, Class<P> priClass, Class<V> valClass, Class<D> dataClass, 
			FileBasedUpdater<K, P, V, D> updater) throws IOException{
		this.conf = job;
		this.taskAttemptID = task.getTaskID();
    	this.outputHandle = new FileHandle(task.getJobID());
    	this.outputHandle.setConf(job);
    	this.hdfs = FileSystem.get(job);
		this.localFs = FileSystem.getLocal(job);
		this.updater = updater;
		
		this.keyClass = keyClass;
		this.valClass = valClass;
		this.priClass = priClass;
		this.dataClass = dataClass;
		
		SerializationFactory serializationFactory = new SerializationFactory(conf);
	    this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
	    this.istateDeserializer = serializationFactory.getDeserializer(valClass);
	    this.cstateDeserializer = serializationFactory.getDeserializer(valClass);
	    this.dataDeserializer = serializationFactory.getDeserializer(dataClass);
	    
	    iStateFileIntermediate = outputHandle.getiStateFileIntermediate(taskAttemptID);
		cStateFileCurr = outputHandle.getcStateFile(taskAttemptID, iteration);
		staticFile = outputHandle.getStaticFile(taskAttemptID);
		
		iStateFile = outputHandle.getiStateFile(taskAttemptID);
		cStateFileNext = outputHandle.getcStateFile(taskAttemptID, iteration+1);
		priQueueFile = outputHandle.getPriorityQueueFile(taskAttemptID);
		
		signalFile = outputHandle.getSignalFile(taskAttemptID);
		
		partitions = job.getInt("priter.graph.partitions", 0);
		if(partitions == 0){
			JobClient jobclient = new JobClient(job);
			ClusterStatus status = jobclient.getClusterStatus();
			partitions = 2 * status.getTaskTrackers();
		}
		
		topkDir = job.get("mapred.output.dir") + "/" + this.taskAttemptID.getTaskID().getId();
		this.topk = job.getInt("priter.snapshot.topk", 1000);
		this.topk = this.topk * job.getInt("priter.snapshot.topk.scale", 4) / partitions;
		SAMPLESIZE = job.getInt("priter.job.samplesize", 1000);
		topk_threshold = (P)updater.resetiState();
	}
	
	public long initFiles() throws IOException{

		Writer<K, V> istate_writer = new Writer<K, V>(conf, localFs, iStateFile, keyClass, valClass, null, null);
		Writer<K, V> cstate_writer = new Writer<K, V>(conf, localFs, cStateFileCurr, keyClass, valClass, null, null);
		Writer<K, D> static_writer = new Writer<K, D>(conf, localFs, staticFile, keyClass, dataClass, null, null);
		PriorityQueueWriter<K, V, D> priqueue_writer = new PriorityQueueWriter<K, V, D>(conf, localFs, priQueueFile, keyClass, valClass, dataClass, null, null);
		
		LOG.info("initializing files...");
		LOG.info("initializing ... " + iStateFile);
		LOG.info("initializing ... " + cStateFileCurr);
		LOG.info("initializing ... " + staticFile);
		LOG.info("initializing ... " + priQueueFile);
		
		localRecords = updater.initFiles(istate_writer, cstate_writer, static_writer, priqueue_writer);
		
		istate_writer.close();
		cstate_writer.close();
		static_writer.close();
		priqueue_writer.close();
		
		localFs.create(signalFile);
		
		checkInterval = localRecords / SAMPLESIZE + 1;
		samples = new ArrayList<KPRecord<K, P>>(localRecords > SAMPLESIZE ? SAMPLESIZE : (int)localRecords);
		
		init = true;

		return localRecords;
	}
	
	/*input: istate file, cstate file, static file, priority threshold
	 * output: istate file, cstate file, priorityqueue file
	 * */
	public void generatePrioriyQueue(P prithreshold) throws IOException{

		synchronized(synclock){
			cStateFileCurr = outputHandle.getcStateFile(taskAttemptID, iteration);
			cStateFileNext = outputHandle.getcStateFile(taskAttemptID, iteration+1);
			
			Reader<K, V> istate_reader = new Reader<K, V>(conf, localFs, iStateFileIntermediate, null, null);
			Reader<K, V> cstate_reader = new Reader<K, V>(conf, localFs, cStateFileCurr, null, null);
			Reader<K, D> static_reader = new Reader<K, D>(conf, localFs, staticFile, null, null);
			Writer<K, V> istate_writer = new Writer<K, V>(conf, localFs, iStateFile, keyClass, valClass, null, null);
			Writer<K, V> cstate_writer = new Writer<K, V>(conf, localFs, cStateFileNext, keyClass, valClass, null, null);
			PriorityQueueWriter<K, V, D> priqueue_writer = new PriorityQueueWriter<K, V, D>(conf, localFs, priQueueFile, keyClass, valClass, dataClass, null, null);
		
			DataInputBuffer keyIn = new DataInputBuffer();
			DataInputBuffer istateIn = new DataInputBuffer();
			DataInputBuffer cstateIn = new DataInputBuffer();
			DataInputBuffer dataIn = new DataInputBuffer();
			
			samples.clear();
			while (istate_reader.next(keyIn, istateIn)) {
				keyDeserializer.open(keyIn);
				istateDeserializer.open(istateIn);
				key = keyDeserializer.deserialize(key);
				istate = istateDeserializer.deserialize(istate);
	
				if(!cstate_reader.next(keyIn, cstateIn)){
					throw new IOException("cstate file length not match to intermediate file");
				}
				
				keyDeserializer.open(keyIn);
				cstateDeserializer.open(cstateIn);
				key = keyDeserializer.deserialize(key);
				cstate = cstateDeserializer.deserialize(cstate);
				
				if(!static_reader.next(keyIn, dataIn)){
					throw new IOException("static file length not match to intermediate file");
				}
				
				keyDeserializer.open(keyIn);
				dataDeserializer.open(dataIn);
				key = keyDeserializer.deserialize(key);
				data = dataDeserializer.deserialize(data);
				
				V defaultistate = updater.resetiState();
				
				P pri = updater.decidePriority(key, istate);
				if(pri.compareTo(prithreshold) >= 0){
					//LOG.info("key " + key + " value " + istate + " threshold " + prithreshold);
					V accum = updater.accumulate(istate, cstate);
					cstate_writer.append(key, accum);
					priqueue_writer.append(key, istate, data);
					istate_writer.append(key, defaultistate);
					
					activations++;
				}else{
					istate_writer.append(key, istate);
					cstate_writer.append(key, cstate);
				}
				
				//sampling for topk extraction
				if(++counter % checkInterval == 0){
					pri = updater.decideTopK(key, cstate);
					samples.add(new KPRecord<K, P>(key, pri));
				}
				
			}
			
			LOG.info("we have collected samples " + samples.size() + " counter is " + counter);
			
			istate_reader.close();
			cstate_reader.close();
			static_reader.close();
			istate_writer.close();
			cstate_writer.close();
			priqueue_writer.close();
			
			localFs.delete(iStateFileIntermediate, false);

			this.iteration++;
			
			if(iteration % 10 == 0){
				for(int i=iteration-10; i<iteration; i++){
					Path oldStateFile = outputHandle.getcStateFile(taskAttemptID, i);
					localFs.delete(oldStateFile, false);
				}
			}
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
	
	public OutputFile generateFakeOutput() throws IOException{
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
			throw new IOException("generating fake output -- spill file exists! " + filename);
		}

		FSDataOutputStream out = null;
		FSDataOutputStream indexOut = null;
		IFile.Writer<K, V> fakewriter = null;
		
		try{		
			out = localFs.create(filename, false);
			indexOut = localFs.create(indexFilename, false);
	
			if (out == null ) throw new IOException("Unable to create spill file " + filename);

			fakewriter = new IFile.Writer<K, V>(conf, out, keyClass, valClass, null, null);
			//fakewriter.append(this.defaultKey, this.defaultiState);
	
			writeIndexRecord(indexOut, out, 0, fakewriter);

		}catch(IOException e){
			e.printStackTrace();
		}finally {
			if (null != fakewriter) {
				fakewriter.close();
				fakewriter = null;
			}
			
			if (out != null){
				out.close();
				out = null;
			}
			if (indexOut != null) {
				indexOut.close();
				indexOut = null;
			}
		}
		
		return new OutputFile(this.taskAttemptID, this.iteration, filename, indexFilename, 1);
	}
	
	public void snapshot(int index) throws IOException, InterruptedException{
		
		//before entering the lock area, check whether the cstate file for next iteration have been generated
		cStateFileNext = outputHandle.getcStateFile(taskAttemptID, iteration);
		while(!localFs.exists(cStateFileNext) || iteration < 2){
			Thread.sleep(500);
		}
		
		synchronized(synclock){
			if(counter > Long.MAX_VALUE / 2) counter = 0;
			
			LOG.info("OK, lock is released");
			
			cStateFileNext = outputHandle.getcStateFile(taskAttemptID, iteration);
			Reader<K, V> cstate_reader = new Reader<K, V>(conf, localFs, cStateFileNext, null, null);
			Path topkFile = new Path(topkDir + "/topKsnapshot-" + index);
			IFile.PriorityWriter<P, K, V> topkwriter = new IFile.PriorityWriter<P, K, V>(conf, hdfs, topkFile, priClass, keyClass, valClass, null, null);
	
			DataInputBuffer keyIn = new DataInputBuffer();
			DataInputBuffer cstateIn = new DataInputBuffer();
			
			progress = 0;
			
			Date start = new Date();
			int cutindex = 0;
			
			if(localRecords <= topk || localRecords <= SAMPLESIZE){
				while (cstate_reader.next(keyIn, cstateIn)) {
					keyDeserializer.open(keyIn);
					cstateDeserializer.open(cstateIn);
					key = keyDeserializer.deserialize(key);
					cstate = cstateDeserializer.deserialize(cstate);
					P pri = updater.decidePriority(key, cstate);
					
					topkwriter.append(pri, key, cstate);	
					progress += cstate.getV();
				}
			}else{
				if(samples.size() == 0){
					//in the interval period, we didn't finish one subpass, so samples set is empty
					LOG.warn("the snapshot interval is set too short, please set it longer!!!");
				}else{
					Collections.sort(samples, 
							new Comparator(){
								public int compare(Object left, Object right){
									P leftrecord = ((KPRecord<K, P>)left).pri;
									P rightrecord = ((KPRecord<K, P>)right).pri;
									return -leftrecord.compareTo(rightrecord);
								}
							});
		
					if(samples.size() == 0){
						throw new IOException("no samples iteration " + iteration);
					}
					
					cutindex = (int)(this.topk * samples.size() / localRecords) + 1;
					topk_threshold = samples.get(cutindex-1>=0?cutindex-1:0).pri;
				}
			
				while (cstate_reader.next(keyIn, cstateIn)) {
					keyDeserializer.open(keyIn);
					cstateDeserializer.open(cstateIn);
					key = keyDeserializer.deserialize(key);
					cstate = cstateDeserializer.deserialize(cstate);
					P pri = updater.decidePriority(key, cstate);
					
					if(pri.compareTo(topk_threshold) > 0){
						topkwriter.append(pri, key, cstate);
					}
					
					progress += cstate.getV();
				}
			}
			Date end = new Date();
			long spendtime = (end.getTime() - start.getTime());
			LOG.info("table size " + localRecords + " sample size " + samples.size() 
				+ " cutindex " + cutindex + " threshold " + topk_threshold + " use time " + spendtime);
			
			samples.clear();
			cstate_reader.close();
			topkwriter.close();
		}
	}
	
	public double measureProgress() throws IOException{
		synchronized(synclock){
			Reader<K, V> cstate_reader = new Reader<K, V>(conf, localFs, cStateFileNext, null, null);
			DataInputBuffer keyIn = new DataInputBuffer();
			DataInputBuffer cstateIn = new DataInputBuffer();
			
			while (cstate_reader.next(keyIn, cstateIn)) {
				cstateDeserializer.open(cstateIn);
				cstate = cstateDeserializer.deserialize(cstate);
				progress += cstate.getV();
			}
		
		}
		return progress;
	}
	
	public int getIteration(){
		return iteration;
	}
}
