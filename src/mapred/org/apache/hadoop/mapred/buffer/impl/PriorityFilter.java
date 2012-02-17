package org.apache.hadoop.mapred.buffer.impl;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Valueable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.FileBasedUpdater;
import org.apache.hadoop.mapred.FileHandle;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.IFile.PriorityQueueWriter;
import org.apache.hadoop.mapred.buffer.OutputFile;

public class PriorityFilter<K extends Object, P extends Valueable, V extends Valueable, D extends Object> {
	private static final Log LOG = LogFactory.getLog(OutputPKVFile.class.getName());
	
	private JobConf conf;
	private FileHandle outputHandle = null;
	private TaskAttemptID taskAttemptID;
	private FileSystem fs;
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
	
	private Deserializer<K> keyDeserializer;
	private Deserializer<V> istateDeserializer;
	private Deserializer<V> cstateDeserializer;
	private Deserializer<D> dataDeserializer;
	
	private Path iStateFile4Read;
	private Path cStateFile4Read;
	private Path staticFile;
	private Path iStateFile4Write;
	private Path cStateFile4Write;
	private Path priQueueFile;
	
	private FileBasedUpdater<K, P, V, D> updater;
	
	public PriorityFilter(Task task, JobConf job, 
			Class<K> keyClass, Class<P> priClass, Class<V> valClass, Class<D> dataClass, 
			FileBasedUpdater<K, P, V, D> updater) throws IOException{
		this.conf = job;
		this.taskAttemptID = task.getTaskID();
    	this.outputHandle = new FileHandle(task.getJobID());
    	this.outputHandle.setConf(job);
		this.fs = FileSystem.get(job);
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
	    
		iStateFile4Read = outputHandle.getiStateFile4Read(taskAttemptID);
		cStateFile4Read = outputHandle.getcStateFile4Read(taskAttemptID);
		staticFile = outputHandle.getStaticFile(taskAttemptID);
		
		iStateFile4Write = outputHandle.getiStateFile4Write(taskAttemptID);
		cStateFile4Write = outputHandle.getcStateFile4Write(taskAttemptID);
		priQueueFile = outputHandle.getPriorityQueueFile(taskAttemptID);
	}
	
	public void initFiles() throws IOException{
		Writer<K, V> istate_writer = new Writer<K, V>(conf, fs, iStateFile4Write, keyClass, valClass, null, null);
		Writer<K, V> cstate_writer = new Writer<K, V>(conf, fs, cStateFile4Write, keyClass, valClass, null, null);
		Writer<K, D> static_writer = new Writer<K, D>(conf, fs, staticFile, keyClass, dataClass, null, null);
		PriorityQueueWriter<K, V, D> priqueue_writer = new PriorityQueueWriter<K, V, D>(conf, fs, priQueueFile, keyClass, valClass, dataClass, null, null);
		
		updater.initFiles(istate_writer, cstate_writer, static_writer, priqueue_writer);
	}
	
	/*input: istate file, cstate file, static file, priority threshold
	 * output: istate file, cstate file, priorityqueue file
	 * */
	public void generatePrioriyQueue(P prithreshold) throws IOException{

		Reader<K, V> istate_reader = new Reader<K, V>(conf, fs, iStateFile4Read, null, null);
		Reader<K, V> cstate_reader = new Reader<K, V>(conf, fs, cStateFile4Read, null, null);
		Reader<K, D> static_reader = new Reader<K, D>(conf, fs, staticFile, null, null);
		Writer<K, V> istate_writer = new Writer<K, V>(conf, fs, iStateFile4Write, keyClass, valClass, null, null);
		Writer<K, V> cstate_writer = new Writer<K, V>(conf, fs, cStateFile4Write, keyClass, valClass, null, null);
		PriorityQueueWriter<K, V, D> priqueue_writer = new PriorityQueueWriter<K, V, D>(conf, fs, priQueueFile, keyClass, valClass, dataClass, null, null);
	
		DataInputBuffer keyIn = new DataInputBuffer();
		DataInputBuffer istateIn = new DataInputBuffer();
		DataInputBuffer cstateIn = new DataInputBuffer();
		DataInputBuffer dataIn = new DataInputBuffer();
		
		while (istate_reader.next(keyIn, istateIn)) {
			keyDeserializer.open(keyIn);
			istateDeserializer.open(istateIn);
			key = keyDeserializer.deserialize(key);
			istate = istateDeserializer.deserialize(istate);

			cstate_reader.next(keyIn, cstateIn);
			
			keyDeserializer.open(keyIn);
			cstateDeserializer.open(cstateIn);
			key = keyDeserializer.deserialize(key);
			cstate = cstateDeserializer.deserialize(cstate);
			
			static_reader.next(keyIn, dataIn);
			
			keyDeserializer.open(keyIn);
			dataDeserializer.open(dataIn);
			key = keyDeserializer.deserialize(key);
			data = dataDeserializer.deserialize(data);
			
			V defaultistate = updater.resetiState();
			
			P pri = updater.decidePriority(key, istate);
			if(pri.compareTo(prithreshold) >= 0){
				V accum = updater.accumulate(istate, cstate);
				cstate_writer.append(key, accum);
				priqueue_writer.append(key, istate, data);
				istate_writer.append(key, defaultistate);
			}else{
				istate_writer.append(key, istate);
				cstate_writer.append(key, cstate);
			}
		}
		
		istate_reader.close();
		cstate_reader.close();
		static_reader.close();
		istate_writer.close();
		cstate_writer.close();
		priqueue_writer.close();
		
		//rename the istatefile_for_write to istatefile_for_read for next iteration
	    fs.delete(iStateFile4Read, false);
	    fs.delete(cStateFile4Read, false);
	    
		File toBeRenamed = new File(iStateFile4Write.getName());
		File newfile = new File(iStateFile4Read.getName());
		if(!toBeRenamed.renameTo(newfile)){
			LOG.error("error renaming file " + toBeRenamed + " to " + newfile);
		}
		
		toBeRenamed = new File(cStateFile4Write.getName());
		newfile = new File(cStateFile4Read.getName());
		if(!toBeRenamed.renameTo(newfile)){
			LOG.error("error renaming file " + toBeRenamed + " to " + newfile);
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
		IFile.Writer<K, V> writer = null;
		IFile.Writer<IntWritable, IntWritable> fakewriter = null;
		
		try{		
			out = localFs.create(filename, false);
			indexOut = localFs.create(indexFilename, false);
	
			if (out == null ) throw new IOException("Unable to create spill file " + filename);

			fakewriter = new IFile.Writer<IntWritable, IntWritable>(conf, out, IntWritable.class, IntWritable.class, null, null);
			fakewriter.append(new IntWritable(-521), new IntWritable(-594396));
			fakewriter.close();
	
			writeIndexRecord(indexOut, out, 0, writer);

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
		
		this.iteration++;
		return new OutputFile(this.taskAttemptID, this.iteration, filename, indexFilename, 1);
	}
}
