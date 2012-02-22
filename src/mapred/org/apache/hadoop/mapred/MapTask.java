/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import static org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.TaskCompletionEvent.Status;
import org.apache.hadoop.mapred.buffer.BufferUmbilicalProtocol;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapred.buffer.impl.InputKVDFile;
import org.apache.hadoop.mapred.buffer.impl.InputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.JOutputBuffer;
import org.apache.hadoop.mapred.buffer.impl.UnSortOutputBuffer;
import org.apache.hadoop.mapred.buffer.net.BufferExchange;
import org.apache.hadoop.mapred.buffer.net.BufferRequest;
import org.apache.hadoop.mapred.buffer.net.BufferExchangeSink;
import org.apache.hadoop.mapred.buffer.net.ReduceBufferRequest;
import org.apache.hadoop.mapred.lib.PrIterPartitioner;
import org.apache.hadoop.util.ReflectionUtils;

/** A Map task. */
public class MapTask extends Task {
	
	private class ReduceOutputFetcher extends Thread {
		private TaskID oneReduceTaskId;

		private TaskUmbilicalProtocol trackerUmbilical;
		
		private BufferUmbilicalProtocol bufferUmbilical;
		
		private BufferExchangeSink sink;
		
		public ReduceOutputFetcher(TaskUmbilicalProtocol trackerUmbilical, 
				BufferUmbilicalProtocol bufferUmbilical, 
				BufferExchangeSink sink,
				TaskID reduceTaskId) {
			this.trackerUmbilical = trackerUmbilical;
			this.bufferUmbilical = bufferUmbilical;
			this.sink = sink;
			this.oneReduceTaskId = reduceTaskId;
		}

		public void run() {
			Set<TaskID> finishedReduceTasks = new HashSet<TaskID>();
			Set<TaskAttemptID>  reduceTasks = new HashSet<TaskAttemptID>();
			int eid = 0;
			
			while (!isInterrupted() && finishedReduceTasks.size() < getNumberOfInputs()+1) {
				try {
					ReduceTaskCompletionEventsUpdate updates = 
						trackerUmbilical.getReduceCompletionEvents(getJobID(), eid, Integer.MAX_VALUE);

					eid += updates.events.length;

					//LOG.info("get reduce task completion events : " + eid);
					// Process the TaskCompletionEvents:
					// 1. Save the SUCCEEDED maps in knownOutputs to fetch the outputs.
					// 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to stop fetching
					//    from those maps.
					// 3. Remove TIPFAILED maps from neededOutputs since we don't need their
					//    outputs at all.
					for (TaskCompletionEvent event : updates.events) {
						//LOG.info("event is " + event + " status is " + event.getTaskStatus());
						switch (event.getTaskStatus()) {
						case FAILED:
						case KILLED:
						case OBSOLETE:
						case TIPFAILED:
						{
							TaskAttemptID reduceTaskId = event.getTaskAttemptId();
							if (!reduceTasks.contains(reduceTaskId)) {
								reduceTasks.remove(reduceTaskId);
							}
						}
						case SUCCEEDED:
						{
							TaskAttemptID mapTaskId = event.getTaskAttemptId();
							finishedReduceTasks.add(mapTaskId.getTaskID());
						}
						case RUNNING:
						{
							URI u = URI.create(event.getTaskTrackerHttp());
							String host = u.getHost();
							TaskAttemptID reduceTasktId = event.getTaskAttemptId();
							
							//LOG.info(reduceAttemptId.getTaskID() + " : " + reduceTaskId);

							if(job.getBoolean("priter.job.mapsync", false)){
								if (!reduceTasks.contains(reduceTasktId)) {
									BufferExchange.BufferType type = BufferExchange.BufferType.PKVBUF;
									
									BufferRequest request = 
										new ReduceBufferRequest(host, getTaskID(), sink.getAddress(), type, reduceTasktId.getTaskID());
									try {
										bufferUmbilical.request(request);
										reduceTasks.add(reduceTasktId);
										if (reduceTasks.size() == getNumberOfInputs()) {
											LOG.info("ReduceTask " + getTaskID() + " has requested all reduce buffers. " + 
													reduceTasks.size() + " reduce buffers.");
										}
									} catch (IOException e) {
										LOG.warn("BufferUmbilical problem in taking request " + request + ". " + e);
									}
								}
							}else{
								//LOG.info("I am here for reduce buffer request " + reduceTasktId.getTaskID() + " : " + oneReduceTaskId);
								//wrong
								if (reduceTasktId.getTaskID().equals(oneReduceTaskId)) {
									LOG.info("Map " + getTaskID() + " sending buffer request to reducer " + oneReduceTaskId);
									
									BufferExchange.BufferType type = BufferExchange.BufferType.PKVBUF;
	
									BufferRequest request = 
										new ReduceBufferRequest(host, getTaskID(), sink.getAddress(), type, oneReduceTaskId);
									
									try {
										bufferUmbilical.request(request);
										if (event.getTaskStatus() == Status.SUCCEEDED) return;
									} catch (IOException e) {
										e.printStackTrace();
										LOG.warn("BufferUmbilical problem sending request " + request + ". " + e);
									}
								}
							}					
						}
						break;
						}
					}
				}
				catch (IOException e) {
					e.printStackTrace();
				}

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) { return; }
			}
		}
	}
	
	private class RollbackCheckThread extends Thread {
		
		private long interval = 0;
		private TaskUmbilicalProtocol trackerUmbilical;
		private BufferUmbilicalProtocol srcs;
		private BufferExchangeSink buffersink;
		private InputPKVBuffer pkvBuffer;
		private Task task;
		
		
		public RollbackCheckThread(TaskUmbilicalProtocol umbilical, BufferUmbilicalProtocol srcs, BufferExchangeSink sink, Task task, InputPKVBuffer pkvBuffer) {
			interval = conf.getInt("priter.task.checkrollback.frequency", 2000);		  
			this.trackerUmbilical = umbilical;
			this.srcs = srcs;
			this.buffersink = sink;
			this.task = task;
			this.pkvBuffer = pkvBuffer;
		}
		
		public void run() {
			while(true) {
				synchronized(rollbackLock){
					try{
						rollbackLock.wait(interval);
						LOG.info("check roll back");
						
						CheckPoint checkpointEvent = trackerUmbilical.rollbackCheck(getTaskID());
						
						synchronized(task){
							checkpointIter = checkpointEvent.getIter();
								
							if(checkpointIter > 0){
								LOG.info("rolled back to checkpoint " + checkpointIter);
								pkvBuffer.free();
								srcs.rollbackForMap(this.task.getJobID());
								pkvBuffer.iteration = checkpointIter+1;		//no use
								nsortBuffer.iteration = checkpointIter+1;
								buffersink.resetCursorPosition(checkpointIter+1);
								checkpointIter = 0;
								
								task.notifyAll();	
							}
						}

					}catch(IOException ioe){
						ioe.printStackTrace();
					}catch (InterruptedException e) {
						return;
					}
				}
			}
		}
	}
	/**
	 * The size of each record in the index file for the map-outputs.
	 */
	public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;

	protected TrackedRecordReader recordReader = null;
	
	protected OutputCollector collector = null;	//the original one uses it
	protected JOutputBuffer buffer = null;		//iterative mapreduce uses it
	protected UnSortOutputBuffer nsortBuffer = null;		//no sort uses it

	private BytesWritable split = new BytesWritable();
	private String splitClass;
	private InputSplit instantiatedSplit = null;
	
    protected Class inputKeyClass;
    protected Class inputValClass;
    protected Class dataClass;
    
    protected TaskID pipeReduceTaskId = null;
    protected boolean ftsupport = false;
    
    private Thread rollbackCheckThread = null;
    private Object rollbackLock = new Object();
    
    public boolean mapsync = false;
    private JobConf job;
    
    public Activator activator = null;
    public FileBasedActivator filebasedactivator = null;
    public InputPKVBuffer pkvBuffer = null;
    public InputKVDFile kvdFile = null;

	private static final Log LOG = LogFactory.getLog(MapTask.class.getName());

	{   // set phase for this task
		setPhase(TaskStatus.Phase.MAP); 
	}

	public MapTask() {
		super();
	}

	public MapTask(String jobFile, TaskAttemptID taskId, 
			int partition, String splitClass, BytesWritable split, boolean iterative) {
		super(jobFile, taskId, partition);
		this.splitClass = splitClass;
		this.split = split;
		this.iterative = iterative;
	}

	@Override
	public boolean isMapTask() {
		return true;
	}
	
	public TaskID getIterativeReduceTask() {
		return new TaskID(this.getJobID(), false, this.pipeReduceTaskId.id);
	}
	
	public TaskID predecessorReduceTask() {
		return new TaskID(this.getJobID(), false, getTaskID().getTaskID().id);
	}
	
	@Override
	public int getNumberOfInputs() { 	
		if(job != null && job.getBoolean("priter.job.mapsync", false)){
			return job.getInt("priter.graph.partitions", 0);
		}else{
			return 1;
		}
	}
	
	@Override
	public void localizeConfiguration(JobConf conf) throws IOException {
		super.localizeConfiguration(conf);
		Path localSplit = new Path(new Path(getJobFile()).getParent(), 
				"split.dta");
		LOG.debug("Writing local split to " + localSplit);
		DataOutputStream out = FileSystem.getLocal(conf).create(localSplit);
		Text.writeString(out, splitClass);
		split.write(out);
		out.close();
	}

	@Override
	public TaskRunner createRunner(TaskTracker tracker, TaskTracker.TaskInProgress tip) {
		return new MapTaskRunner(tip, tracker, this.conf);
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		write(out);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		Text.writeString(out, splitClass);
		if (split != null) split.write(out);
		else throw new IOException("SPLIT IS NULL");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		splitClass = Text.readString(in);
		split.readFields(in);
	}

	@Override
	InputSplit getInputSplit() throws UnsupportedOperationException {
		return instantiatedSplit;
	}

	/**
	 * This class wraps the user's record reader to update the counters and progress
	 * as records are read.
	 * @param <K>
	 * @param <V>
	 */
	class TrackedRecordReader<K, V> 
	implements RecordReader<K,V> {
		private RecordReader<K,V> rawIn;
		private Counters.Counter inputByteCounter;
		private Counters.Counter inputRecordCounter;

		TrackedRecordReader(RecordReader<K,V> raw, Counters counters) {
			rawIn = raw;
			inputRecordCounter = counters.findCounter(MAP_INPUT_RECORDS);
			inputByteCounter = counters.findCounter(MAP_INPUT_BYTES);
		}

		public K createKey() {
			return rawIn.createKey();
		}

		public V createValue() {
			return rawIn.createValue();
		}

		public synchronized boolean next(K key, V value)
		throws IOException {

			setProgress(getProgress());
			long beforePos = getPos();
			boolean ret = rawIn.next(key, value);
			if (ret) {
				inputRecordCounter.increment(1);
				inputByteCounter.increment(Math.abs(getPos() - beforePos));
			}
			return ret;
		}
		public long getPos() throws IOException { return rawIn.getPos(); }
		public void close() throws IOException { rawIn.close(); }
		public float getProgress() throws IOException {
			return rawIn.getProgress();
		}
	}
	
	public void setProgress(float progress) {
		super.setProgress(progress);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void run(JobConf job, final TaskUmbilicalProtocol umbilical, final BufferUmbilicalProtocol bufferUmbilical)
	throws IOException {
		final Reporter reporter = getReporter(umbilical);
		this.job = job;
		
	    // start thread that will handle communication with parent
	    startCommunicationThread(umbilical);

		initialize(job, reporter);

	    // check if it is a cleanupJobTask
	    if (jobCleanup) {
	      runJobCleanupTask(umbilical);
	      return;
	    }
	    if (jobSetup) {
	      runJobSetupTask(umbilical);
	      return;
	    }
	    if (taskCleanup) {
	      runTaskCleanupTask(umbilical);
	      return;
	    }
	    
		int numReduceTasks = conf.getNumReduceTasks();
		LOG.info("numReduceTasks: " + numReduceTasks);

		//iterative version
		if(this.iterative){
			job.setPartitionerClass(PrIterPartitioner.class);
			//job.setBoolean("priter.job.inmem", true);
			
			setPhase(TaskStatus.Phase.PIPELINE); 
			
			if (numReduceTasks <= 0) {LOG.error("I didn't consider this");}
			
			Class mapCombiner = job.getClass("mapred.map.combiner.class", null);
			if (mapCombiner != null) {
				job.setCombinerClass(mapCombiner);
			}

			this.inputKeyClass = job.getMapOutputKeyClass();
			this.inputValClass = job.getMapOutputValueClass();
			this.dataClass = job.getStaticDataClass();
			
			Class<? extends CompressionCodec> codecClass = null;
			if (conf.getCompressMapOutput()) {
				codecClass = conf.getMapOutputCompressorClass(DefaultCodec.class);
			}

			ftsupport = job.getBoolean("priter.checkpoint", true);
			long workload = 0;
			long counter = 0;
			
			//in memory case
			if(job.getBoolean("priter.job.inmem", true)){
				this.nsortBuffer = new UnSortOutputBuffer(bufferUmbilical, this, job, 
						reporter, getProgress(), false, 
						this.inputKeyClass, this.inputValClass, codecClass);
				
				activator = (Activator) ReflectionUtils.newInstance(job.getActivatorClass(), job);
				pkvBuffer = new InputPKVBuffer(bufferUmbilical, this, job, reporter, null, inputKeyClass, inputValClass);
			
			    /* This object will be the sink's input buffer. */
				BufferExchangeSink sink = new BufferExchangeSink(job, pkvBuffer, this); 
				sink.open();
				LOG.info("buffere exchange sink opened");
				/* Start the reduce output fetcher 
				 * I should refine it later, let we choose the exact taskID in the same machine
				 * */
				while(this.pipeReduceTaskId == null){
					this.pipeReduceTaskId = new TaskID(this.getJobID(), false, this.getTaskID().getTaskID().getId());

					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				LOG.info("local reduce task id extracted " + pipeReduceTaskId);
				
				ReduceOutputFetcher rof = new ReduceOutputFetcher(umbilical, bufferUmbilical, sink, pipeReduceTaskId);
				rof.setDaemon(true);
				rof.start();
				
				LOG.info("mapper initPKVBuffer phase");
				if(this.checkpointIter <= 0){
					activator.initStarter(pkvBuffer);
				}
				
				if(ftsupport){
					//rollback check thread
					this.rollbackCheckThread = new RollbackCheckThread(umbilical, bufferUmbilical, sink, this, pkvBuffer);
					this.rollbackCheckThread.setDaemon(true);
					this.rollbackCheckThread.start();
				}
				
				try{
					synchronized(this){		
						//iteration loop, stop when reduce let it stop
						
						//for processing time measurement
						long processstart = new Date().getTime();
						long processend;
						while(true) {
							while(!pkvBuffer.next()){
								activator.iterate();
								if(counter == 0){
									LOG.info("no records left, do nothing");
								}else if(mapsync){
									LOG.info("sync barrier");
								}else{		
									this.nsortBuffer.iterate();
									counter = 0;
								}
								
								//measure process time
								processend = new Date().getTime();
								long processtime = processend - processstart;
									
								LOG.info("unsort total workload is " + workload + " use time " + processtime);
								LOG.info("no records, I am waiting!");
								
								setProgressFlag();
								try {
									this.wait();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								
								processstart = new Date().getTime();
								workload = 0;
							}
											
							Object keyObject = pkvBuffer.getTopKey();
							Object valObject = pkvBuffer.getTopValue();
							activator.activate(keyObject, valObject, this.nsortBuffer, reporter);
							reporter.incrCounter(Counter.MAP_INPUT_RECORDS, 1);
							workload++;
							counter++;										
						}
					}
				}finally {
						rof.interrupt();
						rof = null;
						if(ftsupport){
							rollbackCheckThread.interrupt();
							rollbackCheckThread = null;
						}
						sink.close();
				}
			}else{
				//on disk case
				
				//output should be sorted in the number order, doesn't help, since jobconf is
				//declared as final
				//job.setOutputKeyComparatorClass(IntWritableIncreasingComparator.class);
				
				this.buffer = new JOutputBuffer(bufferUmbilical, this, job, 
						reporter, getProgress(), false, 
						this.inputKeyClass, this.inputValClass, codecClass);
				
				filebasedactivator = (FileBasedActivator) ReflectionUtils.newInstance(job.getFileActivatorClass(), job);
				kvdFile = new InputKVDFile(this, job, reporter, buffer, inputKeyClass, inputValClass, dataClass, filebasedactivator);
			
			    /* This object will be the sink's input buffer. */
				BufferExchangeSink sink = new BufferExchangeSink(job, kvdFile, this); 
				sink.open();
				LOG.info("buffere exchange sink opened");
				/* Start the reduce output fetcher 
				 * I should refine it later, let we choose the exact taskID in the same machine
				 * */
				while(this.pipeReduceTaskId == null){
					this.pipeReduceTaskId = new TaskID(this.getJobID(), false, this.getTaskID().getTaskID().getId());

					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				LOG.info("local reduce task id extracted " + pipeReduceTaskId);
				
				ReduceOutputFetcher rof = new ReduceOutputFetcher(umbilical, bufferUmbilical, sink, pipeReduceTaskId);
				rof.setDaemon(true);
				rof.start();
				
				LOG.info("mapper initPKVBuffer phase");
				while(!kvdFile.isReady()){
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				LOG.info("priority queue file is ready!");
				//not implemented yet
				/*
				if(ftsupport){
					//rollback check thread
					this.rollbackCheckThread = new RollbackCheckThread(umbilical, bufferUmbilical, sink, this, kvdFile);
					this.rollbackCheckThread.setDaemon(true);
					this.rollbackCheckThread.start();
				}
				*/
				
				try{
					synchronized(this){		
						//iteration loop, stop when reduce let it stop
						while(true) {

							if(workload != 0){
								try {
									this.wait();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}

							long processstart = new Date().getTime();
							
							long kvds = kvdFile.process();
							workload += kvds;
							//measure process time
							long processend = new Date().getTime();
								
							LOG.info("process kvds " + kvds + " use time " + (processend - processstart));
							LOG.info("no records, I am waiting!");
							setProgressFlag();		
							
							filebasedactivator.iterate();
							buffer.iterate();
						}
					}
				}finally {
						rof.interrupt();
						rof = null;
						if(ftsupport){
							rollbackCheckThread.interrupt();
							rollbackCheckThread = null;
						}
						sink.close();
				}
			}
		}else{
			job.setBoolean("priter.job.inmem", false);
			boolean pipeline = job.getBoolean("mapred.map.pipeline", false);
			if (numReduceTasks > 0) {
				Class mapCombiner = job.getClass("mapred.map.combiner.class", null);
				if (mapCombiner != null) {
					job.setCombinerClass(mapCombiner);
				}

				Class keyClass = job.getMapOutputKeyClass();
				Class valClass = job.getMapOutputValueClass();
				Class<? extends CompressionCodec> codecClass = null;
				if (conf.getCompressMapOutput()) {
					codecClass = conf.getMapOutputCompressorClass(DefaultCodec.class);
				}
				JOutputBuffer buffer = new JOutputBuffer(bufferUmbilical, this, job, 
						reporter, getProgress(), pipeline, 
						keyClass, valClass, codecClass);
				collector = buffer;
			} else { 
				collector = new DirectMapOutputCollector(umbilical, job, reporter);
			}

			// reinstantiate the split
			try {
				instantiatedSplit = (InputSplit) 
				ReflectionUtils.newInstance(job.getClassByName(splitClass), job);
			} catch (ClassNotFoundException exp) {
				IOException wrap = new IOException("Split class " + splitClass + 
				" not found");
				wrap.initCause(exp);
				throw wrap;
			}
			DataInputBuffer splitBuffer = new DataInputBuffer();
			splitBuffer.reset(split.get(), 0, split.getSize());
			instantiatedSplit.readFields(splitBuffer);

			// if it is a file split, we can give more details
			if (instantiatedSplit instanceof FileSplit) {
				FileSplit fileSplit = (FileSplit) instantiatedSplit;
				job.set("map.input.file", fileSplit.getPath().toString());
				job.setLong("map.input.start", fileSplit.getStart());
				job.setLong("map.input.length", fileSplit.getLength());
			}


			RecordReader rawIn =                  // open input
				job.getInputFormat().getRecordReader(instantiatedSplit, job, reporter);
			this.recordReader = new TrackedRecordReader(rawIn, getCounters());

			MapRunnable runner =
				(MapRunnable)ReflectionUtils.newInstance(job.getMapRunnerClass(), job);

			try {
				runner.run(this.recordReader, collector, reporter);      
				getProgress().complete();
				LOG.info("Map task complete. Perform final close.");

				if (collector instanceof JOutputBuffer) {
					JOutputBuffer buffer = (JOutputBuffer) collector;
					OutputFile finalOut = buffer.close();
					buffer.free();
					if (finalOut != null) {
						LOG.debug("Register final output");
						bufferUmbilical.output(finalOut);
					}
				}
				else {
					((DirectMapOutputCollector)collector).close();
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw e;
			} finally {
				//close
				this.recordReader.close();                               // close input
			}
		}
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		done(umbilical);
	}
	

	class DirectMapOutputCollector<K, V>
	implements OutputCollector<K, V> {

		private RecordWriter<K, V> out = null;

		private Reporter reporter = null;

		private final Counters.Counter mapOutputRecordCounter;

		@SuppressWarnings("unchecked")
		public DirectMapOutputCollector(TaskUmbilicalProtocol umbilical,
				JobConf job, Reporter reporter) throws IOException {
			this.reporter = reporter;
			String finalName = getOutputName(getPartition());
			FileSystem fs = FileSystem.get(job);

			out = job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);

			Counters counters = getCounters();
			mapOutputRecordCounter = counters.findCounter(MAP_OUTPUT_RECORDS);
		}

		public void close() throws IOException {
			if (this.out != null) {
				out.close(this.reporter);
			}
		}

		public void collect(K key, V value) throws IOException {
			reporter.progress();
			out.write(key, value);
			mapOutputRecordCounter.increment(1);
		}

	}
}
