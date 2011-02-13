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

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.xml.soap.Text;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.buffer.BufferUmbilicalProtocol;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapred.buffer.impl.JInputBuffer;
import org.apache.hadoop.mapred.buffer.impl.JOutputBuffer;
import org.apache.hadoop.mapred.buffer.impl.JSnapshotBuffer;
import org.apache.hadoop.mapred.buffer.impl.OutputPKVBuffer;
import org.apache.hadoop.mapred.buffer.impl.PriorityRecord;
import org.apache.hadoop.mapred.buffer.impl.ValuesIterator;
import org.apache.hadoop.mapred.buffer.net.BufferExchange;
import org.apache.hadoop.mapred.buffer.net.BufferRequest;
import org.apache.hadoop.mapred.buffer.net.BufferExchangeSink;
import org.apache.hadoop.mapred.buffer.net.MapBufferRequest;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

/** A Reduce task. */
public class ReduceTask extends Task {
	  
	private class MapOutputFetcher extends Thread {

		private TaskUmbilicalProtocol trackerUmbilical;
		
		private BufferUmbilicalProtocol bufferUmbilical;

		private BufferExchangeSink sink;
		
		private Reporter reporter;
		
		public MapOutputFetcher(TaskUmbilicalProtocol trackerUmbilical, BufferUmbilicalProtocol bufferUmbilical, Reporter reporter, BufferExchangeSink sink) {
			this.trackerUmbilical = trackerUmbilical;
			this.bufferUmbilical = bufferUmbilical;
			this.reporter = reporter;
			this.sink = sink;
		}

		public void run() {
			Set<TaskID> finishedMapTasks = new HashSet<TaskID>();
			Set<TaskAttemptID>  mapTasks = new HashSet<TaskAttemptID>();

			int eid = 0;
			while (!isInterrupted() && finishedMapTasks.size() < getNumberOfInputs()) {
				try {
					MapTaskCompletionEventsUpdate updates = 
						trackerUmbilical.getMapCompletionEvents(getJobID(), eid, Integer.MAX_VALUE, ReduceTask.this.getTaskID());

					reporter.progress();
					eid += updates.events.length;

					// Process the TaskCompletionEvents:
					// 1. Save the SUCCEEDED maps in knownOutputs to fetch the outputs.
					// 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to stop fetching
					//    from those maps.
					// 3. Remove TIPFAILED maps from neededOutputs since we don't need their
					//    outputs at all.
					for (TaskCompletionEvent event : updates.events) {
						switch (event.getTaskStatus()) {
						case FAILED:
						case KILLED:
						case OBSOLETE:
						case TIPFAILED:
						{
							TaskAttemptID mapTaskId = event.getTaskAttemptId();
							if (!mapTasks.contains(mapTaskId)) {
								mapTasks.remove(mapTaskId);
							}
						}
						break;
						case SUCCEEDED:
						{
							TaskAttemptID mapTaskId = event.getTaskAttemptId();
							finishedMapTasks.add(mapTaskId.getTaskID());
						}
						case RUNNING:
						{
							URI u = URI.create(event.getTaskTrackerHttp());
							String host = u.getHost();
							TaskAttemptID mapTaskId = event.getTaskAttemptId();
							if (!mapTasks.contains(mapTaskId)) {
								BufferExchange.BufferType type = BufferExchange.BufferType.FILE;
								if (inputSnapshots) type = BufferExchange.BufferType.SNAPSHOT;
								if (stream) type = BufferExchange.BufferType.STREAM;
								if (iterative) type = BufferExchange.BufferType.STREAM;
								
								BufferRequest request = 
									new MapBufferRequest(host, getTaskID(), sink.getAddress(), type, mapTaskId.getJobID(), getPartition());
								try {
									bufferUmbilical.request(request);
									mapTasks.add(mapTaskId);
									if (mapTasks.size() == getNumberOfInputs()) {
										LOG.info("ReduceTask " + getTaskID() + " has requested all map buffers. " + 
												  mapTasks.size() + " map buffers.");
									}
								} catch (IOException e) {
									LOG.warn("BufferUmbilical problem in taking request " + request + ". " + e);
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
					int waittime = mapTasks.size() == getNumberOfInputs() ? 60000 : 1000;
					sleep(waittime);
				} catch (InterruptedException e) { return; }
			}
		}
	}

	private class snapshotThread extends Thread {
		
		private int partitions = 0;
		private TaskUmbilicalProtocol trackerUmbilical;
		private Task reduceTask;
		
		public snapshotThread(TaskUmbilicalProtocol umbilical, Task task) {
			//topk = conf.getInt("mapred.iterative.topk", 1000);
			//get number of tasktrackers
			partitions = conf.getInt("mapred.iterative.partitions", -1);
			if(partitions == -1){
				try {
					throw new Exception("should specify the number of partitions");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
			}
			  		  
			this.trackerUmbilical = umbilical;
			this.reduceTask = task;
		}
		
		public void run() {
			
			int index = 0;		//iteration index
			
			TaskAttemptContext context = new TaskAttemptContext(conf,
	                TaskAttemptID.forName(conf.get("mapred.task.id")));
		    int id = context.getTaskAttemptID().getTaskID().getId();
		    
			while(true) {
				synchronized(this){
					try{
						this.wait(conf.getLong("mapred.iterative.snapshot.interval", 20000));
						LOG.info("pkvBuffer size " + pkvBuffer.size() + " index " + index + " total maps is " + pkvBuffer.total_map);
						
						while((pkvBuffer == null) || (pkvBuffer.size() == 0)){
							this.wait(500);
						}
						while(!pkvBuffer.start){
							this.wait(500);
						}
						
						//snapshot generation
						pkvBuffer.snapshot(index);
						
						SnapshotCompletionEvent event = new SnapshotCompletionEvent(index, id, getJobID());
						try {
							this.trackerUmbilical.snapshotCommit(event);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						index++;
						
					}catch(IOException ioe){
						ioe.printStackTrace();
					}catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	static {                                        // register a ctor
		WritableFactories.setFactory
		(ReduceTask.class,
				new WritableFactory() {
			public Writable newInstance() { return new ReduceTask(); }
		});
	}

	private static final Log LOG = LogFactory.getLog(ReduceTask.class.getName());
	
	protected JOutputBuffer outputBuffer = null;
	public OutputPKVBuffer pkvBuffer = null;
	protected int numMaps;
    protected Class inputKeyClass;
    protected Class inputValClass;
    protected Class outputKeyClass;
    protected Class outputValClass;
    protected Class priorityClass;
	protected Class<? extends CompressionCodec> codecClass = null;

	protected CompressionCodec codec;
	
	protected InputCollector inputCollector = null;
	
	private boolean reducePipeline = false;
	
	private float   snapshotThreshold = 1f;
	private float   snapshotFreq    = 1f;
	private boolean inputSnapshots = false;
	private boolean stream = false;
	private Reducer reducer = null;
	private IterativeReducer iterReducer = null;
	public boolean spillIter = false;
	
	private MapOutputFetcher fetcher = null;
	private Thread termCheckThread = null;
	private int iterindex = 0;
	
	private long lasttime;
	
	private Reporter reporter;


	{ 
		getProgress().setStatus("reduce"); 
		setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
	}

	protected Progress copyPhase = getProgress().addPhase("copy sort");
	protected Progress reducePhase = getProgress().addPhase("reduce");
	private Counters.Counter reduceInputKeyCounter = 
		getCounters().findCounter(Counter.REDUCE_INPUT_GROUPS);
	private Counters.Counter reduceInputValueCounter = 
		getCounters().findCounter(Counter.REDUCE_INPUT_RECORDS);
	private Counters.Counter reduceOutputCounter = 
		getCounters().findCounter(Counter.REDUCE_OUTPUT_RECORDS);
	private Counters.Counter reduceCombineInputCounter =
		getCounters().findCounter(Counter.COMBINE_INPUT_RECORDS);
	private Counters.Counter reduceCombineOutputCounter =
		getCounters().findCounter(Counter.COMBINE_OUTPUT_RECORDS);

	public ReduceTask() {
		super();
	}

	public ReduceTask(String jobFile, TaskAttemptID taskId,
			int partition, int numMaps, boolean iterative) {
		super(jobFile, taskId, partition);
		this.numMaps = numMaps;
		this.iterative = iterative;
	}

	@Override
	public TaskRunner createRunner(TaskTracker tracker, TaskTracker.TaskInProgress tip) throws IOException {
		return new ReduceTaskRunner(tip, tracker, this.conf);
	}

	@Override
	public boolean isMapTask() {
		return false;
	}
	
	@Override
	public boolean isPipeline() {
		if (!(jobCleanup || jobSetup || taskCleanup)) {
			return conf != null && 
				   conf.getBoolean("mapred.reduce.pipeline", false);
		}
		return false;
	}
	
	@Override
	public int getNumberOfInputs() { return numMaps; }

	/**
	 * Localize the given JobConf to be specific for this task.
	 */
	@Override
	public void localizeConfiguration(JobConf conf) throws IOException {
		super.localizeConfiguration(conf);
		conf.setNumMapTasks(numMaps);
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		write(out);
	}

	private void readObject(ObjectInputStream in) 
	throws IOException, ClassNotFoundException {
		readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(numMaps);                        // write the number of maps
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		numMaps = in.readInt();
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void run(JobConf job, final TaskUmbilicalProtocol umbilical, final BufferUmbilicalProtocol bufferUmbilical)
	throws IOException {
		// start thread that will handle communication with parent
		startCommunicationThread(umbilical);

		reporter = getReporter(umbilical); 
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
	    
	    float [] weights = {0.75f, 0.25f};
	    getProgress().setWeight(weights);
	    
	    this.inputKeyClass = job.getMapOutputKeyClass();
	    this.inputValClass = job.getMapOutputValueClass();
	    
	    this.outputKeyClass = job.getOutputKeyClass();
	    this.outputValClass = job.getOutputValueClass();
	    
	    this.priorityClass = (job.getPriorityClass() == Text.class) ? this.outputValClass : job.getPriorityClass();
	    
		if (job.getCompressMapOutput()) {
			this.codecClass = conf.getMapOutputCompressorClass(DefaultCodec.class);
		}

		reducePipeline = job.getBoolean("mapred.reduce.pipeline", false);
		snapshotFreq   = job.getFloat("mapred.snapshot.frequency", 1f);
		snapshotThreshold = snapshotFreq;
		inputSnapshots  = job.getBoolean("mapred.job.input.snapshots", false);
		iterative = job.getBoolean("mapred.job.iterative", false);
		
		InputCollector inputCollector = null;
		if (inputSnapshots) {
			LOG.info("Task " + getTaskID() + " creating input snapshot buffer.");
			inputCollector = new JSnapshotBuffer(job, this, reporter, copyPhase, 
				                                 inputKeyClass, inputValClass, codecClass);
		}
		else {
			inputCollector = new JInputBuffer(job, this, reporter, copyPhase, 
				                              inputKeyClass, inputValClass, codecClass);
		}
		
		BufferExchangeSink sink = new BufferExchangeSink(job, inputCollector, this); 
		
		this.fetcher = new MapOutputFetcher(umbilical, bufferUmbilical, reporter, sink);
		this.fetcher.setDaemon(true);
		this.fetcher.start();

		setPhase(TaskStatus.Phase.SHUFFLE); 
		stream = job.getBoolean("mapred.stream", false) ||
				 job.getBoolean("mapred.job.monitor", false);
		
		if(iterative) {
			iteration(job, inputCollector, sink, umbilical, bufferUmbilical);
		}
		else if (stream) {
			stream(job, inputCollector, sink, reporter, umbilical, bufferUmbilical);
		}
		else {
			copy(job, inputCollector, sink, reporter, umbilical, bufferUmbilical);
		}
		this.fetcher.interrupt();
		this.fetcher = null;
		
		if(iterative) {
			this.termCheckThread.interrupt();
			this.termCheckThread = null;
			reducePhase.complete();
			setProgressFlag();
			inputCollector.free();
			done(umbilical);
			return;
		}
		
		long begin = System.currentTimeMillis();
		try {
			setPhase(TaskStatus.Phase.REDUCE); 
			reduce(job, reporter, inputCollector, umbilical, bufferUmbilical, sink.getProgress(), reducePhase);
		} finally {
			reducePhase.complete();
			setProgressFlag();
			inputCollector.free();
		}	
			
		done(umbilical);
		LOG.info("Reduce task total time = " + (System.currentTimeMillis() - begin) + " ms.");
	}
	
	protected void iteration(JobConf job, InputCollector inputCollector,
			BufferExchangeSink sink, TaskUmbilicalProtocol taskUmbilical,
			BufferUmbilicalProtocol umbilical) throws IOException {
		int window = job.getInt("mapred.iterative.reduce.window", 1000);

		this.iterReducer = (IterativeReducer)ReflectionUtils.newInstance(job.getReducerClass(), job);
		if (this.pkvBuffer == null) {
			Progress progress = sink.getProgress(); 				
			this.pkvBuffer = new OutputPKVBuffer(umbilical, this, job, reporter, progress, 
										priorityClass, outputValClass, 
										this.iterReducer);
		}
		
		//termination check thread, also do generating snapshot work
		this.termCheckThread = new snapshotThread(taskUmbilical, this);
		this.termCheckThread.setDaemon(true);
		this.termCheckThread.start();
		
		synchronized (this) {	
			LOG.info("ReduceTask " + getTaskID() + ": in iterative process function.");
			sink.open();
			
			long windowTimeStamp = System.currentTimeMillis();
			lasttime = System.currentTimeMillis();
			while(true) {
				setProgressFlag();		
				LOG.info("ReduceTask: " + getTaskID() + " perform reduce. window = " + 
						 (System.currentTimeMillis() - windowTimeStamp) + "ms.");
				windowTimeStamp = System.currentTimeMillis();
				reduce(job, reporter, inputCollector, taskUmbilical, umbilical, sink.getProgress(), null);
				inputCollector.free(); // Free current data
				

				if(window == -1){
					try { this.wait();
					} catch (InterruptedException e) { }
				}else{
					try { this.wait(window);
					} catch (InterruptedException e) { }
				}
			}
		}	
	
	}
	
	protected void stream(JobConf job, InputCollector inputCollector,
			BufferExchangeSink sink, Reporter reporter, TaskUmbilicalProtocol taskUmbilical, BufferUmbilicalProtocol umbilical) throws IOException {
		int window = job.getInt("mapred.reduce.window", 1000);
		long starttime = System.currentTimeMillis();
		synchronized (this) {
			LOG.info("ReduceTask " + getTaskID() + ": in stream function.");
			sink.open();
			long windowTimeStamp = System.currentTimeMillis();
			while(!sink.complete()) {
				setProgressFlag();
				
				if (System.currentTimeMillis() > (windowTimeStamp + window)) {
					LOG.info("ReduceTask: " + getTaskID() + " perform stream window snapshot. window = " + 
							 (System.currentTimeMillis() - windowTimeStamp) + "ms.");
					windowTimeStamp = System.currentTimeMillis();
					reduce(job, reporter, inputCollector, taskUmbilical, umbilical, sink.getProgress(), null);
					inputCollector.free(); // Free current data
				}
				
				try { this.wait(window);
				} catch (InterruptedException e) { }
			}
			copyPhase.complete();
			setProgressFlag();
			LOG.info("ReduceTask " + getTaskID() + " copy phase completed in " + 
					 (System.currentTimeMillis() - starttime) + " ms.");
			sink.close();
		}
	}
	
	protected void copy(JobConf job, InputCollector inputCollector, 
			BufferExchangeSink sink, Reporter reporter, TaskUmbilicalProtocol taskUmbilical, 
			BufferUmbilicalProtocol bufferUmbilical) 
	throws IOException {
		float maxSnapshotProgress = job.getFloat("mapred.snapshot.max.progress", 0.9f);

		long starttime = System.currentTimeMillis();
		synchronized (this) {
			LOG.info("ReduceTask " + getTaskID() + ": In copy function.");
			sink.open();
			
			this.reducer = ReflectionUtils.newInstance(job.getReducerClass(), job);
			
			while(!sink.complete()) {
				copyPhase.set(sink.getProgress().get());
				setProgressFlag();
				
				if (sink.getProgress().get() > snapshotThreshold && 
						 sink.getProgress().get() < maxSnapshotProgress) {
						snapshotThreshold += snapshotFreq;
						LOG.info("ReduceTask: " + getTaskID() + " perform snapshot. progress " + (snapshotThreshold - snapshotFreq));
						reduce(job, reporter, inputCollector, taskUmbilical, bufferUmbilical, sink.getProgress(), null);
						LOG.info("ReduceTask: " + getTaskID() + " done with snapshot. progress " + (snapshotThreshold - snapshotFreq));
				}
				try { this.wait();
				} catch (InterruptedException e) { }
			}
			copyPhase.complete();
			setProgressFlag();
			LOG.info("ReduceTask " + getTaskID() + " copy phase completed in " + 
					 (System.currentTimeMillis() - starttime) + " ms.");
			sink.close();
		}
	}
	
	private int reduce(JobConf job, InputCollector input, OutputCollector output, Reporter reporter, Progress progress) throws IOException {		
		// apply reduce function
		int count = 0;
		try {
			if(iterative){
				synchronized(((OutputPKVBuffer)output).stateTable){
					ValuesIterator values = input.valuesIterator();
					while (values.more()) {	
						count++;
						iterReducer.updateState(values.getKey(), values, (OutputPKVBuffer)output, reporter);

						values.nextKey();
						
				        if (progress != null) {
				        	progress.set(values.getProgress().get());
				        }
				        if (reporter != null) reporter.progress();
				        setProgressFlag();
					}
					values.close();
				}
			}else{
				ValuesIterator values = input.valuesIterator();
				while (values.more()) {	
					count++;
					reducer.reduce(values.getKey(), values, output, reporter);
					
					values.nextKey();
					
			        if (progress != null) {
			        	progress.set(values.getProgress().get());
			        }
			        if (reporter != null) reporter.progress();
			        setProgressFlag();
				}
				values.close();
			}

			//LOG.info("Reducer called on " + count + " records.");
		} catch (Throwable t) {
			t.printStackTrace();
		}
		finally {
			//Clean up: repeated in catch block below
			if(!iterative){
				reducer.close();
			}
		}
		
		return count;
	}
	
	@SuppressWarnings("unchecked")
	private void reduce(JobConf job, final Reporter reporter, InputCollector inputCollector, 
						TaskUmbilicalProtocol taskUmbilical, BufferUmbilicalProtocol umbilical, 
			            Progress inputProgress,  Progress reduceProgress) throws IOException {
		boolean snapshot = snapshotFreq < 1f;
		
		if(iterative) {
			long processstart = new Date().getTime();
			inputCollector.flush();		
			long flushend = System.currentTimeMillis();
			LOG.info("reduce read flush use time " + (flushend-processstart));

			//LOG.info("ReduceTask: " + getTaskID() + " start iterative reduce phase.");
			int count = reduce(job, inputCollector, pkvBuffer, reporter, reduceProgress);	
			long processend = new Date().getTime();
			long processtime = processend - processstart;
			LOG.info("processed " + count + " use time " + processtime);

			if(this.spillIter){
				long sortstart = new Date().getTime();
				
				LOG.info("average processing " + pkvBuffer.actualEmit + " takes time " + (sortstart-lasttime));
				lasttime = sortstart;
				
				//retrieve the top records, and generate a file
				OutputFile outputFile = pkvBuffer.spillTops();
				if(outputFile != null){
					umbilical.output(outputFile);
					iterReducer.iterate();
					LOG.info("output file " + outputFile);
				}else{
					LOG.info("no record is reduced, so wait!");
				}
				this.spillIter = false;
				long sortend = new Date().getTime();
				long sorttime = sortend - sortstart;
				
				if(iterindex > 5){
					//after the system is stable
					int id = this.getTaskID().getTaskID().getId();
					IterationCompletionEvent event = new IterationCompletionEvent(iterindex, id, getJobID());
					try {
						taskUmbilical.afterIterCommit(event);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				LOG.info("iteration " + iterindex + "emit " + pkvBuffer.actualEmit + " reduce write/scan use time " + sorttime);
				iterindex++;
			}
			
			LOG.debug("Reduce phase complete.");			
		} else if (reducePipeline) {
			inputCollector.flush();
			if (outputBuffer == null) {
				Progress progress = snapshot ? inputProgress : reducePhase; 
				outputBuffer = new JOutputBuffer(umbilical, this, job, reporter, progress, 
												 false, outputKeyClass, outputValClass, codecClass);
			} else {
				outputBuffer.malloc();
			}
			
			LOG.debug("ReduceTask: " + getTaskID() + " start pipelined reduce phase.");
			reduce(job, inputCollector, outputBuffer, reporter, reduceProgress);
			
			OutputFile outputFile = null;
			if (snapshot) {
				outputFile = outputBuffer.snapshot();
			} else {
				outputFile = outputBuffer.close();
			}
			LOG.debug("Register output file " + outputFile);
			umbilical.output(outputFile);
			outputBuffer.free();
			LOG.debug("Reduce phase complete.");
		} else {
			// make output collector
			String filename = snapshot ? 
					getSnapshotOutputName(getPartition(), inputProgress.get()) :
				    getOutputName(getPartition());

			FileSystem fs = FileSystem.get(job);
			final RecordWriter out = job.getOutputFormat().getRecordWriter(fs, job, filename, reporter);  
			OutputCollector outputCollector = new OutputCollector() {
				@SuppressWarnings("unchecked")
				public void collect(Object key, Object value)
				throws IOException {
					out.write(key, value);
					reduceOutputCounter.increment(1);
					// indicate that progress update needs to be sent
					reporter.progress();
				}
			};
			LOG.debug("ReduceTask: create final output file " + filename);
			reduce(job, inputCollector, outputCollector, reporter, reduceProgress);
			out.close(reporter);
		}
	}
	
	
}
