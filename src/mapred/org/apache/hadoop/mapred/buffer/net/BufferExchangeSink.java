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

package org.apache.hadoop.mapred.buffer.net;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.InputCollector;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ReduceTask;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.util.Progress;


/**
 * Manages the receipt of output buffers from other tasks.
 * Any task that takes its input from other tasks will create one
 * of these objects. The address field in all BufferRequest objects
 * will contain the address associated with this object.
 * 
 * If the input consists of snapshots, then we will create a SnapshotManager
 * object.
 * @author tcondie
 *
 * @param <K> The input key type.
 * @param <V> The input value type.
 */
public class BufferExchangeSink<K extends Object, V extends Object> implements BufferExchange {
	
	private static class Position<N extends Number> {
		N position;
		
		Position(N position) {
			this.position = position;
		}
		
		void set(N position) {
			this.position = position;
		}
		
		float floatValue() {
			return this.position.floatValue();
		}
		
		long longValue() {
			return this.position.longValue();
		}
		
		int intValue() {
			return this.position.intValue();
		}
	}
	
	private static final Log LOG = LogFactory.getLog(BufferExchangeSink.class.getName());

	/* The job configuration w.r.t. the receiving task. */
	private final JobConf conf;
	
	private final Progress progress;

	/* The identifier of that receiving task. */
	private TaskAttemptID ownerid;

	/* The maximum number of incoming connections. */
	private int maxConnections;

	/* An executor for running incoming connections. */
	private Executor executor;

	/* A thread for accepting new connections. */
	private Thread acceptor;

	/* The channel used for accepting new connections. */
	private ServerSocketChannel server;

	private InputCollector<K, V> collector;

	/* All live task handlers. */
	private Set<Handler> handlers;

	/* The identifiers of the tasks that have sent us their
	 * complete output. */
	private Set<TaskID> successful;
	
	/* The total number of inputs we expect. e.g., number of maps in
	   the case that the owner is a reduce task. */
	private final int numInputs;
	private Map<TaskID, Float> inputProgress;
	private float progressSum = 0f;
	
	/* The current input position along each input task (max aggregate over attempts). */
	private Map<TaskID, Position> cursor;
	
	//for synchronization
	private Map<Long, Integer> syncMapPos;
	private int syncMaps;
	
	private long compStart;
	private long syncStart;

	/* The task that owns this sink and is receiving the input. */
	private Task task;


	public BufferExchangeSink(JobConf conf,
			           InputCollector<K, V> collector,
			           Task task)
	throws IOException {
		this.conf = conf;
		this.progress = new Progress();
		this.ownerid = task.getTaskID();
		this.collector = collector;
		this.maxConnections = conf.getInt("mapred.reduce.parallel.copies", 20);

		this.task = task;
	    this.numInputs = task.getNumberOfInputs();
	    this.inputProgress = new HashMap<TaskID, Float>();
	    
	    this.cursor = new HashMap<TaskID, Position>();
	    this.syncMapPos = new HashMap<Long, Integer>();
	    this.syncMaps = conf.getInt("priter.graph.partitions", 0);
		if(syncMaps == 0){
			JobClient jobclient = new JobClient(conf);
			ClusterStatus status = jobclient.getClusterStatus();
			syncMaps = 2 * status.getTaskTrackers();
		}
	    
		this.executor = Executors.newFixedThreadPool(Math.min(maxConnections, Math.max(numInputs, 5)));
		this.handlers = Collections.synchronizedSet(new HashSet<Handler>());
		this.successful = Collections.synchronizedSet(new HashSet<TaskID>());

		/* The server socket and selector registration */
		this.server = ServerSocketChannel.open();
		this.server.configureBlocking(true);
		this.server.socket().bind(new InetSocketAddress(0));
		
		/* recording the time */
		this.compStart = System.currentTimeMillis();
	}

	public InetSocketAddress getAddress() {
		try {
			String host = InetAddress.getLocalHost().getCanonicalHostName();
			return new InetSocketAddress(host, this.server.socket().getLocalPort());
		} catch (UnknownHostException e) {
			return new InetSocketAddress("localhost", this.server.socket().getLocalPort());
		}
	}
	
	public Progress getProgress() {
		return this.progress;
	}

	/** Open the sink for incoming connections. */
	public void open() {
		/* Create a new thread for accepting new connections. */
		this.acceptor = new Thread() {
			public void run() {
				try {
					while (server.isOpen()) {
						LOG.info("in server open");
						SocketChannel channel = server.accept();
						channel.configureBlocking(true);
						/* Note: no buffered input stream due to memory pressure. */
						DataInputStream  istream = new DataInputStream(channel.socket().getInputStream());
						DataOutputStream ostream = new DataOutputStream(new BufferedOutputStream(channel.socket().getOutputStream()));
						
						LOG.info("server is open");
						if (complete()) {
							WritableUtils.writeEnum(ostream, Connect.BUFFER_COMPLETE);
							ostream.close();
						}
						else if (handlers.size() > maxConnections) {
							LOG.info("Connections full. connections = " + handlers.size() + 
									 ", max allowed " + maxConnections);
							WritableUtils.writeEnum(ostream, Connect.CONNECTIONS_FULL);
							ostream.close();
						}
						else {
							WritableUtils.writeEnum(ostream, Connect.OPEN);
							ostream.flush();
							
							BufferExchange.BufferType type = WritableUtils.readEnum(istream, BufferExchange.BufferType.class);
							Handler handler = null;
							if (BufferType.FILE == type) {
								handler = new FileHandler(collector, istream, ostream);
							}
							else if (BufferType.SNAPSHOT == type) {
								handler = new SnapshotHandler(collector, istream, ostream);
							}
							else if (BufferType.STREAM == type) {
								handler = new StreamHandler(collector, istream, ostream);
							}
							else if (BufferType.PKVBUF == type) {
								handler =  new PKVBufferHandler(collector, istream, ostream);
							}
							else {
								LOG.error("Unknown buffer type " + type);
								channel.close();
								continue;
							}
							
							LOG.info("JBufferSink: " + ownerid + " opening connection.");
							handlers.add(handler);
							executor.execute(handler);
						}
					}
					LOG.info("JBufferSink " + ownerid + " buffer response server closed.");
				} catch (IOException e) { 
					if (!complete()) {
						e.printStackTrace();
					}
				}
			}
		};
		acceptor.setDaemon(true);
		acceptor.setPriority(Thread.MAX_PRIORITY);
		acceptor.start();
	}

	/**
	 * Close sink.
	 * @throws IOException
	 */
	public synchronized void close() throws IOException {
		LOG.info("JBufferSink is closing.");
		if (this.acceptor == null) return; // Already done.
		try {
			this.acceptor.interrupt();
			this.server.close();
			this.acceptor = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Are we done yet?
	 * @return true if all inputs have sent all their input.
	 */
	public boolean complete() {
		return this.successful.size() == numInputs;
	}

	/**
	 * Connection is done.
	 * @param connection The completed connection.
	 */
	private void done(Handler handler) {
		this.handlers.remove(handler);
	}

	private void updateProgress(OutputFile.Header header) {
		TaskID taskid = header.owner().getTaskID();
		LOG.info("Task " + taskid + ": copy from "  + header.owner() + " progress "+ header.progress());
		if (inputProgress.containsKey(taskid)) {
			progressSum -= inputProgress.get(taskid);
		} 
			
		inputProgress.put(taskid, header.progress());
		progressSum += header.progress();
		
		if (header.eof()) {
			successful.add(header.owner().getTaskID());
			LOG.info(successful.size() + " completed connections. " +
					(numInputs - successful.size()) + " remaining.");
		}
		if (complete()) {
			this.progress.complete();
			this.collector.close();
		}
		else {
			//LOG.info("Task " + taskid + " total copy progress = " + (progressSum / (float) numInputs));
			this.progress.set(progressSum / (float) numInputs);
		}
		//LOG.info("Task " + taskid + " total sink progress = " + progress.get());
	}

	public void resetCursorPosition(int checkpoint){
		for(TaskID task : cursor.keySet()){
			cursor.get(task).set(checkpoint);
		}
	}
	
	/************************************** CONNECTION CLASS **************************************/
	
	abstract class Handler<H extends OutputFile.Header> implements Runnable {
		protected InputCollector<K, V> collector;
		
		protected DataInputStream istream;
		
		protected DataOutputStream ostream;
		
		protected Handler(InputCollector<K, V> collector,
				          DataInputStream istream, DataOutputStream ostream) { 
			this.collector = collector;
			this.istream = istream;
			this.ostream = ostream;
		}
		
		public final void close() {
			try {
				LOG.debug("Close. Owner task " + task.getTaskID());
				if (this.istream != null) {
					this.istream.close();
					this.istream = null;
				}
				
				if (this.ostream != null) {
					this.ostream.close();
					this.ostream = null;
				}
			} catch (IOException e) {
				LOG.error("Close exception -- " + e);
			}
		}
		
		public final void run() {
			try {
				int open = Integer.MAX_VALUE;
				while (open == Integer.MAX_VALUE) {
					try {
						//LOG.info("Waiting for open signal.");
						open = istream.readInt();
						LOG.info("what is open? " + open);

						if (open == Integer.MAX_VALUE) {
							H header = (H) OutputFile.Header.readHeader(istream);
							//LOG.info("Handler " + this + " receive " + header.compressed() + " bytes. header: " + header);
							receive(header);
						}
					} catch (IOException e) {
						e.printStackTrace();
						LOG.error(e);
						LOG.info(e);
						return;
					}
					LOG.info("open ? " + open + " " + (open == Integer.MAX_VALUE));
				}
			} finally {
				//LOG.info("Handler done: " + this);
				done(this);
				close();
			}
			//LOG.info("run finished?");
		}
		
		protected abstract void receive(H header) throws IOException;
		
	}
	
	final class SnapshotHandler extends Handler<OutputFile.SnapshotHeader> {
		public SnapshotHandler(InputCollector<K, V> collector,
				DataInputStream istream, DataOutputStream ostream) { 
			super(collector, istream, ostream);
		}

		public void receive(OutputFile.SnapshotHeader header) throws IOException {
			Position position = null;
			TaskID inputTaskID = header.owner().getTaskID();
			synchronized (cursor) {
				if (!cursor.containsKey(inputTaskID)) {
					cursor.put(inputTaskID, new Position(0));
				}
				position = cursor.get(inputTaskID);
			}

			if (position.floatValue() < header.progress()) {				
				WritableUtils.writeEnum(ostream, Transfer.READY);
				ostream.flush();
				LOG.debug("TaskConnectionHandler " + this + " received header -- " + header);
				if (collector.read(istream, header)) {
					updateProgress(header);
					synchronized (task) {
						task.notifyAll();
					}
				}
				position.set(header.progress());			
			}
			else {
				WritableUtils.writeEnum(ostream, Transfer.IGNORE);
			}

			/* Indicate my current position. */
			ostream.writeFloat(position.floatValue());
			ostream.flush();
		}
	}
	
	final class FileHandler extends Handler<OutputFile.FileHeader> {
		public FileHandler(InputCollector<K, V> collector,
				           DataInputStream istream, DataOutputStream ostream) {
			super(collector, istream, ostream);
		}
		
		public void receive(OutputFile.FileHeader header) throws IOException {
			/* Get my position for this source taskid. */
			Position position = null;
			TaskID inputTaskID = header.owner().getTaskID();
			synchronized (cursor) {
				if (!cursor.containsKey(inputTaskID)) {
					cursor.put(inputTaskID, new Position(-1));
				}
				position = cursor.get(inputTaskID);
			}

			/* I'm the only one that should be updating this position. */
			int pos = position.intValue() < 0 ? header.ids().first() : position.intValue(); 
			synchronized (position) {
				if (header.ids().first() == pos) {
					WritableUtils.writeEnum(ostream, BufferExchange.Transfer.READY);
					ostream.flush();
					LOG.debug("File handler " + hashCode() + " ready to receive -- " + header);
					if (collector.read(istream, header)) {
						updateProgress(header);
						synchronized (task) {
							task.notifyAll();
						}
					}
					position.set(header.ids().last() + 1);
					LOG.debug("File handler " + " done receiving up to position " + position.intValue());
				}
				else {
					LOG.debug(this + " ignoring -- " + header);
					WritableUtils.writeEnum(ostream, BufferExchange.Transfer.IGNORE);
				}
			}
			/* Indicate the next spill file that I expect. */
			pos = position.intValue();
			LOG.debug("Updating source position to " + pos);
			ostream.writeInt(pos);
			ostream.flush();
		}
	}
	
	final class StreamHandler extends Handler<OutputFile.StreamHeader> {
		public StreamHandler(InputCollector<K, V> collector,
				           DataInputStream istream, DataOutputStream ostream) {
			super(collector, istream, ostream);
		}
		/*
		public void receive(OutputFile.StreamHeader header) throws IOException {
			//Get my position for this source taskid. 
			Position position = null;
			TaskID inputTaskID = header.owner().getTaskID();
			synchronized (cursor) {
				if (!cursor.containsKey(inputTaskID)) {
					cursor.put(inputTaskID, new Position(-1));
				}
				position = cursor.get(inputTaskID);
			}

			// I'm the only one that should be updating this position.
			synchronized (task) {			
				long pos = position.longValue() < 0 ? header.sequence() : position.longValue(); 
				if (pos <= header.sequence()) {
					WritableUtils.writeEnum(ostream, BufferExchange.Transfer.READY);
					ostream.flush();
					LOG.debug("Stream handler " + hashCode() + " ready to receive -- " + header);
					if (collector.read(istream, header)) {
						updateProgress(header);
						synchronized (task) {
							task.notifyAll();
						}
					}
					position.set(header.sequence() + 1);
					LOG.debug("Stream handler " + " done receiving up to position " + position.longValue());
				}
				else {
					LOG.debug(this + " ignoring -- " + header);
					WritableUtils.writeEnum(ostream, BufferExchange.Transfer.IGNORE);
				}
				// Indicate the next spill file that I expect.
				pos = position.longValue();
				LOG.debug("Updating source position to " + pos);
				ostream.writeLong(pos);
				ostream.flush();
			}
		}
		*/
		
		//synchronization
		public void receive(OutputFile.StreamHeader header) throws IOException {
			//Get my position for this source taskid. 
			Position position = null;
			TaskID inputTaskID = header.owner().getTaskID();
			synchronized (cursor) {
				if (!cursor.containsKey(inputTaskID)) {
					cursor.put(inputTaskID, new Position(-1));
				}
				position = cursor.get(inputTaskID);
			}

			synchronized (task) {			
				long pos = position.longValue() < 0 ? header.sequence() : position.longValue(); 
				LOG.info("position is: " + pos + "; sequence() is " + header.sequence());
				if (pos == header.sequence()) {
					WritableUtils.writeEnum(ostream, BufferExchange.Transfer.READY);
					ostream.flush();
					LOG.debug("Stream handler " + hashCode() + " ready to receive -- " + header);
					if (collector.read(istream, header)) {
						updateProgress(header);
									
						int recMaps = 0;
						if(!syncMapPos.containsKey(position.longValue())){
							recMaps = 1;
						}else{
							recMaps = syncMapPos.get(position.longValue()) + 1;
						}
						
						LOG.info("recMaps: " + recMaps + " syncMaps: " + syncMaps);
						syncMapPos.put(position.longValue(), recMaps);
						
						if(header.owner().getTaskID().getId() == task.getTaskID().getTaskID().getId()){
							long current = System.currentTimeMillis();
							((ReduceTask)task).pkvBuffer.timeComp = current - compStart;
							syncStart = System.currentTimeMillis();
							LOG.info("synchronization start. " + syncStart +
									"task id " + header.owner().getTaskID().getId() +
									" and " + task.getTaskID().getTaskID().getId());
						}
							
						if(recMaps == syncMaps){
							long current = System.currentTimeMillis();
							((ReduceTask)task).pkvBuffer.timeSync = current - syncStart;
							
							LOG.info("iteration " + pos + 
									" compute time " + ((ReduceTask)task).pkvBuffer.timeComp +
									" synchronization time " + (current - syncStart));
							
							syncMapPos.remove(position.longValue());	
							((ReduceTask)task).spillIter = true;	
							compStart = System.currentTimeMillis();
							task.notifyAll();								
						}else if(conf.getBoolean("priter.job.inmem", true)){
							// no need to be synchronized, the state is stored in hashmap
							task.notifyAll();
						}

						//task.notifyAll();
						position.set(header.sequence() + 1);
						LOG.debug("Stream handler " + " done receiving up to position " + position.longValue());
					}else {
						LOG.debug(this + " ignoring -- " + header);
						WritableUtils.writeEnum(ostream, BufferExchange.Transfer.IGNORE);
					}
					// Indicate the next spill file that I expect.
					pos = position.longValue();
					LOG.debug("Updating source position to " + pos);
					ostream.writeLong(pos);
					ostream.flush();
				}
			}
			
			/*
			if(conf.getBoolean("mapred.iterative.reducesync", false)){
				synchronized (task) {			
					long pos = position.longValue() < 0 ? header.sequence() : position.longValue();
					if (pos <= header.sequence()) {
						WritableUtils.writeEnum(ostream, BufferExchange.Transfer.READY);
						ostream.flush();
						LOG.debug("Stream handler " + hashCode() + " ready to receive -- " + header);
						if (collector.read(istream, header)) {
							updateProgress(header);
							
							int recMaps = (syncPos.containsKey(position.longValue())) ? syncPos.get(position.longValue()) + 1 : 1;

							LOG.info("recMaps: " + recMaps + " syncMaps: " + syncMaps);
							if(recMaps >= syncMaps){
								syncPos.remove(position.longValue());
								
								task.notifyAll();
								
							}else{
								syncPos.put(position.longValue(), recMaps);
							}
							
						}
						position.set(header.sequence() + 1);
						LOG.debug("Stream handler " + " done receiving up to position " + position.longValue());
					}
					else {
						LOG.debug(this + " ignoring -- " + header);
						WritableUtils.writeEnum(ostream, BufferExchange.Transfer.IGNORE);
					}
					// Indicate the next spill file that I expect.
					pos = position.longValue();
					LOG.debug("Updating source position to " + pos);
					ostream.writeLong(pos);
					ostream.flush();
				}
			}else{
				// I'm the only one that should be updating this position.
				synchronized (task) {			
					long pos = position.longValue() < 0 ? header.sequence() : position.longValue(); 
					if (pos <= header.sequence()) {
						WritableUtils.writeEnum(ostream, BufferExchange.Transfer.READY);
						ostream.flush();
						LOG.debug("Stream handler " + hashCode() + " ready to receive -- " + header);
						if (collector.read(istream, header)) {
							updateProgress(header);
							
							task.notifyAll();			
						}
						position.set(header.sequence() + 1);
						LOG.debug("Stream handler " + " done receiving up to position " + position.longValue());
					}
					else {
						LOG.debug(this + " ignoring -- " + header);
						WritableUtils.writeEnum(ostream, BufferExchange.Transfer.IGNORE);
					}
					// Indicate the next spill file that I expect.
					pos = position.longValue();
					LOG.debug("Updating source position to " + pos);
					ostream.writeLong(pos);
					ostream.flush();
				}
			}
			*/
		}		
	}
	
	final class PKVBufferHandler extends Handler<OutputFile.PKVBufferHeader> {
		public PKVBufferHandler(InputCollector<K, V> collector,
				           DataInputStream istream, DataOutputStream ostream) {
			super(collector, istream, ostream);
		}
		
		public void receive(OutputFile.PKVBufferHeader header) throws IOException {
			Position position = null;
			TaskID inputTaskID = header.owner().getTaskID();
			synchronized (cursor) {
				if (!cursor.containsKey(inputTaskID)) {
					cursor.put(inputTaskID, new Position(-1));
				}
				position = cursor.get(inputTaskID);
			}
			
			synchronized (task) {			
				long pos = position.longValue() < 0 ? header.iteration() : position.longValue(); 
				LOG.info("position is: " + pos + "; header.iteration() is " + header.iteration());
				if (pos == header.iteration()) {
					WritableUtils.writeEnum(ostream, BufferExchange.Transfer.READY);
					ostream.flush();
					LOG.debug("PKVBuffer handler " + hashCode() + " ready to receive -- " + header);
					
					if (collector.read(istream, header)) {
						updateProgress(header);
						/*
						if(conf.getBoolean("mapred.iterative.mapsync", false)){
							int recReduces = (syncReducePos.containsKey(position.longValue())) ? syncReducePos.get(position.longValue()) + 1 : 1;

							LOG.info("recReduces: " + recReduces + " syncReduces: " + syncReduces);
							if(recReduces >= syncReduces){
								syncReducePos.remove(position.longValue());								
								task.notifyAll();
							}else{
								syncReducePos.put(position.longValue(), recReduces);
							}
						}else{
						*/
							task.notifyAll();	
							
						//}
						position.set(header.iteration() + 1);
						LOG.debug("PKVBuffer handler " + " done receiving up to position " + position.longValue());
					}else {
						LOG.debug(this + " ignoring -- " + header);
						WritableUtils.writeEnum(ostream, BufferExchange.Transfer.IGNORE);
					}
					// Indicate the next spill file that I expect.
					pos = position.longValue();
					LOG.debug("Updating source position to " + pos);
					ostream.writeLong(pos);
					ostream.flush();
				}
			}
		}
	}
}