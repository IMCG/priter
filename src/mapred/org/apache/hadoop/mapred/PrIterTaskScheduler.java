package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class PrIterTaskScheduler extends TaskScheduler {

	  private static final int MIN_CLUSTER_SIZE_FOR_PADDING = 3;
	  public static final Log LOG = LogFactory.getLog(PrIterTaskScheduler.class);
	  
	  protected JobQueueJobInProgressListener jobQueueJobInProgressListener;
	  private EagerTaskInitializationListener eagerTaskInitializationListener;
	  private float padFraction;
	  
	  //tasktracker <-> taskid1, taskid2 map
	  public Map<JobID, Map<String, ArrayList<Integer>>> taskTrackerMap = new HashMap<JobID, Map<String, ArrayList<Integer>>>();
	  
	  //for assign task
	  public Map<JobID, Map<Integer, Boolean>> reduceTakenMap = new HashMap<JobID, Map<Integer, Boolean>>();
	  
	  //taskid <-> tasktracker map
	  public Map<JobID, Map<Integer, String>> taskidTTMap = new HashMap<JobID, Map<Integer, String>>();
	  
	  public PrIterTaskScheduler() {
	    this.jobQueueJobInProgressListener = new JobQueueJobInProgressListener();
	    this.eagerTaskInitializationListener =
	      new EagerTaskInitializationListener();
	  }
	  
	  @Override
	  public synchronized void start() throws IOException {
	    super.start();
	    taskTrackerManager.addJobInProgressListener(jobQueueJobInProgressListener);
	    
	    eagerTaskInitializationListener.start();
	    taskTrackerManager.addJobInProgressListener(
	        eagerTaskInitializationListener);
	  }
	  
	  @Override
	  public synchronized void terminate() throws IOException {
	    if (jobQueueJobInProgressListener != null) {
	      taskTrackerManager.removeJobInProgressListener(
	          jobQueueJobInProgressListener);
	    }
	    if (eagerTaskInitializationListener != null) {
	      taskTrackerManager.removeJobInProgressListener(
	          eagerTaskInitializationListener);
	      eagerTaskInitializationListener.terminate();
	    }
	    super.terminate();
	  }
	  
	  @Override
	  public synchronized void setConf(Configuration conf) {
	    super.setConf(conf);
	    padFraction = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 
	                                 0.01f);
	  }

	  @Override
	  public synchronized List<Task> assignTasks(TaskTrackerStatus taskTracker)
	      throws IOException {

	    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
	    int numTaskTrackers = clusterStatus.getTaskTrackers();

	    Collection<JobInProgress> jobQueue =
	      jobQueueJobInProgressListener.getJobQueue();

	    //
	    // Get map + reduce counts for the current tracker.
	    //
	    int maxCurrentMapTasks = taskTracker.getMaxMapTasks();
	    int maxCurrentReduceTasks = taskTracker.getMaxReduceTasks();
	    int numMaps = taskTracker.countMapTasks();
	    int numReduces = taskTracker.countReduceTasks();

	    //
	    // Compute average map and reduce task numbers across pool
	    //
	    int remainingReduceLoad = 0;
	    int remainingMapLoad = 0;
	    synchronized (jobQueue) {
	      for (JobInProgress job : jobQueue) {
	        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
	        	if(job.getJobConf().getBoolean("priter.job", false)){
	  	          if(!taskTrackerMap.containsKey(job.getJobID())){
		        	  Map<String, ArrayList<Integer>> taskmap = new HashMap<String, ArrayList<Integer>>();
		        	  taskTrackerMap.put(job.getJobID(), taskmap);
		          }
		          
		          if(!reduceTakenMap.containsKey(job.getJobID())){
		        	  Map<Integer, Boolean> reduceTaken = new HashMap<Integer, Boolean>();
		        	  reduceTakenMap.put(job.getJobID(), reduceTaken);
		          }
		          
		          if(!taskidTTMap.containsKey(job.getJobID())){
		        	  Map<Integer, String> tidTTMap = new HashMap<Integer, String>();
		        	  taskidTTMap.put(job.getJobID(), tidTTMap);
		          }
	        	}
	          
	          int totalMapTasks = job.desiredMaps();
	          int totalReduceTasks = job.desiredReduces();
	          remainingMapLoad += (totalMapTasks - job.finishedMaps());
	          remainingReduceLoad += (totalReduceTasks - job.finishedReduces());
	        }
	      }
	    }

	    // find out the maximum number of maps or reduces that we are willing
	    // to run on any node.
	    int maxMapLoad = 0;
	    int maxReduceLoad = 0;
	    if (numTaskTrackers > 0) {
	      maxMapLoad = Math.min(maxCurrentMapTasks,
	                            (int) Math.ceil((double) remainingMapLoad / 
	                                            numTaskTrackers));
	      maxReduceLoad = Math.min(maxCurrentReduceTasks,
	                               (int) Math.ceil((double) remainingReduceLoad
	                                               / numTaskTrackers));
	    }
	        
	    int totalMaps = clusterStatus.getMapTasks();
	    int totalMapTaskCapacity = clusterStatus.getMaxMapTasks();
	    int totalReduces = clusterStatus.getReduceTasks();
	    int totalReduceTaskCapacity = clusterStatus.getMaxReduceTasks();

	    //
	    // In the below steps, we allocate first a map task (if appropriate),
	    // and then a reduce task if appropriate.  We go through all jobs
	    // in order of job arrival; jobs only get serviced if their 
	    // predecessors are serviced, too.
	    //

	    //
	    // We hand a task to the current taskTracker if the given machine 
	    // has a workload that's less than the maximum load of that kind of
	    // task.
	    //
	       
	    if (numMaps < maxMapLoad) {

	      int totalNeededMaps = 0;
	      synchronized (jobQueue) {
	        for (JobInProgress job : jobQueue) {
	          if (job.getStatus().getRunState() != JobStatus.RUNNING) {
	            continue;
	          }

	          Task t = job.obtainNewMapTask(taskTracker, numTaskTrackers,
	              taskTrackerManager.getNumberOfUniqueHosts());
	          if (t != null) {
	        	  if(job.getJobConf().getBoolean("priter.job", false)){
		        	  ArrayList<Integer> maps = null;
		        	  if(!taskTrackerMap.get(job.getJobID()).containsKey((taskTracker.trackerName))){		  
		        		  maps = new ArrayList<Integer>();
		        		  taskTrackerMap.get(job.getJobID()).put(taskTracker.trackerName, maps);
		        	  }else{
		        		  maps = taskTrackerMap.get(job.getJobID()).get(taskTracker.trackerName);
		        	  }
		        	  
		        	  int mapid = t.getTaskID().getTaskID().getId();
		        	  maps.add(mapid);
		        	  reduceTakenMap.get(job.getJobID()).put(mapid, true);
		        	  taskidTTMap.get(job.getJobID()).put(mapid, taskTracker.trackerName);
	        	  }
        	  
	            return Collections.singletonList(t);
	          }

	          //
	          // Beyond the highest-priority task, reserve a little 
	          // room for failures and speculative executions; don't 
	          // schedule tasks to the hilt.
	          //
	          totalNeededMaps += job.desiredMaps();
	          int padding = 0;
	          if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
	            padding = Math.min(maxCurrentMapTasks,
	                               (int)(totalNeededMaps * padFraction));
	          }
	          if (totalMaps + padding >= totalMapTaskCapacity) {
	            break;
	          }
	        }
	      }
	    }else{
	    	//LOG.warn("numMaps is " + numMaps + " and maxMapLoad is " + maxMapLoad);
	    }

	    //
	    // Same thing, but for reduce tasks
	    //
	    if (numReduces < maxReduceLoad) {

	      int totalNeededReduces = 0;
	      synchronized (jobQueue) {
	        for (JobInProgress job : jobQueue) {
	          if (job.getStatus().getRunState() != JobStatus.RUNNING ||
	              job.numReduceTasks == 0) {
	            continue;
	          }

	          if(job.getJobConf().getBoolean("priter.job", false)){
		          int reducetasknum = -1;
		          if(taskTrackerMap.get(job.getJobID()) == null) continue;
		          if(taskTrackerMap.get(job.getJobID()).get(taskTracker.trackerName) == null) continue;
		          for(int participate : taskTrackerMap.get(job.getJobID()).get(taskTracker.trackerName)){
		        	  if(reduceTakenMap.get(job.getJobID()).get(participate)){
		        		  reducetasknum = participate;
		        		  reduceTakenMap.get(job.getJobID()).put(participate, false);
		        		  break;
		        	  }
		          }
		          if(reducetasknum == -1){
		        	  try {
						throw new Exception("reduce didn't match to map");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		          }
		          
		          Task t = job.obtainNewReduceTask(taskTracker, numTaskTrackers, 
			              taskTrackerManager.getNumberOfUniqueHosts(), reducetasknum);
		          
		          if (t != null) {
			            return Collections.singletonList(t);
			          }
	          }else{
		          Task t = job.obtainNewReduceTask(taskTracker, numTaskTrackers, 
			              taskTrackerManager.getNumberOfUniqueHosts());
		          
		          if (t != null) {
			            return Collections.singletonList(t);
		          }
	          }

	          //
	          // Beyond the highest-priority task, reserve a little 
	          // room for failures and speculative executions; don't 
	          // schedule tasks to the hilt.
	          //
	          totalNeededReduces += job.desiredReduces();
	          int padding = 0;
	          if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
	            padding = 
	              Math.min(maxCurrentReduceTasks,
	                       (int) (totalNeededReduces * padFraction));
	          }
	          if (totalReduces + padding >= totalReduceTaskCapacity) {
	            break;
	          }
	        }
	      }
	    }else{
	    	//LOG.warn("numReduces is " + numReduces + " and maxReduceLoad is " + maxReduceLoad);
	    }
	    return null;
	  }

	  @Override
	  public synchronized Collection<JobInProgress> getJobs(String queueName) {
	    return jobQueueJobInProgressListener.getJobQueue();
	  }  
}
