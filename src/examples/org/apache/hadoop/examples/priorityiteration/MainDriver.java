package org.apache.hadoop.examples.priorityiteration;





public class MainDriver {

	//for preprocess
	public static final String SUBGRAPH_DIR = "dir.subgraphs";
	public static final String SUBRANK_DIR = "dir.subrank";
	public static final String BASETIME_DIR = "dir.basetime";
	public static final String TIME_DIR = "dir.time";
	public static final int INDEX_BLOCK_SIZE = 100;
	public static final String VALUE_CLASS = "data.val.class";
	
	public static final String CORRECT = "preprocess.correct.data";
	public static final String TOTAL_NODE = "preprocess.total.nodes";
	public static final String START_NODE = "preprocess.start.nodes";
	
	//for kmeans
	public static final String KMEANS_INITCENTERS_DIR = "kmeans.initcenters.dir";
	public static final String KMEANS_CLUSTER_PATH = "kmeans.cluster.path";
	public static final String KMEANS_CLUSTER_K = "kmeans.cluster.k";
	public static final String KMEANS_DATA_DIR = "kmeans.data.dir";
	public static final String KMEANS_TIME_DIR = "kmeans.time.dir";
}
