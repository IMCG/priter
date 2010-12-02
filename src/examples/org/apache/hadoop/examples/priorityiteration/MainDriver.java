package org.apache.hadoop.examples.priorityiteration;




import org.apache.hadoop.util.ProgramDriver;




public class MainDriver {

	//for preprocess
	public static final String SUBGRAPH_DIR = "dir.subgraphs";
	public static final String SUBRANK_DIR = "dir.subrank";
	public static final String BASETIME_DIR = "dir.basetime";
	public static final String TIME_DIR = "dir.time";
	public static final int INDEX_BLOCK_SIZE = 100;
	public static final String VALUE_CLASS = "data.val.class";
	
	//topk
	public static final String TOP_K = "top.k";
	
	//for shortestpath
	public static final String SP_START_NODE = "shortestpath.start.node";
	public static final String SP_TOTAL_NODES = "shortestpath.total.nodes";
	
	//for pagerank
	public static final String PG_TOTAL_PAGES = "pagerank.total.pages";
	public static final String PG_TERM_THRESHOLD = "pagerank.termination.threshold";
	
	//for kmeans
	public static final String KMEANS_INITCENTERS_DIR = "kmeans.initcenters.dir";
	public static final String KMEANS_CLUSTER_PATH = "kmeans.cluster.path";
	public static final String KMEANS_CLUSTER_K = "kmeans.cluster.k";
	public static final String KMEANS_DATA_DIR = "kmeans.data.dir";
	public static final String KMEANS_TIME_DIR = "kmeans.time.dir";
	
	public static final String IN_MEM = "store.in.memory";
	
	public static final int MACHINE_NUM = 3;

}
