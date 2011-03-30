package org.apache.hadoop.examples.priorityiteration;





public class MainDriver {

	//for preprocess
	public static final String SUBGRAPH_DIR = "dir.subgraphs";
	public static final String SUBRANK_DIR = "dir.subrank";
	public static final String BASETIME_DIR = "dir.basetime";
	public static final String TIME_DIR = "dir.time";
	public static final int INDEX_BLOCK_SIZE = 100;
	public static final String VALUE_CLASS = "data.val.class";
	
	//for algorithms
	public static final String CORRECT = "preprocess.correct.data";
	public static final String TOTAL_NODE = "preprocess.total.nodes";
	public static final String START_NODE = "preprocess.start.nodes";
	
	//for gen graph
	public static final String GEN_CAPACITY = "gengraph.capacity";
	public static final String GEN_ARGUMENT = "gengraph.argument";
	public static final String GEN_TYPE = "gengraph.type";
	public static final String GEN_OUT = "gengraph.output";
}
