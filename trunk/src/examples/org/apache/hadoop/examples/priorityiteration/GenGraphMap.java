package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Random;

import jsc.distributions.Lognormal;
import jsc.distributions.Normal;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class GenGraphMap extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {

	private int argument;
	private int capacity;
	private int subcapacity;
	private String type;
	private BufferedWriter out;			//normal static
	private BufferedWriter out2;		//iterative static
	private BufferedWriter out3;		//iterative state
	private boolean done = false;
	private int taskid;
	private JobConf conf;
	private double initial;
	
	public static final double SP_EDGE_LOGN_MU = 1.5;
	public static final double SP_EDGE_LOGN_SIGMA = 1.0;
	public static final double SP_WEIGHT_LOGN_MU = 0.4;
	public static final double SP_WEIGHT_LOGN_SIGMA = 1.2;
	public static final double PG_EDGE_LOGN_MU = -0.5;
	public static final double PG_EDGE_LOGN_SIGMA = 2;
	public static final int KM_FEATURES_SCALE = 10000;
	public static final int KM_WEIGHT_SCALE = 500;
	public static final int KM_NORMAL_M = 20;
	public static final int KM_NORMAL_D = 10;
	public static final int WEIGHT_SCALE = 100;
	
	@Override
	public void configure(JobConf job){
		argument = job.getInt(MainDriver.GEN_ARGUMENT, 0);
		capacity = job.getInt(MainDriver.GEN_CAPACITY, 0);
		initial = (double)capacity / argument;
		subcapacity = capacity / job.getNumReduceTasks();
		type = job.get(MainDriver.GEN_TYPE);
		String outdir = job.get(MainDriver.GEN_OUT);
		try {
			FileSystem fs = FileSystem.get(job);
			FSDataOutputStream os = fs.create(new Path(outdir + "/normalstatic/part" + Util.getTaskId(job)));
			FSDataOutputStream os2 = fs.create(new Path(outdir + "/iterativestatic/part" + Util.getTaskId(job)));
			FSDataOutputStream os3 = fs.create(new Path(outdir + "/iterativestate/part" + Util.getTaskId(job)));
			
			out = new BufferedWriter(new OutputStreamWriter(os));
			System.out.println(out);
			out2 = new BufferedWriter(new OutputStreamWriter(os2));
			out3 = new BufferedWriter(new OutputStreamWriter(os3));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		taskid = Util.getTaskId(job);
		conf = job;
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		
		if(!done){
			if(type.equals("sp")){
				Lognormal logn = new Lognormal(SP_EDGE_LOGN_MU, SP_EDGE_LOGN_SIGMA);
				
				int base = subcapacity * taskid;
				for(int i=0; i<subcapacity; i++){
					int index = base + i;
					
					double rand = logn.random();			
					int num_link = (int) Math.ceil(rand);
					
					while(num_link > capacity){
						rand = logn.random();
						num_link = (int)Math.ceil(rand);
					}

					if(index == argument){
						out.write(String.valueOf(index)+"\tf0:");
						out3.write(String.valueOf(index)+"\tf:0\n");
					}else{
						out.write(String.valueOf(index)+"\tp:");
						out3.write(String.valueOf(index)+"\tv:" + Integer.MAX_VALUE + "\n");
					}
					out2.write(String.valueOf(index)+"\t");
					
					Random r = new Random();
					Lognormal logn2 = new Lognormal(SP_WEIGHT_LOGN_MU, SP_WEIGHT_LOGN_SIGMA);
					
					ArrayList<Integer> links = new ArrayList<Integer>(num_link);
					for(int j=0; j< num_link; j++){
						int link = r.nextInt(capacity);
						while(links.contains(link)){
							link = r.nextInt(capacity);
						}
						links.add(link);
						double rand2 = logn2.random();
						
						int weight = 100 - (int)Math.ceil(rand2);
						if(weight <= 0) weight = 1;
						
						//System.out.println(weight);
						out.write(String.valueOf(link) + "," + String.valueOf(weight));
						out2.write(String.valueOf(link) + "," + String.valueOf(weight));
						if(j < num_link-1){
							out.write(" ");
							out2.write(" ");
						}
					}
					out.write("\n");
					out2.write("\n");
					out.flush();
					out2.flush();
					out3.flush();
				}
			}else if(type.equals("pg")){
				Lognormal logn = new Lognormal(PG_EDGE_LOGN_MU, PG_EDGE_LOGN_SIGMA);

				int base = subcapacity * taskid;
				for(int i=0; i<subcapacity; i++){
					int index = base + i;
					
					double rand = logn.random();

					int num_link = (int)Math.ceil(rand);
					
					while(num_link > capacity){
						rand = logn.random();
						num_link = (int)Math.ceil(rand);
					}
			
					if(index < argument){
						out.write(String.valueOf(index)+"\t" + initial + ":");
					}else{
						out.write(String.valueOf(index)+"\t0:");
					}
					
					out2.write(String.valueOf(index)+"\t");
					out3.write(String.valueOf(index)+"\t1\n");
					
					//System.out.println(prob);
					Random r = new Random();

					ArrayList<Integer> links = new ArrayList<Integer>(num_link);
					for(int j=0; j< num_link; j++){
						int link = r.nextInt(capacity);
						while(links.contains(link)){
							link = r.nextInt(capacity);
						}
						links.add(link);

						//System.out.println(weight);
						out.write(String.valueOf(link));
						out2.write(String.valueOf(link));
						if(j < num_link-1){
							out.write(" ");
							out2.write(" ");
						}
			
					}
					out.write("\n");		
					out2.write("\n");	
					out.flush();
					out2.flush();
					out3.flush();
				}
			}else if(type.equals("km")){
				int nummeans = 0;
				int k = argument / conf.getNumReduceTasks();
				Random r = new Random();
				Normal normal = new Normal(KM_NORMAL_M, KM_NORMAL_D);
				
				int base = subcapacity * taskid;
				int meansbase = k * taskid;
				for(int i=0; i<subcapacity; i++){
					int index = base + i;
					int num_features = (int)Math.ceil(normal.random());
					String builder = new String(); 
					for(int j=0; j<num_features; j++){
						int link = r.nextInt(KM_FEATURES_SCALE);
						int weight = r.nextInt(KM_WEIGHT_SCALE);
						builder += link + "," + weight + " ";
					}

					out.write(index + "\t" + builder + "\n");
					out2.write(index + "\t" + builder + "\n");
									
					if((nummeans < k) && (r.nextInt(100) <2)){
						int globelindex = meansbase + nummeans;
						out3.write((globelindex) + "\t" + builder + "\n");
						nummeans++;
					}
					out.flush();
					out2.flush();
					out3.flush();
				}
				
			}
			out.close();
			out2.close();
			out3.close();
			done = true;
		}
	}

}
