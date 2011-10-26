package org.apache.hadoop.examples.priorityiteration;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Random;

import jsc.distributions.Lognormal;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class GenGraphMap extends MapReduceBase implements
		Mapper<Text, Text, IntWritable, Text> {

	private int capacity;
	private int subcapacity;
	private String graphtype;
	private BufferedWriter out;		
	private boolean done = false;
	private int taskid;
	private float degree_mu;
	private float degree_sigma;
	private float weight_mu;
	private float weight_sigma;

	@Override
	public void configure(JobConf job){
		capacity = job.getInt(MainDriver.GEN_CAPACITY, 0);
		subcapacity = capacity / job.getNumMapTasks();
		graphtype = job.get(MainDriver.GEN_TYPE);
		degree_mu = job.getFloat(MainDriver.DEGREE_MU, (float)0.5);
		degree_sigma = job.getFloat(MainDriver.DEGREE_SIGMA, (float)2);
		weight_mu = job.getFloat(MainDriver.WEIGHT_MU, (float)0.5);
		weight_sigma = job.getFloat(MainDriver.WEIGHT_MU, (float)1); 
		
		String outdir = job.get(MainDriver.GEN_OUT);
		try {
			FileSystem fs = FileSystem.get(job);
			FSDataOutputStream os = fs.create(new Path(outdir + "/part" + Util.getTaskId(job)));
		
			out = new BufferedWriter(new OutputStreamWriter(os));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		taskid = Util.getTaskId(job);
	}
	
	@Override
	public void map(Text key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		
		if(!done){
			if(graphtype.equals("weighted")){
				Lognormal logn = new Lognormal(degree_mu, degree_sigma);
				
				int base = subcapacity * taskid;
				for(int i=0; i<subcapacity; i++){
					int index = base + i;
					
					double rand = logn.random();			
					int num_link = (int) Math.ceil(rand);
					
					while(num_link > capacity){
						rand = logn.random();
						num_link = (int)Math.ceil(rand);
					}

					out.write(String.valueOf(index)+"\t");
					
					Random r = new Random();
					Lognormal logn2 = new Lognormal(weight_mu, weight_sigma);
					
					ArrayList<Integer> links = new ArrayList<Integer>(num_link);
					for(int j=0; j< num_link; j++){
						int link = r.nextInt(capacity);
						while(links.contains(link)){
							link = r.nextInt(capacity);
						}
						links.add(link);
						double rand2 = logn2.random();
						float weight = (float)(1 / rand2);

						out.write(String.valueOf(link) + "," + String.valueOf(weight));
						if(j < num_link-1){
							out.write(" ");
						}
					}
					out.write("\n");
					out.flush();
					done = true;
				}
			}else if(graphtype.equals("unweighted")){
				Lognormal logn = new Lognormal(degree_mu, degree_sigma);

				int base = subcapacity * taskid;
				for(int i=0; i<subcapacity; i++){
					int index = base + i;
					
					double rand = logn.random();

					int num_link = (int)Math.ceil(rand);
					
					while(num_link > capacity){
						rand = logn.random();
						num_link = (int)Math.ceil(rand);
					}

					out.write(String.valueOf(index)+"\t");

					Random r = new Random();
					ArrayList<Integer> links = new ArrayList<Integer>(num_link);
					for(int j=0; j< num_link; j++){
						int link = r.nextInt(capacity);
						while(links.contains(link)){
							link = r.nextInt(capacity);
						}
						links.add(link);

						out.write(String.valueOf(link));
						if(j < num_link-1){
							out.write(" ");
						}
			
					}
					out.write("\n");	
					out.flush();
					done = true;
				}
			}
		}
	}
}
