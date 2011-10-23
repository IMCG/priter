package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class DistributorMaps {
	
	class IntMap extends MapReduceBase implements 
			Mapper<Text, Text, IntWritable, Text> {
		@Override
		public void map(Text key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter report)
				throws IOException {
			output.collect(new IntWritable(Integer.parseInt(key.toString())), value);
		}
	}
	
	class FloatMap extends MapReduceBase implements 
			Mapper<Text, Text, FloatWritable, Text> {
		@Override
		public void map(Text key, Text value,
			OutputCollector<FloatWritable, Text> output, Reporter report)
			throws IOException {
			output.collect(new FloatWritable(Float.parseFloat(key.toString())), value);
		}
	}
	
	class DoubleMap extends MapReduceBase implements 
			Mapper<Text, Text, DoubleWritable, Text> {
		@Override
		public void map(Text key, Text value,
			OutputCollector<DoubleWritable, Text> output, Reporter report)
			throws IOException {
			output.collect(new DoubleWritable(Double.parseDouble(key.toString())), value);
		}
	}
	
	class TextMap extends MapReduceBase implements 
			Mapper<Text, Text, Text, Text> {
		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter report)
				throws IOException {
			output.collect(key, value);
		}
	}
}
