package org.apache.hadoop.examples.priorityiteration;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TransposeGraphMap extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, OutputCollector<Text, Text> output,
			Reporter report) throws IOException {
		String links = value.toString();
		StringTokenizer st = new StringTokenizer(links);
		while(st.hasMoreTokens()){
			output.collect(new Text(st.nextToken()), key);
		}
	}

}
