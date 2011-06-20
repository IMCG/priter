package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class KeyValueTextSourceInputFormat extends FileInputFormat<Text, Text>
	implements JobConfigurable{
	  private CompressionCodecFactory compressionCodecs = null;
	  
	  public void configure(JobConf conf) {
	    compressionCodecs = new CompressionCodecFactory(conf);
	  }
	  
	  protected boolean isSplitable(FileSystem fs, Path file) {
	    return compressionCodecs.getCodec(file) == null;
	  }
	  
	  public RecordReader<Text, Text> getRecordReader(InputSplit genericSplit,
	                                                  JobConf job,
	                                                  Reporter reporter)
	    throws IOException {
	    
	    reporter.setStatus(genericSplit.toString());
	    return new KeyValueLineSourceRecordReader(job, (FileSplit) genericSplit);
	  }
}
