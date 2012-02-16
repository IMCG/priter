package org.apache.hadoop.mapred.buffer.impl;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.IFile.PriorityReader;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.Merger.Segment;

public class InterPriorityFile<K extends Object, V extends Object, P extends Object> extends Segment<K, V> {
	PriorityReader<P, K, V> priorityreader = null;
	DataInputBuffer priority = new DataInputBuffer();
	
    public InterPriorityFile(Configuration conf, FileSystem fs, Path file,
                   CompressionCodec codec, boolean preserve) throws IOException {
      this(conf, fs, file, 0, fs.getFileStatus(file).getLen(), codec, preserve);
    }

    public InterPriorityFile(Configuration conf, FileSystem fs, Path file,
        long segmentOffset, long segmentLength, CompressionCodec codec,
        boolean preserve) throws IOException {
    	
    	super(conf, fs, file, segmentOffset, segmentLength, codec, preserve);
    }
    
    public InterPriorityFile(Reader<K, V> reader, boolean preserve) {
      super(reader, preserve);
    }

    public void init(Counters.Counter readsCounter) throws IOException {
      if (priorityreader == null) {
        FSDataInputStream in = fs.open(file);
        in.seek(segmentOffset);
        priorityreader = new PriorityReader<P, K, V>(conf, (DataInputStream) in, segmentLength, codec, readsCounter);
      }
    }
    
    public DataInputBuffer getKey() { return key; }
    public DataInputBuffer getValue() { return value; }
    public DataInputBuffer getPriority() { return priority; }

    public long getLength() { 
      return (priorityreader == null) ?
        segmentLength : priorityreader.getLength();
    }
    
    public boolean next() throws IOException {
      return priorityreader.next(priority, key, value);
    }
    
    public void close() throws IOException {
    	priorityreader.close();
      
      if (!preserve && fs != null) {
        fs.delete(file, false);
      }
    }

    public long getPosition() throws IOException {
      return priorityreader.getPosition();
    }
}
