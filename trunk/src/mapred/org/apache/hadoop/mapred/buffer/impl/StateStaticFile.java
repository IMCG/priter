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
import org.apache.hadoop.mapred.IFile.StateStaticReader;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.Merger.Segment;

public class StateStaticFile<K extends Object, V extends Object, D extends Object> extends Segment<K, V> {
	StateStaticReader<K, V, D> statestaticreader = null;
	DataInputBuffer data = new DataInputBuffer();
	
    public StateStaticFile(Configuration conf, FileSystem fs, Path file,
                   CompressionCodec codec, boolean preserve) throws IOException {
      this(conf, fs, file, 0, fs.getFileStatus(file).getLen(), codec, preserve);
    }

    public StateStaticFile(Configuration conf, FileSystem fs, Path file,
        long segmentOffset, long segmentLength, CompressionCodec codec,
        boolean preserve) throws IOException {
    	
    	super(conf, fs, file, segmentOffset, segmentLength, codec, preserve);
    }
    
    public StateStaticFile(Reader<K, V> reader, boolean preserve) {
      super(reader, preserve);
    }

    public void init(Counters.Counter readsCounter) throws IOException {
      if (statestaticreader == null) {
        FSDataInputStream in = fs.open(file);
        in.seek(segmentOffset);
        statestaticreader = new StateStaticReader<K, V, D>(conf, (DataInputStream) in, segmentLength, codec, readsCounter);
      }
    }
    
    public DataInputBuffer getKey() { return key; }
    public DataInputBuffer getValue() { return value; }
    public DataInputBuffer getDatay() { return data; }

    public long getLength() { 
      return (statestaticreader == null) ?
        segmentLength : statestaticreader.getLength();
    }
    
    public boolean next() throws IOException {
      return statestaticreader.next(key, value, data);
    }
    
    public void close() throws IOException {
    	statestaticreader.close();
      
      if (!preserve && fs != null) {
        fs.delete(file, false);
      }
    }

    public long getPosition() throws IOException {
      return statestaticreader.getPosition();
    }
}
