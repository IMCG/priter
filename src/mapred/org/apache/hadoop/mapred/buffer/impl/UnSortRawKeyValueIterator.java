package org.apache.hadoop.mapred.buffer.impl;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.util.Progress;

public class UnSortRawKeyValueIterator<K extends Object, V extends Object>
	implements RawKeyValueIterator {

	private static final Log LOG = LogFactory.getLog(UnSortRawKeyValueIterator.class.getName());
	
	private DataInputBuffer key;
    private DataInputBuffer value;
    private Queue<Segment<K, V>> segments = new LinkedList<Segment<K,V>>();
    private  Segment<K, V> currSegment;
    private int totalBytesProcessed = 0;
    private int totalBytes = 0;
    private float progPerByte;
    private Progress scanProgress = new Progress();
    
    public UnSortRawKeyValueIterator(List<Segment<K,V>> inSegments) throws IOException{
    	LOG.info("Scan " + inSegments.size() + " segments");

    	for(Segment<K,V> seg : inSegments){
    		seg.init(null);
    		segments.add(seg);
    		totalBytes += seg.getLength();
    	}
    	
    	currSegment = segments.poll();
    	
        if (totalBytes != 0) //being paranoid
            progPerByte = 1.0f / (float)totalBytes;
    }
    
	@Override
	public void close() throws IOException {
		Segment<K, V> segment;
		while((segment = segments.poll()) != null) {
			segment.close();
		}
	}

	@Override
	public DataInputBuffer getKey() throws IOException {
		return key;
	}

	@Override
	public Progress getProgress() {
		return scanProgress;
	}

	@Override
	public DataInputBuffer getValue() throws IOException {
		return value;
	}

	@Override
	public boolean next() throws IOException {
		if(currSegment == null) return false;
	    long startPos = currSegment.getPosition();
	    boolean hasNext = currSegment.next();
	    long endPos = currSegment.getPosition();

	    //LOG.info("startPos is " + startPos + " hasNext is " + hasNext + " endPos is " + endPos);

	    while (!hasNext) {
	    	 currSegment.close();
	    	 currSegment = segments.poll();
	    	 if(currSegment == null){
	    		 return false;
	    	 }

		     startPos = currSegment.getPosition();
		     hasNext = currSegment.next();
		     endPos = currSegment.getPosition();
	    }
	      
	    totalBytesProcessed += endPos - startPos;
	    scanProgress.set(totalBytesProcessed * progPerByte);
	     

	    key = currSegment.getKey();
	    value = currSegment.getValue();
	    
	    return true;
	}

}
