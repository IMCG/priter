package org.apache.hadoop.examples.priorityiteration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LinkWritable implements Writable {
	
	  public int endpoint;
	  public float weight;
	  
	  public LinkWritable() {}

	  public LinkWritable(int nid, float weight) 
	  { 
		  this.endpoint = nid;
		  this.weight = weight;
	  }

	  /** Set the value of this ClusterWritable. */
	  public void setEndPoint(int nid) { this.endpoint = nid; }
	  public void setWeight(float weight) {  this.weight = weight; }

	  /** Return the value of this ClusterWritable. */
	  public int getEndPoint() { return endpoint; }
	  public float getWeight() { return weight; }

	  public void readFields(DataInput in) throws IOException {
		  endpoint = in.readInt();
		  weight = in.readFloat();
	  }

	  public void write(DataOutput out) throws IOException {
		  out.writeInt(endpoint);
		  out.writeFloat(weight);
	  }

	  public String toString() {
	    return endpoint + ":" + weight;
	  }


}

