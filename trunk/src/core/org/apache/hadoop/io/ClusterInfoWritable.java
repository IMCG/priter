package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ClusterInfoWritable implements WritableComparable {

	public int nodeid;
	public int clusterid;
	public int iteration;
	public String info;
	
	public ClusterInfoWritable() {}

	public ClusterInfoWritable (int nid, int cid, int itr, String cinfo) {
		this.nodeid = nid;
		this.clusterid = cid;
		this.iteration = itr;
		this.info = cinfo;
	}
	
	public void readFields(DataInput in) throws IOException {
		  nodeid = in.readInt();
		  clusterid = in.readInt();
		  iteration = in.readInt();
		  info = in.readLine();
	  }
	
	 public void write(DataOutput out) throws IOException {
		  out.writeInt(nodeid);
		  out.writeInt(clusterid);
 		  out.writeInt(iteration);
		  out.writeBytes(info);
	  }
	 
	 /** Returns true iff <code>o</code> is a VLongWritable with the same value. */
	  public boolean equals(Object o) {
	    if (!(o instanceof ClusterInfoWritable))
	      return false;
	    ClusterInfoWritable other = (ClusterInfoWritable)o;
	    if(this.nodeid == other.nodeid && this.clusterid == other.clusterid)
	    	return true;
	    else
	    	return false;
	  }
	  
	  public int hashCode() {
		    return nodeid;
		  }

	  public String toString() {
			 String str = Integer.toString(nodeid);
			 str = str + " " + Integer.toString(clusterid);
			 str = str + " " + Integer.toString(iteration);
			 str = str + " " + info;
		     return str;
	  }
	  
	  @Override
		public int compareTo(Object o) {
		  ClusterInfoWritable other = (ClusterInfoWritable)o;
			return (this.nodeid < other.nodeid ? -1 : (this.nodeid == other.nodeid ? 0 : 1));
		}

}
