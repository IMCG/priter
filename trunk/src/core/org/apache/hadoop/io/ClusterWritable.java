package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ClusterWritable implements WritableComparable {
	
	  public int nodeid;
	  public int clusterid;
	  public double addvalue;
	  public double sciremove;
	  public double sciadd;

	  public ClusterWritable() {}

	  public ClusterWritable(int nid, int cid, double av, double sre, double sad) 
	  { 
		  this.nodeid = nid;
		  this.clusterid = cid;
		  this.addvalue = av;
		  this.sciremove = sre;
		  this.sciadd = sad;
	  }

	  /** Set the value of this ClusterWritable. */
	  public void setNodeid(int nid) { this.nodeid = nid; }
	  public void setClusterid(int cid) {  this.clusterid = cid; }
	  public void setAddvalue(double av) { this.addvalue = av; }
	  public void setSciremove(double sre) { this.sciremove = sre; }
	  public void setSciadd(double sad) { this.sciadd = sad; }

	  /** Return the value of this ClusterWritable. */
	  public int getNodeid() { return nodeid; }
	  public int getClusterid() { return clusterid; }
	  public double getAddvalue() { return addvalue; }
	  public double getSciremove() { return sciremove;}
	  public double getSciadd() { return sciadd;}

	  public void readFields(DataInput in) throws IOException {
		  nodeid = in.readInt();
		  clusterid = in.readInt();
		  addvalue = in.readDouble();
		  sciremove = in.readDouble();
		  sciadd = in.readDouble();
	  }

	  public void write(DataOutput out) throws IOException {
		  out.writeInt(nodeid);
		  out.writeInt(clusterid);
		  out.writeDouble(addvalue);
		  out.writeDouble(sciremove);
		  out.writeDouble(sciadd);
	  }

	  /** Returns true iff <code>o</code> is a VLongWritable with the same value. */
	  public boolean equals(Object o) {
	    if (!(o instanceof ClusterWritable))
	      return false;
	    ClusterWritable other = (ClusterWritable)o;
	    if(this.nodeid == other.nodeid && this.clusterid == other.clusterid && this.addvalue == other.addvalue)
	    	return true;
	    else
	    	return false;
	  }

	  public int hashCode() {
	    return (int)addvalue;
	  }

	  public String toString() {
		  String str = Integer.toString(nodeid);
		  str = str + " " + Integer.toString(clusterid);
		  str = str + " " + Double.toString(addvalue);
	    return str;
	  }

	@Override
	public int compareTo(Object o) {
		ClusterWritable other = (ClusterWritable)o;
		return (this.addvalue < other.addvalue ? -1 : (this.addvalue == other.addvalue ? 0 : 1));
	}

	}

