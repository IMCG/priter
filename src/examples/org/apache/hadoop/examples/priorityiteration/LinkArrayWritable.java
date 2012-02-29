/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.examples.priorityiteration;

import org.apache.hadoop.io.ArrayWritable;

/**
 *
 * @author yzhang
 */
public class LinkArrayWritable extends ArrayWritable{
 	
    public LinkArrayWritable() {
        super(LinkWritable.class);
    }
	
    public LinkArrayWritable(LinkWritable[] values) {
        super(LinkWritable.class, values);
    }
}
