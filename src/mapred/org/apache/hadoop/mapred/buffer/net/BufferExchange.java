package org.apache.hadoop.mapred.buffer.net;

public interface BufferExchange {
	public static enum Connect{OPEN, CONNECTIONS_FULL, BUFFER_COMPLETE, ERROR, CLOSED};
	
	public static enum Transfer{READY, IGNORE, SUCCESS, RETRY, TERMINATE, CLOSED};
	
	public static enum BufferType{FILE, SNAPSHOT, STREAM, PKVBUF};
}
