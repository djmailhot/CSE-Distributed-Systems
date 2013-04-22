
/* Store a msg sequence number as well as its contents */

public class MsgLogEntry implements Comparable<MsgLogEntry> {
	private final int seqNum;
	private final String msg;
	private final int addr;
	
	public MsgLogEntry(String msg, int seqNum, int addr){
		this.seqNum = seqNum;
		this.msg = msg;
		this.addr = addr;
	}
	
	public int seqNum(){
		return this.seqNum;
	}
	
	public String msg(){
		return this.msg;
	}
	
	public int from(){
		return this.addr;
	}

	/* Gives a total ordering of logs by sequence number.
	 * Precondition: mle != NULL
	 */
	@Override
	public int compareTo(MsgLogEntry mle) {
		return this.seqNum - mle.seqNum;
	}
}
