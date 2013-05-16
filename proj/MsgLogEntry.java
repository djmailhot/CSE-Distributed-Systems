
/* Store a msg sequence number, destination/source, as well as its contents */
public class MsgLogEntry implements Comparable<MsgLogEntry> {
	private final int seqNum;
	private final byte[] msg;
	private final int addr;
	
	/* Public constructor
	 * msg - message contents
	 * seqNum - same as message number
	 * addr - address to which the message is being sent, or from which it is being received
	 */
	public MsgLogEntry(byte[] msg, int seqNum, int addr){
		this.seqNum = seqNum;
		this.msg = msg;
		this.addr = addr;
	}
	
	/* Accessors for private fields
	 */
	public int seqNum(){
		return this.seqNum;
	}
	
	public byte[] msg(){
		return this.msg;
	}
	
	public int addr(){
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
