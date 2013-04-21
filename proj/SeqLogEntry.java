/* essentially a tuple that tells use what our last_recv index was, and last_sent, for some log entry */
public class SeqLogEntry {
	private final int seq_send;
	private final int seq_recv;
	
	public SeqLogEntry(int seq_send, int seq_recv){
		this.seq_send = seq_send;
		this.seq_recv = seq_recv;
	}
	
	public int seq_send(){
		return this.seq_send;
	}
	
	public int seq_recv(){
		return this.seq_recv;
	}
}
