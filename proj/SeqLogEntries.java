import java.util.LinkedList;

/* essentially a tuple that tells use what our last_recv index was, and last_sent, for some log entry */
public class SeqLogEntries {
	public class AddrSeqPair{
		private final int addr;
		private final int seq;
		
		public AddrSeqPair(int addr, int seq){
			this.addr = addr;
			this.seq = seq;
		}
		
		public int addr(){
			return addr;
		}
		
		public int seq(){
			return seq;
		}
	}
	
	private final LinkedList<AddrSeqPair> seq_send;
	private final LinkedList<AddrSeqPair> seq_recv;
	
	public SeqLogEntries(LinkedList<AddrSeqPair> seq_send, LinkedList<AddrSeqPair> seq_recv){
		this.seq_send = seq_send;
		this.seq_recv = seq_recv;
	}
	
	public LinkedList<AddrSeqPair> seq_send(){
		return this.seq_send;
	}
	
	public LinkedList<AddrSeqPair> seq_recv(){
		return this.seq_recv;
	}
}
