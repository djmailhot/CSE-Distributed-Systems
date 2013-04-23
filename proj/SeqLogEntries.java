import java.util.LinkedList;
import java.util.List;

/* Holds the from/to addresses and last sequence numbers for a set of input and output channels */
public class SeqLogEntries {
	/* Tuple of address, sequence number */
	public static class AddrSeqPair{
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
	
	// one list for output channels and one for in channels
	private final LinkedList<AddrSeqPair> seq_send;
	private final LinkedList<AddrSeqPair> seq_recv;
	
	/* Makes a shallow copy of the linked lists passed in, then this finalizes so it can't be messed with. */
	@SuppressWarnings("unchecked")
	public SeqLogEntries(List<AddrSeqPair> seq_sends, List<AddrSeqPair> seq_recvs){
		this.seq_send = new LinkedList<AddrSeqPair>(seq_sends);
		this.seq_recv = new LinkedList<AddrSeqPair>(seq_recvs);
	}
	
	/* Accessor methods */
	public LinkedList<AddrSeqPair> seq_send(){
		return this.seq_send;
	}
	
	public LinkedList<AddrSeqPair> seq_recv(){
		return this.seq_recv;
	}
}
