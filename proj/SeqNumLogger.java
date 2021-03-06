import java.util.LinkedList;
import java.util.List;
import java.io.IOException;

import edu.washington.cs.cse490h.lib.Node;

/* Logs the present last sequence number for the various in and out channels that are running 
 * Uses file names as the persistent storage since we only need those few bytes for what we are doing.
 * The actual files will be empty.
 */
public class SeqNumLogger {
	/* Delimits this type of log */
	private static final String delim = "#";
	
	/* Used to make a file system */
	private Node node;
	
	/* The file system */
	private NFSService nfs;
	
	/* Tells us if the current log is for a in or out channel */
	public static final int SEND = 2;
	public static final int RECV = 1;
	
	public SeqNumLogger(Node node){
		this.node = node;
		nfs = new NFSService(node);
	}
	
	/* Forms a log file name from the various parameters passed in.
	 * if sencRecv == send, return param takes the form:
	 * 		<delim><delim><addr><delim><seq>.log
	 * else it takes the form:
	 * 		<delim><addr><delim><seq>.log
	 */
	private String buildFilename(int seq, int addr, int sendRecv){
		String filename = delim;
		if(sendRecv == SEND) filename = filename.concat(delim);
		filename = filename.concat(Integer.toString(addr));
		filename = filename.concat(delim);
		filename = filename.concat(Integer.toString(seq));
		filename = filename.concat(".log");
		
		return filename;
	}
	
	/* Writes a new log file for a channel if we haven't seen it before, otherwise we update the existing one.
	 * seq - same as the last sequence number
	 * addr - the node address of the from/to of the channel
	 * sendRecv - whether this is a sending or receiving channel
	 */
	public void updateSeq(int seq, int addr, int sendRecv){
		String filename = buildFilename(seq, addr, sendRecv);
    try {
      List<String> fileNames = nfs.getFileList();
      
      // iterate all files in directory until we hit the seq num logs
      boolean alreadyExists = false;
      for(String s : fileNames){
        if(s.charAt(0) == delim.charAt(0) && ((s.charAt(1) == delim.charAt(0) && sendRecv == SEND) || (s.charAt(1) != delim.charAt(0) && sendRecv == RECV))){
          int currentAddr = Integer.parseInt(s.substring(sendRecv, s.indexOf(delim, sendRecv)));
          if(currentAddr == addr){
            nfs.rename(s,filename);
            alreadyExists = true;
            break;
          }
        }
      }
      
      if(!alreadyExists) nfs.create(filename);
    } catch(IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Error with NFS file system");
    }
	}
	
	/* Gets all saved sequence numbers for all in and out channels. */
	public SeqLogEntries getSeqLog(){
		int seq_recv = -1;
		List<SeqLogEntries.AddrSeqPair> seq_sends = new LinkedList<SeqLogEntries.AddrSeqPair>();
		List<SeqLogEntries.AddrSeqPair> seq_recvs = new LinkedList<SeqLogEntries.AddrSeqPair>();
		
	    try {
	      List<String> fileNames = nfs.getFileList();
	      
	      // iterate all files in directory until we hit the ones starting with delim
	      for(String s : fileNames){
	        if(s.charAt(0) == delim.charAt(0)){
	          if(s.charAt(1) == delim.charAt(0)){
	            int currentAddr = Integer.parseInt(s.substring(2, s.indexOf(delim, 2)));
	            int currentSeq = Integer.parseInt(s.substring(s.indexOf(delim, 2)+1,s.length()-4));
	            seq_sends.add(new SeqLogEntries.AddrSeqPair(currentAddr,currentSeq));
	          }
	          else{
	            int currentAddr = Integer.parseInt(s.substring(1, s.indexOf(delim, 1)));
	            int currentSeq = Integer.parseInt(s.substring(s.indexOf(delim, 1)+1,s.length()-4));
	            seq_recvs.add(new SeqLogEntries.AddrSeqPair(currentAddr,currentSeq));
	          }
	        }
	      }
	    } catch(IOException e) {
	      e.printStackTrace();
	      throw new RuntimeException("Error with NFS file system");
	    }
		
		return new SeqLogEntries(seq_sends, seq_recvs);
		
	}

}
