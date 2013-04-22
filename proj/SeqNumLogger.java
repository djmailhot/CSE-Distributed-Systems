import java.util.LinkedList;


public class SeqNumLogger {
	private static final String delim = "$";
	public static final int SEND = 2;
	public static final int RECV = 1;
	
	
	private static String buildFilename(int seq, int addr, int sendRecv){
		String filename = delim;
		if(sendRecv == SEND) filename = filename.concat(delim);
		filename = filename.concat(Integer.toString(addr));
		filename = filename.concat(delim);
		filename = filename.concat(Integer.toString(seq));
		filename = filename.concat(".log");
		
		return filename;
	}
	
	
	public static void updateSeq(int seq, int addr, int sendRecv){
		String filename = buildFilename(seq, addr, sendRecv);
		LinkedList<String> fileNames = FS.getFileList();
		
		// iterate all files in directory until we hit the seq num logs
		boolean alreadyExists = false;
		for(String s : fileNames){
			if(s.charAt(0) == delim.charAt(0) && ((s.charAt(1) == delim.charAt(0) && sendRecv == SEND) || (s.charAt(1) != delim.charAt(0) && sendRecv == RECV))){
				int currentAddr = Integer.parseInt(s.substring(sendRecv, s.indexOf("$", sendRecv)));
				if(currentAddr == addr){
					FS.rename(s,filename);
					alreadyExists = true;
					break;
				}
			}
		}
		
		if(!alreadyExists) FS.create(filename);
	}
	
	
	public static SeqLogEntries getSeqLog(){
		int seq_recv = -1;
		LinkedList<SeqLogEntries.AddrSeqPair> seq_sends = new LinkedList<SeqLogEntries.AddrSeqPair>();
		LinkedList<SeqLogEntries.AddrSeqPair> seq_recvs = new LinkedList<SeqLogEntries.AddrSeqPair>();
		LinkedList<String> fileNames = FS.getFileList();
		
		// iterate all files in directory until we hit the ones starting with delim
		for(String s : fileNames){
			if(s.charAt(0) == delim.charAt(0)){
				if(s.charAt(1) == delim.charAt(0)){
					int currentAddr = Integer.parseInt(s.substring(2, s.indexOf("$", 2)));
					int currentSeq = Integer.parseInt(s.substring(s.indexOf("$", 2)+1,s.length()-4));
					seq_sends.add(new SeqLogEntries.AddrSeqPair(currentAddr,currentSeq));
				}
				else{
					int currentAddr = Integer.parseInt(s.substring(1, s.indexOf("$", 1)));
					int currentSeq = Integer.parseInt(s.substring(s.indexOf("$", 1)+1,s.length()-4));
					seq_recvs.add(new SeqLogEntries.AddrSeqPair(currentAddr,currentSeq));
				}
			}
		}
		
		return new SeqLogEntries(seq_sends, seq_recvs);
		
	}

}
