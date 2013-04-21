import java.util.LinkedList;


public class SeqNumLogger {
	private static final String delim = "$";
	private int last_send;
	private int last_recv;
	
	public SeqNumLogger(){
		LinkedList<String> fileNames = FS.getFileList();
		
		// iterate all files in directory until we hit the one starting with delim (should be no more than one!)
		boolean alreadyExists = false;
		for(String s : fileNames){
			if(s.charAt(0) == delim.charAt(0)){
				this.last_send = Integer.parseInt(s.substring(1, s.indexOf(delim,1)));
				this.last_recv = Integer.parseInt(s.substring(s.indexOf(delim,1),s.length()-4));
				alreadyExists = true;
			}
		}
		
		if(!alreadyExists){
			this.last_send = -1;
			this.last_recv = -1;
		}
	}
	
	private static String getFilename(int seq_send, int seq_recv){
		String filename = delim;
		filename = filename.concat(Integer.toString(seq_send));
		filename = filename.concat(delim);
		filename = filename.concat(Integer.toString(seq_recv));
		filename = filename.concat(".log");
		return filename;
	}
	
	public void updateSeqSend(int seq_send){
		String filename = getFilename(seq_send, this.last_recv);
		updateLogFile(filename);
	}
	
	public void updateSeqRecv(int seq_recv){
		String filename = getFilename(this.last_send, seq_recv);
		updateLogFile(filename);
	}
	
	private void updateLogFile(String filename){
		LinkedList<String> fileNames = FS.getFileList();
		
		// iterate all files in directory until we hit the one starting with delim (should be no more than one!)
		boolean alreadyExists = false;
		for(String s : fileNames){
			if(s.charAt(0) == delim.charAt(0)){
				FS.rename(s,filename);
				alreadyExists = true;
			}
		}
		
		if(!alreadyExists) FS.create(filename);
	}
	
	// will return null if the log was never set
	public SeqLogEntry getSeqLog(){
		if(this.last_recv==-1 && this.last_send==-1) return null;
		else{
			return new SeqLogEntry(this.last_send, this.last_recv);
		}
	}

}
