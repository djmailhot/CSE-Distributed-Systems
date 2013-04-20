import java.util.LinkedList;
import java.util.PriorityQueue;

/* This class is used to log messages in a persistent way so we can recover them upon node failures.
 * 
 */

public class MsgLogger {
	
	public MsgLogger(){}
	
	public void logMsg(String msg, int seqNum){

		String filename = "~";
		filename = filename.concat(Integer.toString(seqNum));
		filename = filename.concat(".log");
		
		// can we make this atomic instead?  Is that possible?
		FS.create(filename);
		FS.write(filename,msg);
	}
	
	public PriorityQueue<MsgLogEntry> getLogs(){
		PriorityQueue<MsgLogEntry> logs = new PriorityQueue<MsgLogEntry>();
		LinkedList<String> fileNames = FS.getFileList();
		
		
		// iterate all files in directory prefixed with '~' and load them as files
		for(String s : fileNames){
			if(s.charAt(0) == '~'){
				String msg = FS.read(s);
				int seqNum = Integer.parseInt(s.substring(1, s.length()-4));
				logs.add(new MsgLogEntry(msg, seqNum));
			}
		}
		
		return logs;
	}	
}
