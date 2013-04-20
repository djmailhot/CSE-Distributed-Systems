import java.util.PriorityQueue;

/* This class is used to log messages in a persistent way so we can recover them upon node failures.
 * 
 */

public class MsgLogger {
	
	public MsgLogger(){}
	
	public void logMsg(String msg, int seqNum){
		//TODO: make new file with contents msg
		//  can we make that atomic?
	}
	
	public PriorityQueue<MsgLogEntry> getLogs(){
		PriorityQueue<MsgLogEntry> logs = new PriorityQueue<MsgLogEntry>();
		
		//TODO: iterate all files in directory prefixed with '~' and load them as files
		
		//for f in list_of_filenames_in_dir:
		//	if f[0] == '~':
		//		msg = read in contents of file named 'f'
		//  	seqNum = Integer.parseInt(stuff between '~' and '.temp' in f)
		//  	logs.add(new MsgLogEntry(msg, seqNum);
		
		return logs;
	}	
}
