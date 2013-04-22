import java.util.LinkedList;
import java.util.PriorityQueue;

/* This class is used to log messages in a persistent way so we can recover them upon node failures.
 * 
 */

public class MsgLogger {
	public static final int SEND = 0;
	public static final int RECV = 1;
	
	private static final String delim_send = "@";
	private static final String delim_recv = "~";
	
	private static String getDelim(int sendRecv){
		return (sendRecv == SEND)?delim_send:delim_recv;
	}
	
	
	private static String getFilename(int seqNum, int addr, int sendRecv){
		String delim = getDelim(sendRecv);
		String filename = delim;
		filename = filename.concat(Integer.toString(addr));
		filename = filename.concat(delim);
		filename = filename.concat(Integer.toString(seqNum));
		filename = filename.concat(".log");
		
		return filename;
	}
	
	
	public static boolean logMsg(int addr, String msg, int seqNum, int sendRecv){
		String filename = getFilename(seqNum, addr, sendRecv);
		
		
		// can we make this atomic instead?  Is that possible?
		boolean alreadyLogged = FS.exists(filename);
		if(!alreadyLogged){
			FS.create(filename);
			FS.write(filename,msg);
		}
		return alreadyLogged;		
	}
	
	
	public static void deleteLog(int addr, int seqNum, int sendRecv){
		String filename = getFilename(seqNum, addr, sendRecv);
		FS.delete(filename);
	}
	
	public static PriorityQueue<MsgLogEntry> getLogs(int sendRecv){
		PriorityQueue<MsgLogEntry> logs = new PriorityQueue<MsgLogEntry>();
		LinkedList<String> fileNames = FS.getFileList();
		char delim = getDelim(sendRecv).charAt(0);
		
		
		// iterate all files in directory prefixed with delim_recv and load them as files
		for(String s : fileNames){
			if(s.charAt(0) == delim){
				String msg = FS.read(s);
				int addr = Integer.parseInt(s.substring(1, s.indexOf(delim,1)));
				int seqNum = Integer.parseInt(s.substring(s.indexOf(delim,1)+1,s.length()-4));
				logs.add(new MsgLogEntry(msg, seqNum, addr));
			}
		}
		
		return logs;
	}
	
	public static PriorityQueue<MsgLogEntry> getChannelLogs(int addr, int sendRecv){
		PriorityQueue<MsgLogEntry> logs = new PriorityQueue<MsgLogEntry>();
		LinkedList<String> fileNames = FS.getFileList();
		char delim = getDelim(sendRecv).charAt(0);
		
		
		// iterate all files in directory prefixed with delim_recv and load them as files
		for(String s : fileNames){
			if(s.charAt(0) == delim){
				String msg = FS.read(s);
				int currentAddr = Integer.parseInt(s.substring(1, s.indexOf(delim,1)));
				int seqNum = Integer.parseInt(s.substring(s.indexOf(delim,1)+1,s.length()-4));
				if (currentAddr == addr) logs.add(new MsgLogEntry(msg, seqNum, addr));
			}
		}
		
		return logs;
	}	
}
