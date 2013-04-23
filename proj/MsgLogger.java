import java.util.LinkedList;
import java.util.PriorityQueue;

import edu.washington.cs.cse490h.lib.Node;

/* This class is used to log messages in a persistent way so we can recover them upon node failures.
 */

public class MsgLogger {
	/* Tells us if the current log is for a message just received, or about to be sent. */
	public static final int SEND = 0;
	public static final int RECV = 1;
	
	/* These are used to mark files so we know they are logs. */
	private static final String delim_send = "@";
	private static final String delim_recv = "~";
	
	/* Used to make a file system */
	private Node node;
	
	/* The file system */
	private NFSService FS;
	
	public MsgLogger(Node node){
		this.node = node;
		FS = new NFSService(node);
	}
	
	/* switches on a send/recv to give us the correct delimeter */
	private String getDelim(int sendRecv){
		return (sendRecv == SEND)?delim_send:delim_recv;
	}
	
	/* Constructs a file name from the information passed in. Output will have
	 * the form:
	 * 		<delim><addr><delim><seqNum>.log
	 * The delimeter switched on the send/recv variable.
	 */
	private String getFilename(int seqNum, int addr, int sendRecv){
		String delim = getDelim(sendRecv);
		String filename = delim;
		filename = filename.concat(Integer.toString(addr));
		filename = filename.concat(delim);
		filename = filename.concat(Integer.toString(seqNum));
		filename = filename.concat(".log");
		
		return filename;
	}
	
	/* Logs a message.  If we already have a log by this name, we return and do nothing.
	 * Otherwise we create the log and write msg to it.
	 * msg - the message
	 * addr - the node address to which the message will be sent, or has been received
	 * seqNum - same as message number
	 * sendRecv - a member of {SEND,RECV}
	 */
	public boolean logMsg(int addr, String msg, int seqNum, int sendRecv){
		String filename = getFilename(seqNum, addr, sendRecv);
		
		
		// can we make this atomic instead?  Is that possible?
		boolean alreadyLogged = FS.exists(filename);
		if(!alreadyLogged){
			FS.create(filename);
			FS.append(filename,msg);
		}
		return alreadyLogged;		
	}
	
	/* Removes a log, if it exists.
	 * addr - the node address to which the message will be sent, or has been received
	 * seqNum - same as message number
	 * sendRecv - a member of {SEND,RECV}
	 */
	public void deleteLog(int addr, int seqNum, int sendRecv){
		String filename = getFilename(seqNum, addr, sendRecv);
		FS.delete(filename);
	}
	
	/* Get a list of all logs in the directory, either send or recv, 
	 * ordered by increasing seqNum.
	 */
	public PriorityQueue<MsgLogEntry> getLogs(int sendRecv){
		PriorityQueue<MsgLogEntry> logs = new PriorityQueue<MsgLogEntry>();
		LinkedList<String> fileNames = FS.getFileList();
		char delim = getDelim(sendRecv).charAt(0);
		
		
		// iterate all files in directory prefixed with delim_recv and load them as files
		for(String s : fileNames){
			if(s.charAt(0) == delim){
				String msg = FS.read(s).get(0);
				int addr = Integer.parseInt(s.substring(1, s.indexOf(delim,1)));
				int seqNum = Integer.parseInt(s.substring(s.indexOf(delim,1)+1,s.length()-4));
				logs.add(new MsgLogEntry(msg, seqNum, addr));
			}
		}
		
		return logs;
	}
	
	/* Get a list of all logs in the directory for the given send/recv indicator and
	 * given node address.
	 */
	public PriorityQueue<MsgLogEntry> getChannelLogs(int addr, int sendRecv){
		PriorityQueue<MsgLogEntry> logs = new PriorityQueue<MsgLogEntry>();
		LinkedList<String> fileNames = FS.getFileList();
		char delim = getDelim(sendRecv).charAt(0);
		
		
		// iterate all files in directory prefixed with delim_recv and load them as files
		for(String s : fileNames){
			if(s.charAt(0) == delim){
				String msg = FS.read(s).get(0);
				int currentAddr = Integer.parseInt(s.substring(1, s.indexOf(delim,1)));
				int seqNum = Integer.parseInt(s.substring(s.indexOf(delim,1)+1,s.length()-4));
				if (currentAddr == addr) logs.add(new MsgLogEntry(msg, seqNum, addr));
			}
		}
		
		return logs;
	}	
}
