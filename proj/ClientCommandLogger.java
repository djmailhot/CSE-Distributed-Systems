import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import plume.Pair;

import edu.washington.cs.cse490h.lib.Node;


public class ClientCommandLogger {
	private static final String prefix = "-clientAction-";
	
	private int seqNum;
	
	/* Used to make a file system */
	private Node node;
	
	/* The file system */
	private NFSService nfs;
	
	private HashMap<Integer, Integer> transID_seqNum;
	
	public ClientCommandLogger(Node node){
		this.node = node;
		this.nfs = new NFSService(node);
		transID_seqNum = new HashMap<Integer, Integer>();
		
		/* reload the sequence number based on the last file written */
		try{
			List<String> fileNames = nfs.getFileList();
		    this.seqNum = 0;
		    
		    // iterate all files in directory until we hit the client action logs, if they exist
		    for(String s : fileNames){
		    	if(s.startsWith(prefix)){
		    		int currentSeq = getSeqNum(s);
		    		int currentTransID = getTransID(s);
		    		
		    		this.seqNum = Math.max(this.seqNum,currentSeq);
		    		this.transID_seqNum.put(currentTransID, currentSeq);
		    	}		        
		    }
		} catch(IOException e){
			e.printStackTrace();
		    throw new RuntimeException("Error with NFS file system");
		}
	}
	
	
	/* Convenience method to build a filename from a sequence number.
	 * 	Result is <prefix><seqNum><"_"><transactionID><".log">
	 */
	private static String buildFilename(int seqNum, int transID){
		String filename = prefix;
		filename = filename.concat(Integer.toString(seqNum));
		filename = filename.concat("_");
		filename = filename.concat(Integer.toString(transID));
		filename = filename.concat(".log");
		
		return filename;
	}
	
	private static int getSeqNum(String filename){
		int endIndex = filename.indexOf("_", prefix.length());
		return Integer.parseInt(filename.substring(prefix.length(),endIndex));
	}
	
	private static int getTransID(String filename){
		int startIndex = filename.indexOf("_", prefix.length())+1;
		int endIndex = filename.indexOf(".",startIndex);
		return Integer.parseInt(filename.substring(startIndex,endIndex));
	}
	
	
	/* Writes a log file for a new command.
	 * 	commandString - the command to put in the file
	 */
	public void logCommand(String commandString, int transID){
		String filename = buildFilename(this.seqNum, transID);
		this.transID_seqNum.put(transID, seqNum);
		seqNum++;
		
		try{
			nfs.write(filename,commandString);	
		} catch(IOException e){
			e.printStackTrace();
		    throw new RuntimeException("Error with NFS file system");
		}
	}
	
	
	/* Deletes the last log written by this logger
	 */
	public void deleteLog(int transID){
	    try {
	      int seqNum = this.transID_seqNum.get(transID);
	      String fileToDelete = buildFilename(seqNum, transID);
	      nfs.delete(fileToDelete);
	    } catch(IOException e) {
	      e.printStackTrace();
	      throw new RuntimeException("Error with NFS file system");
	    }
	}
	
	
	
	/* Since files have newlines and we need to have a single string, this handles
	 * concatenating the whole file's contents into a single string.
	 */
	private String loadFile(String filename){
		String contents = "";
		try {
			
			List<String> strings = nfs.read(filename);
			if(strings != null) {
				for(String s: strings){
					contents = contents.concat(s);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return contents;
	}
	
	
	
	public List<Pair<String, Integer>> loadLogs(){
		LinkedList<Pair<String, Integer>> commands = new LinkedList<Pair<String, Integer>>();
		
		try{
			List<String> fileNames = nfs.getFileList();
		    
		    // iterate all files in directory until we hit the client action logs, if they exist
		    for(String s : fileNames){
		    	if(s.startsWith(prefix)){
		    		commands.add(new Pair<String,Integer>(loadFile(s),getTransID(s)));
		    	}
		    }
		} catch(IOException e){
			e.printStackTrace();
		    throw new RuntimeException("Error with NFS file system");
		}
		
		return commands;
	}
	
}
