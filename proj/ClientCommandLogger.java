import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import edu.washington.cs.cse490h.lib.Node;


public class ClientCommandLogger {
	private static final String prefix = "-clientAction-";
	
	private int seqNum;
	
	/* Used to make a file system */
	private Node node;
	
	/* The file system */
	private NFSService nfs;
	
	public ClientCommandLogger(Node node){
		this.node = node;
		this.nfs = new NFSService(node);
		
		/* reload the sequence number based on the last file written */
		try{
			List<String> fileNames = nfs.getFileList();
		    this.seqNum = 0;
		    
		    // iterate all files in directory until we hit the client action logs, if they exist
		    for(String s : fileNames){
		    	if(s.startsWith(prefix)){
		    		int currentIndex = Integer.parseInt(s.substring(prefix.length(),prefix.length()+1));
		    		this.seqNum = Math.max(this.seqNum,currentIndex);
		    	}		        
		    }
		} catch(IOException e){
			e.printStackTrace();
		    throw new RuntimeException("Error with NFS file system");
		}
	}
	
	
	/* Convenience method to build a filename from a sequence number.
	 * 	Result is <prefix><seqNum><".log">
	 */
	private static String buildFilename(int seqNum){
		String filename = prefix;
		filename = filename.concat(Integer.toString(seqNum));
		filename = filename.concat(".log");
		
		return filename;
	}
	
	
	/* Writes a log file for a new command.
	 * 	commandString - the command to put in the file
	 */
	public void logCommand(String commandString){
		String filename = buildFilename(this.seqNum);
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
	public void deleteLastLog(){
	    try {
	      String fileToDelete = buildFilename(this.seqNum-1);
	      nfs.delete(fileToDelete);
	      this.seqNum--;
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
	
	
	
	public List<String> loadLogs(){
		LinkedList<String> commands = new LinkedList<String>();
		
		try{
			List<String> fileNames = nfs.getFileList();
		    this.seqNum = 0;
		    
		    // iterate all files in directory until we hit the client action logs, if they exist
		    for(String s : fileNames){
		    	if(s.startsWith(prefix)) commands.add(loadFile(s));
		    }
		} catch(IOException e){
			e.printStackTrace();
		    throw new RuntimeException("Error with NFS file system");
		}
		
		return commands;
	}
	
}
