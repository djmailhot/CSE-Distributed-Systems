import edu.washington.cs.cse490h.lib.Utility;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

import plume.Pair;

/**
 * The MCCNode module gives Multiversion Concurrency Control over the 
 * Named File Storage of a particular node.
 *
 * It uses discrete Transactions to jump from one version to the next.
 */
public abstract class MCCNode extends RPCNode {
  private static final String TAG = "RPCNode";
  private static final String METAFILE = "METAFILE";
  private static final String METAFILE_DELIMITER = "\t";
  private static final String VERSION_DELIMITER = "@";

  protected final NFSService nfsService;

  private Set<Integer> committedTids;
  private Map<String, Pair<Integer, Boolean>> fileVersions;

  /**
   * Create a new Multiversioned Name File Storage Node.
   */
  public MCCNode() {
    super();
    this.nfsService = new NFSService(this);
    this.committedTids = new HashSet<Integer>();
    this.fileVersions = new HashMap<String, Pair<Integer, Boolean>>(); // filename, (version, deleted)
  }

  public void start() {
    this.committedTids.clear();
    this.fileVersions.clear();

    // read the metafile and populate the internal data structures
    try {
      if(nfsService.exists(METAFILE)) {
        List<String> lines = nfsService.read(METAFILE);
        for(String line : lines) {
          line = line.trim();
          // if the empty string
          if(line.length() == 0) {
            continue;
          }

          String[] tokens = line.split(METAFILE_DELIMITER);
          if(tokens.length == 1) {
            committedTids.add(Integer.parseInt(tokens[0]));
          } else if(tokens.length == 3) {
            fileVersions.put(tokens[1], new Pair<Integer, Boolean>(Integer.parseInt(tokens[0]), Boolean.parseBoolean(tokens[2])));
          } else {
            throw new IllegalStateException("Metafile format corrupted");
          }
        }
      }
    } catch(IOException e) {
      e.printStackTrace();
      throw new IllegalStateException("Metafile read failure");
    } catch(IndexOutOfBoundsException e) {
      throw new IllegalStateException("Metafile format corrupted");
    } catch(NumberFormatException e) {
      throw new IllegalStateException("Metafile format corrupted");
    }

    Log.i(TAG, String.format("read in METAFILE with contents %s", fileVersions));

    super.start();
  }

	//----------------------------------------------------------------------------
	// MCC filesystem commands accessing local files
	//----------------------------------------------------------------------------

  /**
   * Read the specified file.
   *
   * If the file does not exist, returns null;
   */
  public List<String> read(String filename) throws IOException {
    return nfsService.read(getVersionedFilename(filename));
  }

  /**
   * Returns a list of filenames and directory names in this node's NFS root.
   * Note, this is not recursive on directories.
   *
   * All returned filenames are rooted at this node's file system root.
   * That is, the filenames can be safely passed to other NFSService methods.
   */
  public List<String> getFileList() throws IOException {
    return nfsService.getFileList();
  }


  /**
   * @return true if the specified file exists.
   */
  public boolean exists(String filename) throws IOException {
    return nfsService.exists(getVersionedFilename(filename));
  }

	//----------------------------------------------------------------------------
	// MCC filesystem management
	//----------------------------------------------------------------------------

  /**
   * Commits the specified transaction to this node's filesystem.
   *
   * Also increments version numbers accordingly.
   *
   * @return true if successful
   */
  private boolean commitTransaction(NFSTransaction transaction) {
    Log.i(TAG, String.format("Commit attempt for %s", transaction));
    // break early if transaction has already been committed
    if(committedTids.contains(transaction.tid)) {
      Log.i(TAG, String.format("Transaction %d already committed", transaction.tid));
      return true;
    }
    boolean success = false;
    boolean runningSuccess = true;
    try {
      for(NFSTransaction.NFSOperation op : transaction.ops) {
        String filename = op.filename;
        String oldVersionedFile = getVersionedFilename(filename);
        System.out.println("oldVersionedFile: " + oldVersionedFile);
        String newVersionedFile;
        int version;
        Pair<Integer, Boolean> versionAndDeleted;
        switch (op.opType) {
          case CREATEFILE:
          	versionAndDeleted = fileVersions.get(filename); // Check for previous version
          	if(versionAndDeleted != null && versionAndDeleted.b) { // Existed before, is currently deleted.
          		fileVersions.put(filename, new Pair<Integer, Boolean>(versionAndDeleted.a + 1, false));
          	} else if (versionAndDeleted == null) { // Did not exist before
          		fileVersions.put(filename, new Pair<Integer, Boolean>(0, false));
          	} else { // Existed before, is not currently deleted
          		// PANIC!!!!! Matt's checkVersion should make sure this never happens.
          		System.out.println("PANICCCCCCCCC!!!!!!!!!!!!!!!");
          		throw new RuntimeException("this is not ok");
          	}
            newVersionedFile = getVersionedFilename(filename);
            // reserve the 0-version file for blank newly created files
            runningSuccess = runningSuccess && nfsService.create(newVersionedFile);
            break;
          case APPENDLINE:
          	versionAndDeleted = fileVersions.get(filename);
          	if (versionAndDeleted == null) {
          		// file does not yet exists
          		version = 0;
          	} else {
              // make sure we don't overwrite the blank 0-version file
          		version = Math.max(versionAndDeleted.a, 0) + 1;
            }
            System.out.println("Version: " + version);
            fileVersions.put(filename, new Pair<Integer,Boolean>(version, false)); // If we're appending, assume it will exist.
            newVersionedFile = getVersionedFilename(filename);
            System.out.println("oldVersionedFile: " + oldVersionedFile);
            System.out.println("newVersionedFile: " + newVersionedFile);
            System.out.println("Op: " + op);
            if (version > 0) { // only do this for appends to existing files
              runningSuccess = runningSuccess && nfsService.copy(oldVersionedFile, newVersionedFile);
            }

            runningSuccess = runningSuccess && nfsService.append(newVersionedFile, op.dataline);
            break;
          case DELETEFILE:
            versionAndDeleted = fileVersions.get(filename);
            if(versionAndDeleted != null) { // if it does not exist, do nothing
              fileVersions.put(filename, new Pair<Integer, Boolean>(versionAndDeleted.a + 1, true));  // Set deleted flag
            }
            // success = success && nfsService.delete(oldVersionedFile);
            // WE DON'T ACTUALLY WANT TO DELETE IT
            // OTHERWISE WE ARE UNRECOVERABLE ON A CRASH RIGHT >HERE<
            break;
          case DELETELINE:
            // make sure we don't overwrite the blank 0-version file
            version = Math.max(fileVersions.get(filename).a, 0) + 1;
            fileVersions.put(filename, new Pair<Integer, Boolean>(version, false));
            newVersionedFile = getVersionedFilename(filename);
            runningSuccess = runningSuccess && nfsService.copy(oldVersionedFile, newVersionedFile);

            runningSuccess = runningSuccess && nfsService.deleteLine(newVersionedFile, op.dataline);
            break;
          case TOUCHFILE:
            // do nothing
            break;
          default:
            Log.w(TAG, "Received invalid operation type");
        }

        //something went wrong.
        if(runningSuccess == false){
          break;
        }
      }

      if(runningSuccess==false){
        System.out.println("Commit failed; check what happened.");
      }
      else{
        writeMetafile();  // atomically commit this transaction
      }

      success = runningSuccess;
    } catch(IOException e) {
      Log.e(TAG, "File system failure on transaction commit");
      e.printStackTrace();
      throw new RuntimeException("File system failure on cache update");
    } 

    Log.i(TAG, String.format("Commit %d actually committed? %s",
          transaction.tid, success));
    return success;

  }

  /**
   * @return a filename that has been versionafied.
   */
  private String getVersionedFilename(String filename) {
    if(!fileVersions.containsKey(filename)) {
      return String.format("%d%s%s", -1, VERSION_DELIMITER, filename);
    }
    int version = fileVersions.get(filename).a; 
    return String.format("%d%s%s", version, VERSION_DELIMITER, filename);
  }


  /**
   * Computes any what versions are valid and invalid for the specified filedata
   * check and transaction.
   *
   * @return a list of MCCFileData objects of file contents for any checked 
   * filedata that had an invalid version, or empty if the checked filedata has 
   * all valid versions.
   */
  private List<MCCFileData> checkVersions(List<MCCFileData> filedataCheck,
                                          NFSTransaction transaction) {
    Log.i(TAG, String.format("Check versions for transaction %d", transaction.tid));
    Log.i(TAG, String.format("Versions submitted? %s", filedataCheck));
    Log.i(TAG, String.format("Current versions? %s", fileVersions));
    Set<MCCFileData> updates = new HashSet<MCCFileData>();
    
    // map of checked file versions
    Map<String, Pair<Integer,Boolean>> checkVersions = new HashMap<String, Pair<Integer,Boolean>>();
    for(MCCFileData data : filedataCheck) {
      checkVersions.put(data.filename, new Pair<Integer,Boolean>(data.versionNum,(data.versionNum==-1)?true:false));
    }
    
    
    /* Here we are doing two checks effectively.  First, we are making sure that the client's cache
     * is synchronized with the server.  Second, we are checking to make sure that the transactions
     * listed are internally consistent in the order they are listed.  For instance, even if a file
     * currently exists on the server and the client is up-to-date, we cannot allow a delete 
     * followed by delete line, since that logically does not make sense.
     * 
     * Effectively this loop simulates the commit process to make sure it conforms to logic, and
     * checks the versions in the client's cache.
     */    
    Map<String,Pair<Integer,Boolean>> tempActual = new HashMap<String,Pair<Integer,Boolean>>();
    for(String s: fileVersions.keySet()){
    	tempActual.put(s, new Pair<Integer,Boolean>(fileVersions.get(s).a,fileVersions.get(s).b));
    }
    
    Map<String,Pair<Integer,Boolean>> tempCheck = new HashMap<String,Pair<Integer,Boolean>>();
    for(String s: checkVersions.keySet()){
    	tempCheck.put(s, new Pair<Integer,Boolean>(checkVersions.get(s).a,checkVersions.get(s).b));
    }
    
    for(NFSTransaction.NFSOperation op : transaction.ops){
    	Pair<Integer,Boolean> currentActual = tempActual.get(op.filename);
    	Pair<Integer,Boolean> currentCheck = tempCheck.get(op.filename);
    	
    	
    	//check that versions are up-to-date and we can proceed
    	boolean reject = false;
    	if(op.opType == NFSTransaction.NFSOpType.DELETELINE){
    		if(currentActual == null || currentCheck==null || currentActual.b || currentCheck.b || currentActual.a != currentCheck.a){
    			reject = true;
    		}
    	}
    	else if(op.opType == NFSTransaction.NFSOpType.CREATEFILE){
    		if((currentActual!=null && !currentActual.b) || !((currentActual==null && currentCheck==null) || (currentCheck != null && currentCheck.b && currentActual == null) || (currentActual != null && currentActual.b && currentCheck == null) || (currentActual!=null && currentCheck!=null && currentActual.a == currentCheck.a && currentActual.b == currentActual.b))){
    			reject = true;
    		}
    	}
    	else{
    		if(!((currentActual==null && currentCheck==null) || (currentCheck != null && currentCheck.b && currentActual == null) || (currentActual != null && currentActual.b && currentCheck == null) || (currentActual!=null && currentCheck!=null && currentActual.a == currentCheck.a && currentActual.b == currentActual.b))){
    			reject = true;
    		}
    	}
    	
    	//if the operation will fail, time to do an update
    	if(reject){
    		try {
    			if(currentActual==null || currentActual.b){
    				updates.add(new MCCFileData(-1, op.filename, "", true));
    			}
    			else{
	                String contents = nfsService.readFile(getVersionedFilename(op.filename));
	                updates.add(new MCCFileData(currentActual.a, op.filename, contents, false));
    			}
              } catch(IOException e) {
                e.printStackTrace();
                Log.e(TAG, "failure when trying to access file for version update");
              }
    	}
    	//else simulate effect of the command
    	else {
    		//if not present
    		if(currentActual == null){
    			if(op.opType != NFSTransaction.NFSOpType.DELETEFILE && op.opType != NFSTransaction.NFSOpType.TOUCHFILE){
    				tempActual.put(op.filename, new Pair<Integer,Boolean>(0,false));
    				tempCheck.put(op.filename, new Pair<Integer,Boolean>(0,false));
    			}    				
    		}
    		//if present and deleted
    		else if (currentActual.b){
    			if(op.opType == NFSTransaction.NFSOpType.DELETEFILE){
    				tempActual.get(op.filename).b = true;
    				tempCheck.get(op.filename).b = true;
    			}
    			else if(op.opType != NFSTransaction.NFSOpType.TOUCHFILE){
	    			tempActual.get(op.filename).a += 1;
	    			tempActual.get(op.filename).b = false;
	    			
	    			tempCheck.get(op.filename).a += 1;
	    			tempCheck.get(op.filename).b = false;
    			}
    		}
    		//is present and not deleted
    		else{
    			if(op.opType == NFSTransaction.NFSOpType.DELETEFILE){
    				tempActual.get(op.filename).b = true;
    				tempCheck.get(op.filename).b = true;
    			}
    			else if(op.opType == NFSTransaction.NFSOpType.TOUCHFILE){
    				//do nothing
    			}
    			else{
    				tempActual.get(op.filename).a += 1;
    				tempCheck.get(op.filename).a += 1;
    			}
    		}
    	}
    }
 
    
    // check for any new files on the server not in the filedataCheck list
    Set<String> currentFiles = new HashSet<String>(fileVersions.keySet());
    currentFiles.removeAll(checkVersions.keySet());
    for(String newFile : currentFiles) {
      // INVALID: there are files on the server
      Pair<Integer,Boolean> actual = fileVersions.get(newFile);

      if(!actual.b){
	      try {
	        String contents = nfsService.readFile(getVersionedFilename(newFile));
	        updates.add(new MCCFileData(actual.a, newFile, contents, actual.b));
	      } catch(IOException e) {
	        e.printStackTrace();
	        Log.e(TAG, "failure when trying to access file for version update");
	      }
      }
    }

    return new ArrayList<MCCFileData>(updates);
  }

  /**
   * Updates the local file system to match the specified file updates.
   *
   * @requires the specified list of MCCFileData entries to contain valid
   * file contents.
   */
  private void updateVersions(List<MCCFileData> filedataUpdate) {
    Log.i(TAG, String.format("Updating versions to %s", filedataUpdate));
    try {
      for(MCCFileData fileData : filedataUpdate) {
        if(fileData.versionNum == -1) {
          // DELETED
          fileVersions.put(fileData.filename, new Pair<Integer, Boolean>(fileData.versionNum, true));
        } else if(fileData.contents != null) {
          // UPDATED
          fileVersions.put(fileData.filename, new Pair<Integer, Boolean>(fileData.versionNum, fileData.deleted));

          // file version should be good now
          String updatedFilename = getVersionedFilename(fileData.filename);
          nfsService.write(updatedFilename, fileData.contents);
        } else {
          // BADNESS
          throw new IllegalStateException("filedata updates contained null file contents");
        }
      }

      // commit the filedata update
      writeMetafile();
    } catch(IOException e) {
      Log.e(TAG, "File system failure on cache update");
      e.printStackTrace();
      throw new RuntimeException("File system failure on cache update");
    }
  }

  /**
   * Writes a new version of this node's Metafile.
   *
   * Writes the current state of the internal MCC data structures to disk.
   *
   * This is considered an atomic finalizing step to committing a transaction.
   */
  private void writeMetafile() throws IOException {
    Log.i(TAG, "METAFILE commit attempt");
    StringBuilder data = new StringBuilder();
    for(Integer tid : committedTids) {
      data.append(String.format("%d\n", tid));
    }
    for(String filename : fileVersions.keySet()) {
      Pair<Integer, Boolean> versionAndDeleted = fileVersions.get(filename);
      int version = versionAndDeleted.a;
      boolean deleted = versionAndDeleted.b;
      data.append(String.format("%d%s%s%s%s\n", version, METAFILE_DELIMITER, filename, METAFILE_DELIMITER, deleted));
    }
    System.out.println("Meta before");
    nfsService.write(METAFILE, data.toString());
    System.out.println("Meta after");
  }

	//----------------------------------------------------------------------------
	// send routines
	//----------------------------------------------------------------------------

  /**
   * Submit the specified transaction for committing on the specified remote 
   * node.
   *
   * @param destAddr the address of the target remote node.
   * @param transaction the filesystem transaction to commit.
   *
   * Will recive a response through the onMCCResponse callback method.
   */
  public void submitTransaction(int destAddr, NFSTransaction transaction) {
    Log.i(TAG, String.format("Commit submission to %d for transaction %s",
                            destAddr, transaction));
    List<MCCFileData> filedataCheck = getCurrentVersions();
    MCCMsg msg = new MCCMsg(filedataCheck, transaction);
    RPCSendCommitRequest(destAddr, msg);
  }

  private List<MCCFileData> getCurrentVersions() {
    List<MCCFileData> filedata = new ArrayList<MCCFileData>();
    for(String filename : fileVersions.keySet()) {
    	Pair<Integer, Boolean> versionAndDeleted = fileVersions.get(filename);
      filedata.add(new MCCFileData(versionAndDeleted.a, filename, null, versionAndDeleted.b));
    }
    return filedata;
  }

	//----------------------------------------------------------------------------
	// receive routines
	//----------------------------------------------------------------------------

  @Override
  public void onRPCCommitRequest(Integer from, RPCMsg message) {
    MCCMsg msg = (MCCMsg)message;
    Log.i(TAG, String.format("From node %d, received request %s", from, msg));
    List<MCCFileData> list = new ArrayList<MCCFileData>();
    for (MCCFileData file : msg.filearray) {
    	list.add(file);
    }
    onMCCRequest(from, list, msg.transaction);
  }

  @Override
  public void onRPCCommitResponse(Integer from, RPCMsg message) {
    MCCMsg msg = (MCCMsg)message;
    Log.i(TAG, String.format("From node %d, received response %s", from, msg));
    boolean success = msg.success;
    if(success) {
      // if successful, apply updates locally
	    	System.out.println("****************IN ON RPC RESPONSE**************");
	    	System.out.println(addr);
	    	System.out.println(msg.transaction);
	      success = success && commitTransaction(msg.transaction);
    } else {
      // update the cache to the most recent versions
      List<MCCFileData> list = new ArrayList<MCCFileData>();
      for (MCCFileData file : msg.filearray) {
      	list.add(file);
      }
      updateVersions(list);
    }
    onMCCResponse(from, msg.transaction.tid, success);
  }

	/**
	 * Method that is called when an MCC Request message is received.
   * Request messages are MCC invocations on a remote node.
	 * 
	 * @param from
	 *            The address from which the message was received
   * @param filedataCheck
   *            A list of files and version numbers.
   *            The file contents are not used.
   * @param transaction
   *            The filesystem transaction to send.
	 */
  public void onMCCRequest(Integer from, List<MCCFileData> filedataCheck, 
                           NFSTransaction transaction) {
    MCCMsg responseMsg = null;
    if(committedTids.contains(transaction.tid)) {
      // DUPLICATE REQUEST, ALREADY COMMITTED ON THE SERVER
      responseMsg = new MCCMsg(new ArrayList<MCCFileData>(), transaction, true);

    } else {
      // verify that the filedataCheck is up-to-version
      List<MCCFileData> filedataUpdate = checkVersions(filedataCheck, transaction);

      if(filedataUpdate.isEmpty()) {
        // UP-TO-VERSION!  COMMIT THAT SUCKA
        System.out.println("****************IN ON MCC REQUEST**************");
        System.out.println(addr);
        System.out.println(transaction);
        commitTransaction(transaction);
        responseMsg = new MCCMsg(filedataUpdate, transaction, true);

      } else {
        // NO GOOD!  TOO LATE!  get them the new version data
        responseMsg = new MCCMsg(filedataUpdate, transaction, false);
      }
    }

    RPCSendCommitResponse(from, responseMsg);
  }

	/**
	 * Method that is called by the MCC layer when a response is received
   * after a updateAllFiles or a commitTransaction request.
   * Responses are replies to Requests.
	 * 
	 * @param from
	 *            The address from which the message was received
   * @param tid
   *            The transaction id this response is for
	 * @param success
	 *            True if the specified transaction was successful
   *
	 */
  public abstract void onMCCResponse(Integer from, int tid, boolean success);



  /**
   * MCC message to send over RPC
   */
  protected static class MCCMsg implements RPCMsg {
    public static final long serialVersionUID = 0L;

    public final NFSTransaction transaction;
    public final MCCFileData[] filearray;
    public final boolean success;

    /**
     * Wrapper around a file version list and a filesystem transaction.
     *
     * @param success
     *            Whether the message represents a successful request
     * @param filelist
     *            A list of files and version numbers with contents.
     * @param transaction
     *            The filesystem transaction to include.
     */
    MCCMsg(List<MCCFileData> filelist, 
              NFSTransaction transaction, boolean success) {
      this.filearray = filelist.toArray(new MCCFileData[filelist.size()]);
      this.transaction = transaction;
      this.success = success;
    }

    /**
     * Wrapper around a file version list and a filesystem transaction.
     *
     * @param filelist
     *            A list of files and version numbers with contents.
     * @param transaction
     *            The filesystem transaction to include.
     */
    public MCCMsg(List<MCCFileData> filelist, NFSTransaction transaction) {
      this(filelist, transaction, false);
    }

    public int getId() {
      return transaction.tid;
    }

    public String toString() {
      return String.format("MCCMsg{%d, success? %s}", transaction.tid, success);
    }
  }

  protected static class MCCFileData implements Serializable {
    public static final long serialVersionUID = 0L;

    public final int versionNum;
    public final String filename;
    public final String contents;
    public final boolean deleted;

    public MCCFileData(int versionNum, String filename, String contents, boolean deleted) {
      this.versionNum = versionNum;
      this.filename = filename;
      this.contents = contents;
      this.deleted = deleted;
    }

    public String toString() {
      return String.format("MCCFileData{%s, %d, %s}", filename, versionNum, deleted);
    }
  }
}
