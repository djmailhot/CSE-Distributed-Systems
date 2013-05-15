import edu.washington.cs.cse490h.lib.Utility;

import java.io.File;
import java.io.IOException;
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
    boolean success = true;
    try {
      // TODO:  WHAT HAPPENS WITH VERSION NUMBERS ON A FILE CREATE?
      // TODO:  WHAT HAPPENS WITH VERSION NUMBERS ON A FILE DELETE?
      for(NFSTransaction.NFSOperation op : transaction.ops) {
        String filename = op.filename;
        String oldVersionedFile = getVersionedFilename(filename);
        String newVersionedFile;
        int version;
        Pair<Integer, Boolean> versionAndDeleted;
        switch (op.opType) {
          case CREATEFILE:
          	versionAndDeleted = fileVersions.get(filename); // Check for previous version
          	if(versionAndDeleted != null && versionAndDeleted.b) { // Existed before, is currently deleted.
          		fileVersions.put(filename, new Pair(versionAndDeleted.a + 1, false));
          	} else if (versionAndDeleted == null) { // Did not exist before
          		fileVersions.put(filename, new Pair(0, false));
          	} else { // Existed before, is not currently deleted
          		// PANIC!!!!! Matt's checkVersion should make sure this never happens.
          		System.out.println("PANICCCCCCCCC!!!!!!!!!!!!!!!");
          		throw new RuntimeException("this is not ok");
          	}
            newVersionedFile = getVersionedFilename(filename);
            // reserve the 0-version file for blank newly created files
            success = success && nfsService.create(newVersionedFile);
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
            fileVersions.put(filename, new Pair(version, false)); // If we're appending, assume it will exist.
            newVersionedFile = getVersionedFilename(filename);
            if (version > 0) { // only do this for appends to existing files
            	success = success && nfsService.copy(oldVersionedFile, newVersionedFile);
            }

            success = success && nfsService.append(newVersionedFile, op.dataline);
            break;
          case DELETEFILE:
          	versionAndDeleted = fileVersions.get(filename);
          	if(versionAndDeleted != null) { // if it does not exist, do nothing
          		fileVersions.put(filename, new Pair(versionAndDeleted.a + 1, true));  // Set deleted flag
          	}
            // success = success && nfsService.delete(oldVersionedFile);
            // WE DON'T ACTUALLY WANT TO DELETE IT
            // OTHERWISE WE ARE UNRECOVERABLE ON A CRASH RIGHT >HERE<
            break;
          case DELETELINE:
            // make sure we don't overwrite the blank 0-version file
            version = Math.max(fileVersions.get(filename).a, 0) + 1;
            fileVersions.put(filename, new Pair(version, false));
            newVersionedFile = getVersionedFilename(filename);
            success = success && nfsService.copy(oldVersionedFile, newVersionedFile);

            success = success && nfsService.deleteLine(newVersionedFile, op.dataline);
            break;
          case TOUCHFILE:
            // do nothing
            break;
          default:
            Log.w(TAG, "Received invalid operation type");
        }
      }

      writeMetafile();  // atomically commit this transaction
    } catch(IOException e) {
      Log.e(TAG, "File system failure on transaction commit");
      e.printStackTrace();
      throw new RuntimeException("File system failure on transaction commit");
    } finally {
      Log.i(TAG, String.format("Commit %d actually committed? %s",
                              transaction.tid, success));
      return success;
    }
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
    List<MCCFileData> updates = new ArrayList<MCCFileData>();

    // map of checked file versions
    Map<String, Integer> checkVersions = new HashMap<String, Integer>();
    for(MCCFileData data : filedataCheck) {
      checkVersions.put(data.filename, data.versionNum);
    }

    // check on the transaction's requested files
    for(NFSTransaction.NFSOperation op : transaction.ops) {
      String filename = op.filename;
      if(op.opType.equals(NFSTransaction.NFSOpType.CREATEFILE)) {
        // if a create file operation, make sure file doesn't already exist
        // this file will not be in the filedataCheck list
        if(fileVersions.containsKey(filename)) {
          // INVALID: trying to create a pre-existing file
          try {
            String contents = nfsService.readFile(getVersionedFilename(filename));
            Integer fileversion = fileVersions.get(filename);
            updates.add(new MCCFileData(fileversion, filename, contents));
          } catch(IOException e) {
            e.printStackTrace();
            Log.e(TAG, "failure when trying to access file for version update");
          }
        } // VALID
      } else if(checkVersions.containsKey(filename)) {
        Integer fileversion = fileVersions.get(filename);
        Integer checkversion = checkVersions.get(filename);

        // if the requested file version was invalid
        if(!fileVersions.containsKey(filename) || fileversion == null) {
          // INVALID: it was deleted on the server
          updates.add(new MCCFileData(-1, filename, null));
        } else if(fileversion != checkversion) {
          // INVALID: if it has a different version
          try {
            String contents = nfsService.readFile(getVersionedFilename(filename));
            updates.add(new MCCFileData(fileversion, filename, contents));
          } catch(IOException e) {
            e.printStackTrace();
            Log.e(TAG, "failure when trying to access file for version update");
          }
        } // VALID
      } else if (op.opType.equals(NFSTransaction.NFSOpType.APPENDLINE)){
      	// Here, we are trying to append a line to a file that does not yet exist. 
      	// This should be ok.
      	System.out.println("append to a non-existant file !!!!!!!!!!!!!!!!!");
      } else {
        // We've got a problem
      	System.out.println(op.opType);
        throw new IllegalStateException("filename not found in filedataCheck list");
      }
    }

    Set<String> currentFiles = new HashSet<String>(fileVersions.keySet());
    // check for any new files on the server not in the filedataCheck list
    currentFiles.removeAll(checkVersions.keySet());
    for(String newFile : currentFiles) {
      // INVALID: there are files on the server
      Integer fileversion = fileVersions.get(newFile);

      try {
        String contents = nfsService.readFile(getVersionedFilename(newFile));
        updates.add(new MCCFileData(fileversion, newFile, contents));
      } catch(IOException e) {
        e.printStackTrace();
        Log.e(TAG, "failure when trying to access file for version update");
      }
    }

    return updates;
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
          fileVersions.remove(fileData.filename);
        } else if(fileData.contents != null) {
          // UPDATED
          fileVersions.put(fileData.filename, new Pair(fileData.versionNum, fileData.deleted));

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
    nfsService.write(METAFILE, data.toString());
  }

	//----------------------------------------------------------------------------
	// send routines
	//----------------------------------------------------------------------------

  /**
   * DEPRICATED UNTIL FURTHER NOTICE
   * Submit a transaction to refresh the cache
   *
   * Update all files on this NFS file system to match the most recent
   * versions on the specified remote node.
   *
   * @param destAddr the address of the target remote node.
   *
   * Will recive a response through the onMCCResponse callback method.
   */
  //public void updateAllFiles(int destAddr) {
    //TODO: Rainbow Dash
  //}

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
    RPCBundle bundle = RPCBundle.newRequestBundle(filedataCheck, transaction);
    RPCSendRequest(destAddr, bundle);
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

  public void onRPCRequest(Integer from, RPCBundle bundle) {
    Log.i(TAG, String.format("From node %d, received %s", from, bundle));
    onMCCRequest(from, bundle.filelist, bundle.transaction);
  }

  public void onRPCResponse(Integer from, RPCBundle bundle) {
    Log.i(TAG, String.format("From node %d, received %s", from, bundle));
    boolean success = bundle.success;
    if(success) {
      // if successful, apply updates locally
      success = success && commitTransaction(bundle.transaction);
    } else {
      // update the cache to the most recent versions
      updateVersions(bundle.filelist);
    }
    onMCCResponse(from, bundle.tid, success);
  }

	/**
	 * Method that is called by the RPC layer when an RPC Request bundle is 
   * received.
   * Request bundles are MCC invocations on a remote node.
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
    RPCBundle responseBundle = null;
    // verify that the filedataCheck is up-to-version
    List<MCCFileData> filedataUpdate = checkVersions(filedataCheck, transaction);
    if(filedataUpdate.isEmpty()) {
      // UP-TO-VERSION!  COMMIT THAT SUCKA
      commitTransaction(transaction);
      responseBundle = RPCBundle.newResponseBundle(filedataUpdate, transaction, true);
    } else {
      // NO GOOD!  TOO LATE!  get them the new version data
      responseBundle = RPCBundle.newResponseBundle(filedataUpdate, transaction, false);
    }
    RPCSendResponse(from, responseBundle);
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



}
