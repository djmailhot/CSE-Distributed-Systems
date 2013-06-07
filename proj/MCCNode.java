import edu.washington.cs.cse490h.lib.ServerList;
import edu.washington.cs.cse490h.lib.Utility;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;


import plume.Pair;

/**
 * The MCCNode module gives Multiversion Concurrency Control over the 
 * Named File Storage of a particular node.
 *
 * It uses discrete Transactions to jump from one version to the next.
 */
public abstract class MCCNode extends PaxosNode {
  private static final String TAG = "MCCNode";
  private static final String METAFILE = "METAFILE";
  private static final String METAFILE_DELIMITER = "\t";
  private static final String CREDENTIAL_DELIMITER = "\t";
  private static final String VERSION_DELIMITER = "@";

  protected final NFSService nfsService;
  
  private Set<Integer> committedTids;
  private Map<String, Pair<Integer, Boolean>> fileVersions;
  
  /**
   * Security related objects
   */
  private byte[] secretKey;
  private HashMap<String,Pair<byte[],byte[]>> userCredentials;  //username:(salt,key hash) loaded from keystore
  private static String KEYSTORE_FILENAME = "keyStore";
  private static String SECRET_KEY_FILENAME = "secretKey";

  /**
   * Create a new Multiversioned Name File Storage Node.
   */
  public MCCNode() {
    super();
    this.nfsService = new NFSService(this);
    this.committedTids = new HashSet<Integer>();
    this.fileVersions = new HashMap<String, Pair<Integer, Boolean>>(); // filename, (version, deleted)
    this.userCredentials = new HashMap<String, Pair<byte[],byte[]>>();
  }

public void start() {
    this.committedTids.clear();
    this.fileVersions.clear();
    this.userCredentials.clear();
    
    // we started with a couple files, which we add to the fileVersions now.  These values will be
    //  overwritten if the METAFILE is newer than these values, which is perfect.
    //we set deleted = true so that its not read yet.  its technically not versioned properly until
    //  the first read.
    this.fileVersions.put(KEYSTORE_FILENAME, new Pair<Integer,Boolean>(0,true));

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
    
    
    //load user security credentials file
    try {
		List<String> credentialStrings = nfsService.read(getVersionedFilename(KEYSTORE_FILENAME));
		
		if(credentialStrings != null && !credentialStrings.isEmpty()){
			for(String line : credentialStrings) {
		          line = line.trim();
		          // if the empty string
		          if(line.length() == 0) {
		            continue;
		          }
	
		          String[] tokens = line.split(CREDENTIAL_DELIMITER);
		          if(tokens.length == 3) {
		        	  this.userCredentials.put(tokens[0], new Pair<byte[],byte[]>(Utility.hexStringToByteArray(tokens[1]),Utility.hexStringToByteArray(tokens[2])));
		          } else {
		            throw new IllegalStateException("User security credentials format corrupted");
		          }
		        }
		}
	} catch (IOException e) {
		e.printStackTrace();
	}
    
    loadSecretKey();

    super.start();
  }

	//----------------------------------------------------------------------------
	// MCC filesystem commands accessing local files
	//----------------------------------------------------------------------------

	private void loadSecretKey() {
		  try {
			//should have one line if its present
			List<String> lines = nfsService.read(SECRET_KEY_FILENAME);
	
			//if present, then load it
			if(lines!=null && !lines.isEmpty()) this.secretKey = Utility.hexStringToByteArray(lines.get(0));
	
		} catch (IOException e) {
			e.printStackTrace();
		}
	
	}

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
          		System.out.println("PANICCCCCCCCC!!!!!!!!!!!!!!!  Trying to create existing file!");
          		throw new RuntimeException("this is not ok");
          	}
            newVersionedFile = getVersionedFilename(filename);
            // reserve the 0-version file for blank newly created files
            runningSuccess = runningSuccess && nfsService.create(newVersionedFile);
            

        	//else if we are going to commit it, lets create the user credential stuff now
        	if(runningSuccess && ServerList.in(this.addr)){
				try {
            		//create salt
            		KeyGenerator kg;
					kg = KeyGenerator.getInstance("AES");
					SecretKey salt = kg.generateKey();
        			byte[] saltBytes = salt.getEncoded();
        			
        			//get username and password from the transaction
        			String[] userPass = op.dataline.split("\\|");
        			
        			//hash the key
        			byte[] keyHash = Utility.hashBytes(userPass[1],saltBytes);
        			
        			//store in local structure
        			this.userCredentials.put(userPass[0], new Pair<byte[],byte[]>(saltBytes,keyHash));
        			
        			//write to keystore file
        			Pair<Integer,Boolean> versionAndDeleted2 = fileVersions.get(KEYSTORE_FILENAME);
                  	
        			int newVersion = versionAndDeleted2.a + 1;
        			
                    String oldVersionedFile2 = getVersionedFilename(KEYSTORE_FILENAME);
                    fileVersions.put(KEYSTORE_FILENAME, new Pair<Integer,Boolean>(newVersion, false));
                    String newVersionedFile2 = getVersionedFilename(KEYSTORE_FILENAME);
                    
                    if (newVersion > 1) {
                        nfsService.copy(oldVersionedFile2, newVersionedFile2);
                      }

                    String outputString = userPass[0] + CREDENTIAL_DELIMITER + Utility.bytesToHexString(saltBytes) + CREDENTIAL_DELIMITER + Utility.bytesToHexString(keyHash) + "\n";
                    nfsService.append(newVersionedFile2, outputString);
        			
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
        	}
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
    currentFiles.remove(KEYSTORE_FILENAME);
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

    nfsService.write(METAFILE, data.toString());

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
  public void onCommitRequest(Integer from, RPCMsg message) {
    MCCMsg msg = (MCCMsg)message;
    Log.i(TAG, String.format("From node %d, received request %s", from, msg));
    onMCCRequest(from, msg);
  }

  @Override
  public void onCommitResponse(Integer from, RPCMsg message) {
    MCCMsg msg = (MCCMsg)message;
    Log.i(TAG, String.format("From node %d, received response %s", from, msg));
    boolean success = msg.success;
    Pair<Boolean,byte[]> securityResponse = 
                  new Pair<Boolean,byte[]>(msg.securityFlag, msg.securityCred);
    if(success && securityResponse.a) {
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
    onMCCResponse(from, msg.getId(), success, securityResponse);
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
  public void onMCCRequest(Integer from, MCCMsg msg) {
    Log.i(TAG, String.format("MCC Request from %d for transaction %s", 
                              from, msg.transaction));
    Log.v(TAG, String.format("Request to commit %s", msg));

    List<MCCFileData> filedataCheck = 
                      new ArrayList<MCCFileData>(Arrays.asList(msg.filearray));
    NFSTransaction transaction = msg.transaction;
    MCCMsg responseMsg = null;
    if(committedTids.contains(transaction.tid)) {
        Log.v(TAG, "DUPLICATE REQUEST, ALREADY COMMITTED ON THE SERVER");
      // DUPLICATE REQUEST, ALREADY COMMITTED ON THE SERVER
      responseMsg = new MCCMsg(msg, new ArrayList<MCCFileData>(), true, new Pair<Boolean,byte[]>(true,null));

    } else {
      // verify that the filedataCheck is up-to-version
      List<MCCFileData> filedataUpdate = checkVersions(filedataCheck, transaction);
      Pair<Boolean,byte[]> securityResponse = checkSecurity(transaction, from, filedataUpdate.isEmpty());

      if(filedataUpdate.isEmpty() && securityResponse.a) {
        Log.v(TAG, "UP-TO-VERSION!  COMMIT THAT SUCKA");
        // UP-TO-VERSION!  COMMIT THAT SUCKA
        System.out.println("****************IN ON MCC REQUEST**************");
        System.out.println(addr);
        System.out.println(transaction);
        commitTransaction(transaction);
        responseMsg = new MCCMsg(msg, filedataUpdate, true, securityResponse);

      } else {
        Log.v(TAG, "NO GOOD!  TOO LATE!  get them the new version data");
        // NO GOOD!  TOO LATE!  get them the new version data
        responseMsg = new MCCMsg(msg, filedataUpdate, false, securityResponse);
      }
    }

    RPCSendCommitResponse(from, responseMsg);
  }

  	/**
  	 * Returns true iff. the operations in the transaction are allowed given the security credentials
  	 * sent along with it, and our present security model.  Returns false otherwise.
  	 * Return value is a pair with the first being the security check flag, and the second being a 
  	 * 	return authorization token, where applicable.
  	 * @param transaction - the transaction, which includes the security credentials.
  	 * 
  	 */
	private Pair<Boolean,byte[]> checkSecurity(NFSTransaction transaction, int from, boolean willCommit) {
		//if this is a login transaction, the security credential is a username|password, otherwise its
		// an authentication token.
		byte[] credential = transaction.securityCredential;
		
		//if this is a login, the credential does not start with a null character
		//if it is not a login, it will be an authentication token or null.  An 
		// authentication token starts with a null byte so it is easy to identify.
		if(credential != null && credential[0]!=0){
			String loginCredential = new String(credential);
			String[] loginPair = loginCredential.split("\\|");
			Pair<byte[],byte[]> userCredential = this.userCredentials.get(loginPair[0]);
			
			//this user doesn't exist
			if(userCredential==null) return new Pair<Boolean,byte[]>(false,null);
			
			//now we hash the password|salt and make sure it matches our store
			byte[] computedHash = Utility.hashBytes(loginPair[1],userCredential.a);
			if(Arrays.equals(computedHash,userCredential.b)){
				System.out.println("Server says:_________________login credentials good.");
				
				//now we generate a new token
				String clearToken = Integer.toString(from) + CREDENTIAL_DELIMITER + loginPair[0];
				byte[] newTokenRaw = Utility.AESEncrypt(clearToken.getBytes(), this.secretKey);
				byte[] newToken = new byte[newTokenRaw.length + 1 + 32];
				
				byte[] mac = Utility.hashBytes(Utility.bytesToHexString(newTokenRaw), null);
				
				//...and make sure it starts with a \0
				//...and includes a mac at the end
				newToken[0] = 0;
				System.arraycopy(newTokenRaw,0,newToken,1,newTokenRaw.length);
				System.arraycopy(mac,0,newToken,newTokenRaw.length+1,mac.length);
				
				return new Pair<Boolean,byte[]>(true,newToken);
			}
			else{
				System.out.println("Server says:_________________login credentials bad.");
				return new Pair<Boolean,byte[]>(false,null);
			}
		}
		//else do a security check, using the token if it exists and as necessary
		else{		
			//first thing: decrypt the token, if it exists
			//any token will be rejected if malformed.  Only null tokens ignored.
			int tokenAddress = -1;
			String currentUser = "";
			if(credential!=null){
				try{
					byte[] token = new byte[credential.length-1-32];
					byte[] includedMAC = new byte[32];
					System.arraycopy(credential, 1, token, 0, credential.length-1-32);
					System.arraycopy(credential, credential.length-32, includedMAC, 0, 32);
					
					byte[] computedMAC = Utility.hashBytes(Utility.bytesToHexString(token), null);
					
					//the MAC did not match the MAC in the token
					if(!Arrays.equals(computedMAC, includedMAC)){
						System.out.println("Server says:_________________bad MAC on authentication token.");
						return new Pair<Boolean,byte[]>(false,null);
					}
					

					byte[] decrypted = Utility.AESDecrypt(token, this.secretKey);
					
					//bad decryption
					if(decrypted==null) return new Pair<Boolean,byte[]>(false,null);
					
					
					String[] decryptedToken = (new String(decrypted)).split(CREDENTIAL_DELIMITER);
					
					//bad token contents / not formatted or corrupted
					if(decryptedToken==null || decryptedToken.length!=2){
						System.out.println("Server says:_________________credential misformatted.");
						return new Pair<Boolean,byte[]>(false,null);
					}
					
					
					tokenAddress = Integer.parseInt(decryptedToken[0]);
					currentUser = decryptedToken[1];
					
					//from the wrong node; probably a break-in attempt with stolen token from some other node!
					if(tokenAddress != from){
						System.out.println("Server says:_________________credential from wrong node.");
						return new Pair<Boolean,byte[]>(false,null);
					}
					
				} catch (RuntimeException e){
					System.out.println("Server says:_________________bad authentication token.");
					return new Pair<Boolean,byte[]>(false,null);
				}
				
			}
			
			//now lets verify we are authorized to do these transactions
			for(NFSTransaction.NFSOperation op : transaction.ops){
				 switch (op.opType) {
				 	//can only touch files in the user space
		            case TOUCHFILE:
		            	if(!op.filename.endsWith("_followers.txt") && !op.filename.endsWith("_stream.txt")){
		            		return new Pair<Boolean,byte[]>(false,null);
		            	}
		                break;
		            //only create user command uses this, so it is highly restricted use.
		            // may not be logged in at this point, so will not involve token use.
		            // will have already been rejected if the file exists, so only need to check it is
		            //	a create user type of command.
		            case CREATEFILE:
		            	if(!op.filename.endsWith("_followers.txt")){
		            		return new Pair<Boolean,byte[]>(false,null);
		            	}
		            	break;
		            //if it is a followers file, you can append only yourself to it
		            //if it is a stream file, you can append only if that stream's user is in your followers
		            case APPENDLINE:
		            	if(op.filename.endsWith("_followers.txt")){
		            		if(!op.dataline.equals(currentUser)) return new Pair<Boolean,byte[]>(false,null);
		            	}
		            	else if(op.filename.endsWith("_stream.txt")){
		            		List<String> followers;
							try {
								followers = read(currentUser + "_followers.txt");
							} catch (IOException e) {
								return new Pair<Boolean,byte[]>(false,null);
							}
		        			
		        			if (followers != null) {
		        				boolean matchFound = false;
		        				for (String follower : followers) {
		        					String targetUser = op.filename.substring(0, op.filename.indexOf("_"));
		        					if(follower.equals(targetUser)) matchFound = true;
		        				}
		        				if(!matchFound) return new Pair<Boolean,byte[]>(false,null);
		        			}
		        			else return new Pair<Boolean,byte[]>(false,null);
		            	}
		            	else return new Pair<Boolean,byte[]>(false,null);
		            	break;
		            //Again - very restricted.  We only use this to clear your twitter stream, so that is
		            // all you can do with this.
		            case DELETEFILE:
		            	if(!op.filename.equals(currentUser + "_stream.txt")) return new Pair<Boolean,byte[]>(false,null);
		            	break;
		            //Two valid cases:
		            //1. Delete yourself from a followers file
		            //2. Delete any line from your followers
		            case DELETELINE:
		            	if (op.filename.endsWith("_followers.txt") && op.filename != currentUser+"_followers.txt"){
		            		if(!op.dataline.equals(currentUser)) return new Pair<Boolean,byte[]>(false,null);
		            	}
		            	break;
				 }

			}
		}
		
		return new Pair<Boolean,byte[]>(true,null);
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
  public abstract void onMCCResponse(Integer from, int tid, boolean success, Pair<Boolean,byte[]> securityResponse);



  /**
   * MCC message to send over RPC
   */
  protected static class MCCMsg implements RPCMsg {
    public static final long serialVersionUID = 0L;

    public final int id;
    public final NFSTransaction transaction;
    public final MCCFileData[] filearray;
    public final boolean success;
    public final boolean securityFlag;
    public final byte[] securityCred;

    private MCCMsg(int id, List<MCCFileData> filelist, NFSTransaction transaction, 
              boolean success, Pair<Boolean,byte[]> securityResponse) {
      this.id = id;
      this.filearray = filelist.toArray(new MCCFileData[filelist.size()]);
      this.transaction = transaction;
      this.success = success;
      this.securityFlag = securityResponse.a;
      this.securityCred = securityResponse.b;
    }

    /**
     * Wrapper around a file version list and a filesystem transaction.
     *
     * @param originalRequest
     *            The original requesting MCCMsg.  Used for id and transaction.
     * @param filelist
     *            A list of files and version numbers with contents.
     * @param success
     *            Whether the message represents a successful request
     * @param securityResponse
     *            Security-related flags.
     */
    MCCMsg(MCCMsg originalRequest, List<MCCFileData> filelist,
              boolean success, Pair<Boolean,byte[]> securityResponse) {
      this(originalRequest.id, filelist, originalRequest.transaction, 
           success, new Pair<Boolean,byte[]>(true, null));
    }

    /**
     * Wrapper around a file version list and a filesystem transaction.
     *
     * @param filelist
     *            A list of files and version numbers with contents.
     * @param transaction
     *            The filesystem transaction to include.
     */
    MCCMsg(List<MCCFileData> filelist, NFSTransaction transaction) {
      this(Math.abs(Utility.getRNG().nextInt()), filelist, 
           transaction, false, new Pair<Boolean,byte[]>(true, null));
    }

    public int getId() {
      return id;
    }

    public String toString() {
      return String.format("MCCMsg{%d, success? %s}", id, success);
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
