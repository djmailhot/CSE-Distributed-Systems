import edu.washington.cs.cse490h.lib.Utility;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.logging.Logger;

/**
 * The MCCNode module gives Multiversion Concurrency Control over the 
 * Named File Storage of a particular node.
 *
 * It uses discrete Transactions to jump from one version to the next.
 */
public abstract class MCCNode extends RPCNode {
  private static final Logger LOG = Logger.getLogger(MCCNode.class.getName());
  private static final String METAFILE = "METAFILE";
  private static final String METAFILE_DELIMITER = "\t";

  private final NFSService nfsService;

  private Set<Integer> committedTids;
  private Map<String, Integer> fileVersions;

  /**
   * Create a new Multiversioned Name File Storage Node.
   */
  public MCCNode() {
    this.nfsService = new NFSService(this);
  }

  public void start() {
    this.committedTids = new HashSet<Integer>();
    this.fileVersions = new HashMap<String, Integer>();

    // read the metafile and populate the internal data structures
    try {
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
        } else if(tokens.length == 2) {
          fileVersions.put(tokens[1], Integer.parseInt(tokens[0]));
        } else {
          throw new IllegalStateException("Metafile format corrupted");
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
    return nfsService.read(filename);
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
    return nfsService.exists(filename);
  }

	//----------------------------------------------------------------------------
	// MCC filesystem management
	//----------------------------------------------------------------------------

  /**
   * Commits the specified transaction to this node's filesystem.
   *
   * @return true if successful
   */
  private boolean commitTransaction(List<MCCFileData> filedata,
                                    NFSTransaction transaction) {
    boolean success = true;
    try {
      for(NFSTransaction.NFSOperation op : transaction.ops) {
        switch (op.opType) {
          case CREATEFILE:
            success = success && nfsService.create(op.filename);
            break;
          case APPENDLINE:
            success = success && nfsService.append(op.filename, op.dataline);
            break;
          case DELETEFILE:
            success = success && nfsService.delete(op.filename);
            break;
          case DELETELINE:
            success = success && nfsService.deleteLine(op.filename, op.dataline);
            break;
          default:
            LOG.warning("Received invalid operation type");
        }
      }

      writeMetafile();  // atomically commit this transaction
    } catch(IOException e) {
      LOG.severe("File system failure");
      e.printStackTrace();
      return false;
    } finally {
      return success;
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
    StringBuilder data = new StringBuilder();
    for(Integer tid : committedTids) {
      data.append(String.format("%d\n", tid));
    }
    for(String filename : fileVersions.keySet()) {
      int version = fileVersions.get(filename);
      data.append(String.format("%d%s%s\n", version, METAFILE_DELIMITER, filename));
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
    //TODO: compile a list of MCCFileData objects, build an RPCBundle, RPC
    // submit
  }

	//----------------------------------------------------------------------------
	// receive routines
	//----------------------------------------------------------------------------

  public void onRPCRequest(Integer from, RPCBundle bundle) {
    onMCCRequest(from, bundle.filelist, bundle.transaction);
  }


  public void onRPCResponse(Integer from, RPCBundle bundle) {
  }

	/**
	 * Method that is called by the RPC layer when an RPC Request bundle is 
   * received.
   * Request bundles are MCC invocations on a remote node.
	 * 
	 * @param from
	 *            The address from which the message was received
   * @param filelist
   *            A list of files and version numbers.
   *            The file contents are not used.
   * @param transaction
   *            The filesystem transaction to send.
	 */
  public void onMCCRequest(Integer from, List<MCCFileData> filelist, 
                           NFSTransaction transaction) {
    //TODO: make this actually work
    RPCBundle response = null;
    commitTransaction(filelist, transaction);
    RPCSendResponse(from, response);
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
	 * @param committed
	 *            True if the specified transaction was successful
   *
	 */
  public abstract void onMCCResponse(Integer from, int tid, boolean success);



}
