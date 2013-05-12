import edu.washington.cs.cse490h.lib.Utility;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

  protected final NFSService nfsService;


  /**
   * Create a new Multiversioned Name File Storage Node.
   */
  public MCCNode() {
    this.nfsService = new NFSService(this);
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
    for(NFSTransaction.NFSOperation op : transaction.ops) {
      try {
        switch (op.opType) {
          case CREATEFILE:
            nfsService.create(op.filename);
            break;
          case APPENDLINE:
            nfsService.append(op.filename, op.dataline);
            break;
          case DELETEFILE:
            nfsService.delete(op.filename);
            break;
          case DELETELINE:
            nfsService.deleteLine(op.filename, op.dataline);
            break;
          default:
            LOG.warning("Received invalid operation type");
        }
      } catch(IOException e) {
        LOG.severe("File system failure");
        e.printStackTrace();
      }
    }
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
