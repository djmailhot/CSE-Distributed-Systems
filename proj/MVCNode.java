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
 * The MVCNode module gives MultiVersion Control over the Named File 
 * Storage of a particular node.
 *
 * It uses discrete Transactions to jump from one version to the next.
 */
public abstract class MVCNode extends RPCNode {
  private static final Logger LOG = Logger.getLogger(MVCNode.class.getName());

  private static final String TEMP_FILE_PREFIX = "_cow_"; // commit on write
  private static final String METAFILE = "METAFILE";

  protected final NFSService nfsService;


  /**
   * Create a new Multiversioned Name File Storage Node.
   */
  public MVCNode() {
    this.nfsService = new NFSService(this);
  }

  /**
   * Update all files on this NFS file system to match the most recent
   * versions on the specified remote node.
   *
   * @param destAddr the address of the target remote node.
   *
   * Will recive a response through the onMVCResponse callback method.
   */
  public void updateAllFiles(int destAddr) {
    //TODO: Rainbow Dash
  }

  /**
   * Commit the specified transaction to the specified remote node.
   *
   * @param destAddr the address of the target remote node.
   * @param transaction the filesystem transaction to commit.
   *
   * Will recive a response through the onMVCResponse callback method.
   */
  public void commitTransaction(int destAddr, NFSTransaction transaction) {
    //TODO: Twilight Sparkle
  }


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



	/**
	 * Method that is called by the RPC layer when an RPC Request bundle is 
   * received.
   * Request bundles are MVC invocations on a remote node.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param transaction
	 *            The RPC transaction that was received
	 */
  public void onMVCRequest(Integer from, NFSTransaction transaction) {
    //TODO: make this actually work
    MVCBundle response = null;
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
        RPCSend(from, response);
      } catch(IOException e) {
        LOG.severe("File system failure");
        e.printStackTrace();
      }
    }
  }

	/**
	 * Method that is called by the MVC layer when a response is received
   * after a updateAllFiles or a commitTransaction request.
   * Responses are replies to Requests.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param transaction
	 *            The RPC transaction that was received
   *
	 */
  public abstract void onMVCResponse(Integer from, MVCBundle bundle);

  /**
   * Some sweet ass class.
   */
  public static class MVCBundle {

  }

}
