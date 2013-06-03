import edu.washington.cs.cse490h.lib.Utility;
import edu.washington.cs.cse490h.lib.Node.NodeCrashException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;

import plume.Pair;

/**
 * Extension to the RIONode class that adds support for sending and receiving
 * RPC transactions.
 *
 * A subclass of RPCNode must implement onRPCResponse to handle response RPC
 * messages
 */
public abstract class RPCNode extends RIONode {
  private static final Logger LOG = Logger.getLogger(RPCNode.class.getName());

  /**
   * Enum to specify the RPC message type.
   */
  public static enum MessageType {
    REQUEST, RESPONSE;

    public static MessageType fromInt(int value) {
      if(value == REQUEST.ordinal()) { return REQUEST; }
      else if(value == RESPONSE.ordinal()) { return RESPONSE; }
      else { return null; }
    }
  }

  public RPCNode() {
    super();
  }

	//----------------------------------------------------------------------------
	// send routines
	//----------------------------------------------------------------------------

	/**
	 * Send a RPC call request over to a remote node.
	 * Includes a file version list and a filesystem transaction.
	 * 
	 * @param destAddr
	 *            The address to send to
   * @param filelist
   *            A list of files and version numbers.
   *            The file contents are not used.
	 * @param transaction
	 *            The filesystem transaction to send.
   *            If null, won't send any transaction.
	 */
  public void RPCSendRequest(int destAddr, RPCBundle bundle) {
    RIOSend(destAddr, Protocol.DATA, RPCBundle.serialize(bundle));
  }

	/**
	 * Send a RPC call response over to a remote node.
	 * Includes a file version list.
	 * 
	 * @param destAddr
	 *            The address to send to
   * @param filelist
   *            A list of files and version numbers.
   *            They must also contain file contents.
	 */
  public void RPCSendResponse(int destAddr, RPCBundle bundle) {
    RIOSend(destAddr, Protocol.DATA, RPCBundle.serialize(bundle));
  }

	//----------------------------------------------------------------------------
	// receive routines
	//----------------------------------------------------------------------------

	/**
	 * Method that is called by the RIO layer when a message is to be delivered.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param protocol
	 *            The protocol identifier of the message
	 * @param msg
	 *            The message that was received
	 */
  @Override
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
    if(protocol == Protocol.DATA) {
      try {
        RPCBundle bundle = RPCBundle.deserialize(msg);
        MessageType messageType = bundle.type;
        switch (messageType) {
          case REQUEST:
          	// TODO: Here, we hook into Paxos?
          	// Perhaps we should add PAXOS_REQUEST and PAXOS_RESPONSE to messageType?
          	// That way, we know the requests here come from a client machine only
            onRPCRequest(from, bundle);
            break;
          case RESPONSE:
          	// TODO: Here, hook into Paxos?
            onRPCResponse(from, bundle);
            break;
          default:
            LOG.warning("Received invalid message type");
        }
      }
      catch(IllegalArgumentException e) {
        LOG.warning("Data message could not be deserialized into valid RPCBundle");
      }
    } else {
      // no idea what to do
    }
  }

  /**
   * Returns the transaction id of the specified message.
   * @return an int, the transaction id of this message
   */
  public static int extractMessageId(byte[] msg) {
    RPCBundle bundle = RPCBundle.deserialize(msg);
    return bundle.tid;
  }

  /**
   * Returns the message type of the specified message.
   * @return a MessageType describing the type of message received
   */
  public static MessageType extractMessageType(byte[] msg) {
    RPCBundle bundle = RPCBundle.deserialize(msg);
    return bundle.type;
  }
  
  //-------------------- Paxos Methods Begin -------------------------------------------------------//
  
  private void sendPrepareRequest(/* args*/) {
  	
  }  
  
  private void receivePrepareRequest(/* args*/) {
  	// Do stuff
  	sendPrepareResponse();
  }
  
  private void sendPrepareResponse() {
  	
  }
  
  private void sendAcceptRequest(/* args */) {
  	
  }
  
  private void receiveAcceptRequest(/* args */) {
  	// Do stuff
  	sendAcceptRepsonse();
  }
  
  private void sendAcceptRepsonse() {
  	
  }
  
  private void sendLearnValueRequest()	 {
  	
  }
  
  private void receiveLearnValueResponse() {
  	
  }
  //-------------------- Paxos Methods End ---------------------------------------------------------//
  
	/**
	 * Method that is called by the RPC layer when an RPC Request transaction is 
   * received.
   * Request transactions are RPC invocations on a remote node.
	 * 
	 * @param from
	 *            The address from which the message was received
   * @param filelist
   *            A list of files and version numbers.
   *            The file contents are not used.
	 * @param transaction
	 *            The filesystem transaction to send.
	 */
  public abstract void onRPCRequest(Integer from, RPCBundle bundle);

	/**
	 * Method that is called by the RPC layer when an RPC Response transaction is 
   * received.
   * Response transactions are replies to Request transactions.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param from
	 *            The address from which the message was received
   * @param filelist
   *            A list of files and version numbers.
   *            The file contents are not used.
   *
	 */
  public abstract void onRPCResponse(Integer from, RPCBundle bundle);

  /**
   * Some sweet ass class.
   */
  public static class RPCBundle implements Serializable {
    public static final long serialVersionUID = 0L;

    public final MessageType type;  // request or response
    public final int tid;  // transaction id
    public final boolean success;
    public final boolean securityFlag;
    public final byte[] securityCred;
    public final NFSTransaction transaction;
    //public final List<MCCFileData> filelist;
    public final MCCFileData[] filearray;

    /**
     * Wrapper around a file version list and a filesystem transaction.
     *
     * @param type
     *            Whether this is a REQUEST or RESPONSE RPC message
     * @param success
     *            Whether the bundle represents a successful request
     * @param filelist
     *            A list of files and version numbers with contents.
     * @param transaction
     *            The filesystem transaction to include.
     */
    RPCBundle(MessageType type, boolean success, 
                     List<MCCFileData> filelist, NFSTransaction transaction) {
      this.type = type;
      this.success = success;
      this.securityFlag = true;
      this.securityCred = null;
      //this.filelist = filelist;
      this.filearray = new MCCFileData[filelist.size()]; //filelist.toArray();
      for (int i = 0; i < filelist.size(); i++) {
      	this.filearray[i] = filelist.get(i);
      }
      this.transaction = transaction;
      this.tid = transaction.tid;
    }
    
    /**
     * Same constructor as above, but includes a security-related flag.
     */
    RPCBundle(MessageType type, boolean success, Pair<Boolean,byte[]> securityResponse,
                     List<MCCFileData> filelist, NFSTransaction transaction) {
      this.type = type;
      this.success = success;
      this.securityFlag = securityResponse.a;
      this.securityCred = securityResponse.b;
      //this.filelist = filelist;
      this.filearray = new MCCFileData[filelist.size()]; //filelist.toArray();
      for (int i = 0; i < filelist.size(); i++) {
      	this.filearray[i] = filelist.get(i);
      }
      this.transaction = transaction;
      this.tid = transaction.tid;
    }

    /**
     * Specifically constructs a request bundle.
     *
     * @param filelist
     *            A list of files and version numbers.
     * @param transaction
     *            The filesystem transaction to include.
     */
    public static RPCBundle newRequestBundle(List<MCCFileData> filelist,
                                             NFSTransaction transaction) {
      return new RPCBundle(MessageType.REQUEST, false, filelist, transaction);
    }
  
    /**
     * Specifically constructs a response bundle.
     *
     * @param filelist
     *            A list of files and version numbers with contents.
     * @param transaction
     *            The filesystem transaction to include.
     * @param success
     *            Whether the bundle represents a successful request
     */
    public static RPCBundle newResponseBundle(List<MCCFileData> filelist,
                                              NFSTransaction transaction,
                                              boolean success, Pair<Boolean,byte[]> securityResponse) {
      return new RPCBundle(MessageType.RESPONSE, success, securityResponse, filelist, transaction);
    }
    
    public static byte[] serialize(RPCBundle bundle) {
    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
    	ObjectOutput out = null;
    	try {
    	  out = new ObjectOutputStream(bos);   
    	  out.writeObject(bundle);
    	  byte[] bytes = bos.toByteArray();
    	  return bytes;
    	} catch (IOException e) {
				e.printStackTrace();
			} finally {
    	  try {
      	  out.close();
					bos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
    	}
    	return null;
    }
    
    public static RPCBundle deserialize(byte[] bytes) {
    	ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    	ObjectInput in = null;
    	try {
    	  in = new ObjectInputStream(bis);
    	  Object o = in.readObject(); 
    	  RPCBundle bundle = (RPCBundle) o;
    	  return bundle;
    	} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
    	  try {
					bis.close();
	    	  in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
    	}
      return null;
    }
    
    public String toString() {
      if(type == MessageType.REQUEST) {
        return String.format("RPCBundle{REQUEST, %d}", tid);
      } else if(type == MessageType.RESPONSE) {
        return String.format("RPCBundle{RESPONSE, %d, Success? %s}",
                              tid, success);
      } else {
        return "RPCBundle{ invalid state }";
      }
    }
  }
}
