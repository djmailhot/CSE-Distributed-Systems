import edu.washington.cs.cse490h.lib.Utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;

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
    RIOSend(destAddr, Protocol.DATA, serialize(bundle, MessageType.REQUEST));
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
    RIOSend(destAddr, Protocol.DATA, serialize(bundle, MessageType.RESPONSE));
  }

  private byte[] serialize(RPCBundle bundle, MessageType type) {
    return new byte[0];
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
        RPCBundle bundle = deserialize(msg);
        MessageType messageType = bundle.type;
        switch (messageType) {
          case REQUEST:
            onRPCRequest(from, bundle);
            break;
          case RESPONSE:
            onRPCResponse(from, bundle);
            break;
          default:
            LOG.warning("Received invalid message type");
        }
      } catch(IllegalArgumentException e) {
        LOG.warning("Data message could not be parsed into valid RPCBundle");
      }
    } else {
      // no idea what to do
    }
  }

  private static RPCBundle deserialize(byte[] msg) throws
        IllegalArgumentException {
    try {
    } catch(JSONException e) {
      throw new IllegalArgumentException("Data message could not be parsed into valid RPCBundle");
    }
    return null;
  }

  /**
   * Returns the transaction id of the specified message.
   * @return an int, the transaction id of this message
   */
  public static int extractMessageId(byte[] msg) {
    RPCBundle bundle = deserialize(msg);
    return bundle.transaction.tid;
  }

  /**
   * Returns the message type of the specified message.
   * @return a MessageType describing the type of message received
   */
  public static MessageType extractMessageType(byte[] msg) {
    RPCBundle bundle = deserialize(msg);
    return bundle.type;
  }
  
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
  public static class RPCBundle {
    public final MessageType type;
    public final NFSTransaction transaction;
    public final List<MVCFileData> filelist;

    /**
     * Wrapper around a file version list and a filesystem transaction.
     *
     * @param filelist
     *            A list of files and version numbers.
     *            The file contents are not used.
     * @param transaction
     *            The filesystem transaction to send.
     */
    public RPCBundle(List<MVCFileData> filelist, NFSTransaction transaction,
                     MessageType type) {
      this.filelist = filelist;
      this.transaction = transaction;
      this.type = type;
    }
  }
}
