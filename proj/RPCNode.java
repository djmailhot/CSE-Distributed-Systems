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
	// transaction routines
	//----------------------------------------------------------------------------

	//----------------------------------------------------------------------------
	// send routines
	//----------------------------------------------------------------------------

	/**
	 * Send a single RPC transaction over to a remote node
	 * 
	 * @param destAddr
	 *            The address to send to
	 * @param transaction
	 *            The transaction to send
	 */
  public void RPCSend(int destAddr, MVCNode.MVCBundle bundle) {
    byte[] payload = bundle.serialize();
    RIOSend(destAddr, Protocol.DATA, payload);
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
        String payload = Utility.byteArrayToString(msg);
        JSONObject transaction = new JSONObject(payload);

        MessageType messageType = extractMessageType(transaction);
        switch (messageType) {
          case REQUEST:
            onRPCRequest(from, transaction);
            break;
          case RESPONSE:
            onRPCResponse(from, transaction);
            break;
          default:
            LOG.warning("Received invalid message type");
        }
      } catch(JSONException e) {
        LOG.warning("Data message could not be parsed to RPC JSON transaction");
      }
    } else {
      // no idea what to do
    }
  }

  /**
   * Returns the UUID of the specified transaction.
   * @return the UUID, or null if none present
   */
  public static UUID extractUUID(byte[] msg) {
    String payload = Utility.byteArrayToString(msg);
    UUID uuid = null;
    try {
      JSONObject transaction = new JSONObject(payload);
      uuid = UUID.fromString(transaction.getString("uuid"));
    } catch(JSONException e) {
      LOG.warning("JSON parsing error for RPC transaction");
      e.printStackTrace();
    }
    return uuid;
  }
  
  /**
   * Returns the message type of the specified transaction.
   * @return the message type, null if none present.
   */
  public static MessageType extractMessageType(byte[] msg) {
    String payload = Utility.byteArrayToString(msg);
    MessageType mt = null;
    try {
      JSONObject transaction = new JSONObject(payload);
      mt = MessageType.values()[transaction.getInt("messageType")];
    } catch(JSONException e) {
      LOG.warning("JSON parsing error for RPC transaction: " + payload);
      e.printStackTrace();
    }
    return mt;
  }

	/**
	 * Method that is called by the RPC layer when an RPC Request transaction is 
   * received.
   * Request transactions are RPC invocations on a remote node.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param transaction
	 *            The RPC transaction that was received
	 */
  public void onRPCRequest(Integer from, JSONObject transaction) {
    //TODO: do something rad
  }

	/**
	 * Method that is called by the RPC layer when an RPC Response transaction is 
   * received.
   * Response transactions are replies to Request transactions.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param transaction
	 *            The RPC transaction that was received
   *
   * The transaction should contain the following fields:
   * String "uuid" - unique id
   * int "messageType" - MessageType enum ordinal
   * int "operation" - NFSOperation enum ordinal
   * String "filename" - the file that was targeted
   *
   * Conditional fields on MessageType:
   * if("messageType" -> MessageType.READ)
   *   JSONArray "filelines" - array of strings, each a separate line of the file
   * if("messageType" -> MessageType.CHECK)
   *   long "date" - the long representation of a Date object
   *   Boolean "check" - true if the file version is no newer than the
   *                     accompanying Date
   * if("messageType" -> MessageType.EXISTS)
   *   Boolean "exists" - true if the specified file exists
   *
	 */
  public abstract void onRPCResponse(Integer from, JSONObject transaction);

}
