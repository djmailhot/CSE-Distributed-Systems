import edu.washington.cs.cse490h.lib.Utility;

import java.util.Date;
import java.util.List;
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
  private enum MessageType {
    REQUEST, RESPONSE;

    public static NFSProcedure parseInt(int value) {
      switch(value) {
        case REQUEST.ordinal(): return REQUEST;
        case RESPONSE.ordinal(): return RESPONSE;
        default: return null;
      }
    }
  }

  /**
   * Enum to specify the RPC procedure name.
   */
  private enum NFSProcedure {
    CREATE, READ, APPEND, CHECK, DELETE;

    public static NFSProcedure parseInt(int value) {
      switch(value) {
        case CREATE.ordinal(): return CREATE;
        case READ.ordinal(): return READ;
        case APPEND.ordinal(): return APPEND;
        case CHECK.ordinal(): return CHECK;
        case DELETE.ordinal(): return DELETE;
        default: return null;
      }
    }
  }

	//----------------------------------------------------------------------------
	// transaction routines
	//----------------------------------------------------------------------------

  /**
   * Create the specified file.
   */
  public JSONObject transactionCreate(String filename) {
    return newTransaction(NFSProcedure.CREATE.ordinal(), filename);
  }

  /**
   * Read the specified file.
   */
  public JSONObject transactionRead(String filename) {
    return newTransaction(NFSProcedure.READ.ordinal(), filename);
  }

  /**
   * Append the specified string to the specified file.
   * The string will be followed by a newline, such that repeated append
   * calls will be written to separate lines.
   */
  public JSONObject transactionAppend(String filename, String data) {
    JSONObject t = newTransaction(NFSProcedure.APPEND.ordinal(), filename);
    t.put("data", data);
    return t;
  }

  /**
   * Check that the version of the specified file is not newer than the
   * specified date.
   */
  public JSONObject transactionCheck(String filename, Date date) {
    JSONObject t = newTransaction(NFSProcedure.CHECK.ordinal(), filename);
    t.put("date", date.toString());
    return t;
  }

  /**
   * Delete the specified file.
   */
  public JSONObject transactionDelete(String filename) {
    return newTransaction(NFSProcedure.DELETE.ordinal(), filename);
  }

  private JSONObject newTransaction(int procedure, String filename) {
    JSONObject t = new JSONObject();
    t.put("procedure", procedure);
    t.put("messageType", MessageType.REQUEST.ordinal());
    t.put("filename", filename);
    return t;
  }

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
  public void RPCSend(int destAddr, JSONObject transaction) {
    byte[] payload = Utility.stringToByteArray(transaction.toString());
    RIOSend(destAddr, Protocol.DATA, payload);
  }

	/**
	 * Send a bundle of RPC transactions over to a remote node
	 * 
	 * @param destAddr
	 *            The address to send to
	 * @param bundle
	 *            The bundle of transactions to send
	 */
  public void RPCSend(int destAddr, List<JSONObject> bundle) {
    for(JSONObject transaction : bundle) {
      RPCSend(destAddr, transaction);
    }
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
        int messageType = transaction.getInt("messageType");

        switch (MessageType.parseInt(messageType)) {
          case REQUEST:
            onRPCRequest(from, transaction);
            break;
          case RESPONSE:
            onRPCResponse(from, transaction);
            break;
          default:
            LOG.warn("Received invalid message type");
        }
      } catch(JSONException e) {
        LOG.warn("RPC data message could not be parsed");
      }
    } else {
      // no idea what to do
    }
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
  public void onRPCRequest(Integer from, JSONObject transaction) 
        throws JSONException {
    int procedure = message.getInt("procedure");
    String filename = message.getString("filename");

    // TODO:  link these to the NFSService
    switch (NFSProcedure.parseInt(procedure)) {
      case CREATE:
      case READ:
      case APPEND:
      case CHECK:
      case DELETE:
      default:
        LOG.warn("Received invalid procedure type");
    }
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
	 */
  public abstract void onRPCResponse(Integer from, JSONObject transaction)
        throws JSONException;

}
