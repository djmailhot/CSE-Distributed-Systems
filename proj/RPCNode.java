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

  private final NFSService nfsService;

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

  /**
   * Enum to specify the RPC operation name.
   */
  public static enum NFSOperation {
    CREATE, READ, APPEND, CHECK, DELETE;

    public static NFSOperation fromInt(int value) {
      if(value == CREATE.ordinal()) { return CREATE; }
      else if(value == READ.ordinal()) { return READ; }
      else if(value == APPEND.ordinal()) { return APPEND; }
      else if(value == CHECK.ordinal()) { return CHECK; }
      else if(value == DELETE.ordinal()) { return DELETE; }
      else { return null; }
    }
  }

  public RPCNode() {
    this.nfsService = new NFSService(this);
  }

	//----------------------------------------------------------------------------
	// transaction routines
	//----------------------------------------------------------------------------

  /**
   * Returns the UUID of the specified transaction.
   * @return the UUID, or null if none present
   */
  public static UUID extractUUID(JSONObject transaction) {
    UUID uuid = null;
    try {
      uuid = UUID.fromString(transaction.getString("uuid"));
    } catch(JSONException e) {
      LOG.warning("JSON parsing error for RPC transaction");
      e.printStackTrace();
    }
    return uuid;
  }

  /**
   * Returns the MessageType of the specified transaction.
   * @return the MessageType, or null if none present
   */
  public static MessageType extractMessageType(JSONObject transaction) {
    MessageType messageType = null;
    try {
      messageType = MessageType.fromInt(transaction.getInt("messageType"));
    } catch(JSONException e) {
      LOG.warning("JSON parsing error for RPC transaction");
      e.printStackTrace();
    }
    return messageType;
  }

  /**
   * Returns the NFSOperation of the specified transaction.
   * @return the NFSOperation, or null if none present
   */
  public static NFSOperation extractNFSOperation(JSONObject transaction) {
    NFSOperation nfsOperation = null;
    try {
      nfsOperation = NFSOperation.fromInt(transaction.getInt("operation"));
    } catch(JSONException e) {
      LOG.warning("JSON parsing error for RPC transaction");
      e.printStackTrace();
    }
    return nfsOperation;
  }

  /**
   * Create the specified file.
   * @return the transaction, or null if a JSON parsing error
   */
  public static JSONObject transactionCreate(String filename) {
    JSONObject transaction = null;
    try {
      transaction = newTransaction(NFSOperation.CREATE.ordinal(), filename);
    } catch(JSONException e) {
      LOG.warning("JSON parsing error for RPC transaction");
      e.printStackTrace();
    }
    return transaction;
  }

  /**
   * Read the specified file.
   * @return the transaction, or null if a JSON parsing error
   */
  public static JSONObject transactionRead(String filename) {
    JSONObject transaction = null;
    try {
      transaction = newTransaction(NFSOperation.READ.ordinal(), filename);
    } catch(JSONException e) {
      LOG.warning("JSON parsing error for RPC transaction");
      e.printStackTrace();
    }
    return transaction;
  }

  /**
   * Append the specified string to the specified file.
   * The string will be followed by a newline, such that repeated append
   * calls will be written to separate lines.
   * @return the transaction, or null if a JSON parsing error
   */
  public static JSONObject transactionAppend(String filename, String data) {
    JSONObject transaction = null;
    try {
      transaction = newTransaction(NFSOperation.APPEND.ordinal(), filename);
      transaction.put("data", data);
    } catch(JSONException e) {
      LOG.warning("JSON parsing error for RPC transaction");
      e.printStackTrace();
    }
    return transaction;
  }

  /**
   * Check that the version of the specified file is not newer than the
   * specified date.
   * @return the transaction, or null if a JSON parsing error
   */
  public static JSONObject transactionCheck(String filename, Date date) {
    JSONObject transaction = null;
    try {
      transaction = newTransaction(NFSOperation.CHECK.ordinal(), filename);
      transaction.put("date", date.getTime());
    } catch(JSONException e) {
      LOG.warning("JSON parsing error for RPC transaction");
      e.printStackTrace();
    }
    return transaction;
  }

  /**
   * Delete the specified file.
   * @return the transaction, or null if a JSON parsing error
   */
  public static JSONObject transactionDelete(String filename) {
    JSONObject transaction = null;
    try {
      transaction = newTransaction(NFSOperation.DELETE.ordinal(), filename);
    } catch(JSONException e) {
      LOG.warning("JSON parsing error for RPC transaction");
      e.printStackTrace();
    }
    return transaction;
  }

  private static JSONObject newTransaction(int operation, String filename)
        throws JSONException {
    JSONObject t = new JSONObject();
    t.put("operation", operation);
    t.put("messageType", MessageType.REQUEST.ordinal());
    t.put("filename", filename);
    t.put("uuid", UUID.randomUUID());
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
  public List<UUID> RPCSend(int destAddr, List<JSONObject> bundle) {
    List<UUID> uuidList = new ArrayList<UUID>();
    for(JSONObject transaction : bundle) {
      uuidList.add(extractUUID(transaction));
      RPCSend(destAddr, transaction);
    }
    return uuidList;
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
    try {
      String filename = transaction.getString("filename");
      JSONObject response = prepareResponse(transaction);

      NFSOperation nfsOperation = extractNFSOperation(transaction);
      switch (nfsOperation) {
        case CREATE:
          nfsService.create(filename);
          break;
        case READ:
          JSONArray returnData = new JSONArray(nfsService.read(filename));
          response.put("data", returnData);
          break;
        case APPEND:
          String appendData = transaction.getString("data");
          nfsService.append(filename, appendData);
          break;
        case CHECK:
          Date date = new Date(transaction.getLong("date"));
          boolean status = nfsService.check(filename, date);
          break;
        case DELETE:
          nfsService.delete(filename);
          break;
        default:
          LOG.warning("Received invalid operation type");
      }
      RPCSend(from, response);
    } catch(IOException e) {
      LOG.severe("File system failure");
      e.printStackTrace();
    } catch(JSONException e) {
      LOG.severe("Request message incorrectly formatted");
      e.printStackTrace();
    }
  }

  private JSONObject prepareResponse(JSONObject request) throws JSONException {
    JSONObject response = new JSONObject();
    UUID uuid = extractUUID(request);
    response.put("uuid", uuid.toString());
    response.put("messageType", MessageType.RESPONSE.ordinal());
    response.put("filename", request.getString("filename"));
    response.put("operation", request.getString("operation"));
    return response;
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
  public abstract void onRPCResponse(Integer from, JSONObject transaction);

}
