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
import java.util.*;

/**
 * Extension to the RIONode class that adds support for sending and receiving
 * RPC transactions.
 *
 * A subclass of RPCNode must implement onRPCResponse to handle response RPC
 * messages
 */
public abstract class RPCNode extends RIONode {
  private final String TAG;

	//----------------------------------------------------------------------------
	// RPC Layer public interfaces
	//----------------------------------------------------------------------------

  /**
   * Defines an interface for a message meant to be sent over RPC.
   */
  public interface RPCMsg extends Serializable {
    /**
     * Returns an unique id that is shared between Request and Response
     * messages.
     */
    public int getId();

  }

	//----------------------------------------------------------------------------
	// startup routines
	//----------------------------------------------------------------------------


  public RPCNode() {
    super();
    this.TAG = String.format("RPCNode.%d", addr);
  }

  public void start() {
    super.start();
  }

	//----------------------------------------------------------------------------
	// send routines
	//----------------------------------------------------------------------------

	/**
	 * Send a transaction commit request over to a remote node.
	 * Includes a file version list and a filesystem transaction.
	 * 
	 * @param destAddr
	 *            The address to send to
   * @param bundle
   *            The RPCCallBundle to send over, containing
   *            a list of files and version numbers
	 *            and the filesystem transaction to commit.
	 */
  public void RPCSendCommitRequest(int destAddr, RPCMsg msg) {
    Log.i(TAG, String.format("Commit request to %d of %s", destAddr, msg));
    RPCCallBundle bundle = new RPCCallBundle(msg.getId(), RPCCallType.REQUEST,
                                             RPCMsgType.COMMIT, msg);
    RIOSend(destAddr, Protocol.DATA, RPCCallBundle.serialize(bundle));
  }

	/**
	 * Send a transaction commit response over to a remote node.
	 * Includes a file version list.
	 * 
	 * @param destAddr
	 *            The address to send to
   * @param bundle
   *            The RPCCallBundle to send over, containing
   *            a list of files and version numbers
	 *            and the filesystem transaction to commit.
	 */
  public void RPCSendCommitResponse(int destAddr, RPCMsg msg) {
    Log.i(TAG, String.format("Commit response to %d of %s", destAddr, msg));
    RPCCallBundle bundle = new RPCCallBundle(msg.getId(), RPCCallType.RESPONSE,
                                             RPCMsgType.COMMIT, msg);
    RIOSend(destAddr, Protocol.DATA, RPCCallBundle.serialize(bundle));
  }

	/**
	 * Send a Paxos request over to a remote node.
	 * Includes a file version list and a filesystem transaction.
	 * 
	 * @param destAddr
	 *            The address to send to
	 */
  public void RPCSendPaxosRequest(int destAddr, RPCMsg msg) {
    Log.i(TAG, String.format("Paxos request to %d of %s", destAddr, msg));
    RPCCallBundle bundle = new RPCCallBundle(msg.getId(), RPCCallType.REQUEST,
                                             RPCMsgType.PAXOS, msg);
    RIOSend(destAddr, Protocol.DATA, RPCCallBundle.serialize(bundle));
  }

	/**
	 * Send a Paxos call response over to a remote node.
	 * Includes a file version list.
	 * 
	 * @param destAddr
	 *            The address to send to
	 */
  public void RPCSendPaxosResponse(int destAddr, RPCMsg msg) {
    Log.i(TAG, String.format("Paxos response to %d of %s", destAddr, msg));
    RPCCallBundle bundle = new RPCCallBundle(msg.getId(), RPCCallType.RESPONSE,
                                             RPCMsgType.PAXOS, msg);
    RIOSend(destAddr, Protocol.DATA, RPCCallBundle.serialize(bundle));
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
        RPCCallBundle bundle = RPCCallBundle.deserialize(msg);
        RPCMsgType msgType = bundle.msgType;
        switch (msgType) {
          case COMMIT:
            switch (bundle.callType) {
              case REQUEST:
                onRPCCommitRequest(from, bundle.msg);
                break;
              case RESPONSE:
                onRPCCommitResponse(from, bundle.msg);
                break;
            }
            break;
          case PAXOS:
            switch (bundle.callType) {
              case REQUEST:
                onRPCPaxosRequest(from, bundle.msg);
                break;
              case RESPONSE:
                onRPCPaxosResponse(from, bundle.msg);
                break;
            }
            break;
          default:
            Log.w(TAG, "Received invalid message type");
        }
      }
      catch(IllegalArgumentException e) {
        Log.w(TAG, "Data message could not be deserialized into valid RPCCallBundle");
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
    RPCCallBundle bundle = RPCCallBundle.deserialize(msg);
    return bundle.id;
  }

  /**
   * Returns the message type of the specified message.
   * @return a RPCCallType describing the type of message received
   */
  public static RPCCallType extractBundleType(byte[] msg) {
    RPCCallBundle bundle = RPCCallBundle.deserialize(msg);
    return bundle.callType;
  }
  
  /**
   * Called when another Paxos node has submitted a Paxos voting request
   * to this node.
   *
   * @param from
   *            The id of the node this request originated from.
   * @param message
   *            The message of the request.
   */
  public abstract void onRPCPaxosRequest(Integer from, RPCMsg message);

  /**
   * Called when another Paxos node is returning a reply to a Paxos voting
   * request made by this node.
   *
   * @param from
   *            The id of the node this response originated from.
   * @param message
   *            The message of the response.
   */
  public abstract void onRPCPaxosResponse(Integer from, RPCMsg message);
  
	/**
	 * Method that is called by the RPC layer when an RPC Request transaction is 
   * received.
   * Request transactions are RPC invocations on a remote node.
	 * 
	 * @param from
	 *            The address from which the message was received
   * @param message 
   *            The RPCMsg data message
	 */
  public abstract void onRPCCommitRequest(Integer from, RPCMsg message);

	/**
	 * Method that is called by the RPC layer when an RPC Response transaction is 
   * received.
   * Response transactions are replies to Request transactions.
	 * 
	 * @param from
	 *            The address from which the message was received
   * @param message 
   *            The RPCMsg data message
   *
	 */
  public abstract void onRPCCommitResponse(Integer from, RPCMsg message);



	//----------------------------------------------------------------------------
	// RPC Layer constructs (a.k.a. nested classes)
	//----------------------------------------------------------------------------

  /**
   * Enum to specify the type of message being sent over RPC.
   */
  protected static enum RPCMsgType {
    COMMIT, PAXOS;

    public static RPCMsgType fromInt(int value) {
      if(value == COMMIT.ordinal()) { return COMMIT; }
      else if(value == PAXOS.ordinal()) { return PAXOS; }
      else { return null; }
    }
  }

  /**
   * Enum to specify the RPC message type.
   */
  protected static enum RPCCallType {
    REQUEST, RESPONSE;

    public static RPCCallType fromInt(int value) {
      if(value == REQUEST.ordinal()) { return REQUEST; }
      else if(value == RESPONSE.ordinal()) { return RESPONSE; }
      else { return null; }
    }
  }

  /**
   * Some sweet ass class.
   */
  protected static class RPCCallBundle implements Serializable {
    public static final long serialVersionUID = 0L;

    public final int id;  // message id
    public final RPCCallType callType;  // request or response
    public final RPCMsgType msgType;  // the type of the message
    public final RPCMsg msg;  // the message to send

    /**
     * Wrapper around a file version list and a filesystem transaction.
     *
     * @param type
     *            Whether this is a REQUEST or RESPONSE RPC message
     * @param id
     *            The unique id of this bundle
     * @param msg
     *            The RPCMsg to send
     */
    RPCCallBundle(int id, RPCCallType callType, RPCMsgType msgType, RPCMsg msg) {
      this.id = id;
      this.callType = callType;
      this.msgType = msgType;
      this.msg = msg;
    }

    public static byte[] serialize(RPCCallBundle bundle) {
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
    
    public static RPCCallBundle deserialize(byte[] bytes) {
    	ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    	ObjectInput in = null;
    	try {
    	  in = new ObjectInputStream(bis);
    	  Object o = in.readObject(); 
    	  RPCCallBundle bundle = (RPCCallBundle) o;
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
      switch(callType) {
        case REQUEST:
          return String.format("RPCCallBundle{REQUEST, %d, %s}", id, msg);
        case RESPONSE:
          return String.format("RPCCallBundle{RESPONSE, %d, %s}", id, msg);
        default:
          return "RPCCallBundle{ invalid state }";
      }
    }
  }
}
