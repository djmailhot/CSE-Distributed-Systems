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

  // TODO: needs to be hardcoded somewhere
  private Set<Integer> servers;
  private PaxosManager paxos;

  public RPCNode() {
    super();
  }

  public void start() {
    super.start();
    PaxosManager paxos = this.new PaxosManager();
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
  private void RPCSendPaxosRequest(int destAddr, RPCMsg msg) {
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
  private void RPCSendPaxosResponse(int destAddr, RPCMsg msg) {
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
            onCommitMsgReceive(from, bundle);
          case PAXOS:
            onPaxosMsgReceive(from, bundle);
          default:
            LOG.warning("Received invalid message type");
        }
      }
      catch(IllegalArgumentException e) {
        LOG.warning("Data message could not be deserialized into valid RPCCallBundle");
      }
    } else {
      // no idea what to do
    }
  }

	/**
	 * Called when a new Commit message is recieved.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param bundle
	 *            The RPCCallBundle associated with the RPC call
	 */
  private void onCommitMsgReceive(Integer from, RPCCallBundle bundle) {
    // we simply pass the commit message up a layer
    RPCCallType callType = bundle.callType;
    switch (callType) {
      case REQUEST:
        // This is a request to commit

        // onRPCCommitRequest(from, bundle.msg);
        // NOTE: Nope, not going to call this method here.  
        // Instead, the PaxosManager will first use the paxos voting system
        // to determine when it is appropriate to pass the commit request 
        // up a layer .
        paxos.onUpdateAttempt(from, bundle.msg);
        break;
      case RESPONSE:
        // This is a response to a commit attempt
        onRPCCommitResponse(from, bundle.msg);
        break;
      default:
        LOG.warning("Received invalid message type");
    }
  }

	/**
	 * Called when a new Paxos message is recieved.
	 * 
	 * @param from
	 *            The address from which the message was received
	 * @param bundle
	 *            The RPCCallBundle associated with the RPC call
	 */
  private void onPaxosMsgReceive(Integer from, RPCCallBundle bundle) {
      RPCCallType callType = bundle.callType;
      switch (callType) {
        case REQUEST:
          paxos.onRequest(from, bundle.msg);
          break;
        case RESPONSE:
          paxos.onResponse(from, bundle.msg);
          break;
        default:
          LOG.warning("Received invalid message type");
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
    	  try {
      	  out.close();
					bos.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
    	  try {
					bis.close();
	    	  in.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
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


  //---------------------------------------------------------------------------
  //---------------------------------------------------------------------------
  //---------------------------------------------------------------------------
  //
  // *fanfare* PAXOS *fanfare*
  //
  //---------------------------------------------------------------------------
  //---------------------------------------------------------------------------
  //---------------------------------------------------------------------------


  /**
   * The PaxosManager maintains the PAXOS voting state machine.
   *
   * NOTE: Important to make this non-static such that the manager is able
   * to use resources of the outer RPCNode class instance.
   */
  private class PaxosManager {
    public static final String TAG = "PaxosManager";

    /**
     * Very useful resources to access available from the outer RPCNode class
     * instance.
     *
     * Set<Integer> servers
     *    The set of all server nodes.
     *
     * int addr
     *    The id number of this node.
     *
     * RPCSendPaxosRequest(to, msg)
     *    Call this to send a Paxos voting request to a specified node.
     *    (i.e. starting a proposal, send an update to learn)
     *
     * RPCSendPaxosResponse(to, msg)
     *    Call this to send a Paxos voting response to a specified node.
     *    (i.e. responding to a proposal, send the result of learning an update)
     *
     * onRPCCommitRequest(from, msg)
     *    Call this when this node has verified that an update has been 
     *    accepted and should be committed to this local node.
     *    This can be used by both the Proposer when it decides an proposal
     *    has been accepted, as well as by a Listener when it is told to
     *    learn an update.
     */



    // Most recent proposal number for this round

    // Map of round numbers to chosen proposal

    // CurrProposal number counter

    // PREPAREd proposal (but not chosen yet, waiting for ACCEPT request)

    // The last round for which the proposal was processed

    // Map of proposal to client who initiated


    PaxosManager() {
      // TODO: cool stuff, bro
    }

    /**
     * Called when an update needs to be serializable over this distributed
     * system.
     *
     * Will use the Paxos voting system to ensure this update is ordered
     * correctly.
     *
     * @param from
     *            The id of the node this update originated from.
     * @param updateMsg
     *            The message of the update.
     */
    public void onUpdateAttempt(int from, RPCMsg updateMsg) {
      // add this update message to the queue of messages

      // examples on how to put together a PaxosMsg
      int proposalNum = 0;
      int roundNum = 0;
      PaxosProposal proposal = new PaxosProposal(proposalNum, from, updateMsg);
      PaxosMsg msg = new PaxosMsg(PaxosMsgType.PREPARE, roundNum, proposal);
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
    public void onRequest(int from, RPCMsg message) {
      PaxosMsg msg = (PaxosMsg)message;
      PaxosMsgType type = msg.msgType;
      switch(type) {
        // TODO: Can rename these if you like
        case PREPARE:
        case LEARN:
        // case ACCEPT: should never get this
        // case REJECT: should never get this
        default:
          Log.w(TAG, "Invalid PaxosMsgType on a request");
      }
    }

    /**
     * Called when another Paxos node is returning a reply to a Paxos voting
     * request made by this node.
     *
     * @param from
     *            The id of the node this response originated from.
     * @param message
     *            The message of the response.
     */
    public void onResponse(int from, RPCMsg message) {
      PaxosMsg msg = (PaxosMsg)message;
      PaxosMsgType type = msg.msgType;
      switch(type) {
        // TODO: Can rename these if you like
        // case PREPARE: should never get this
        // case LEARN: should never get this
        case ACCEPT:
        case REJECT:
        default:
          Log.w(TAG, "Invalid PaxosMsgType on a response");
      }
    }

    //-------------------- Paxos State Machine --------------------------------
    
    // send of proposal
    private void sendPrepareRequest(/* args*/) {
      
    }  
    
    // recieve proposal
    private void receivePrepareRequest(/* args*/) {
      // Do stuff
      sendPrepareResponse();
    }
    
    //
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
    
    // receive the agreed value
    private void sendLearnValueRequest()	 {
      
    }
    
    // receive the value
    private void receiveLearnValueResponse() {
      
    }
    //-------------------- Paxos Methods End ---------------------------------//

  }

  //-------------------------------------------------------------------------
  // Paxos system constructs
  //-------------------------------------------------------------------------

  /**
   * Enum to specify the Paxos message type.
   */
  private static enum PaxosMsgType {
    PREPARE, LEARN, ACCEPT, REJECT;

    public static PaxosMsgType fromInt(int value) {
      if(value == PREPARE.ordinal()) { return PREPARE; }
      else if(value == LEARN.ordinal()) { return LEARN; }
      else if(value == ACCEPT.ordinal()) { return ACCEPT; }
      else if(value == REJECT.ordinal()) { return REJECT; }
      else { return null; }
    }
  }

  /**
   * An RPC message for the Paxos system.
   */
  private static class PaxosMsg implements RPCMsg {
    public static final long serialVersionUID = 0L;

    // msg type
    public final PaxosMsgType msgType;
    // round number
    public final int roundNum;
    // Paxos proposal
    public final PaxosProposal proposal;

    public PaxosMsg(PaxosMsgType msgType, int roundNum, PaxosProposal proposal) {
      this.msgType = msgType;
      this.roundNum = roundNum;
      this.proposal = proposal;
    }

    /**
     * Returns an unique id that is shared between Request and Response
     * messages.
     */
    public int getId() {
      return proposal.proposalNum;
    }
  }

  /**
   * A update proposal to voted on by the Paxos system.
   */
  private static class PaxosProposal {
    // proposal number:  proposalnum = counter * numservers + uniqueserverid
    public final int proposalNum;
    // origin server number
    public final int clientId;
    // the update's message
    public final RPCMsg updateMsg;

    PaxosProposal(int proposalNum, int clientId, RPCMsg updateMsg) {
      this.proposalNum = proposalNum;
      this.clientId = clientId;
      this.updateMsg = updateMsg;
    }
  }

}
