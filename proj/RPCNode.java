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
     * Very useful resources available from the outer RPCNode class instance.
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



    // The current round of voting
    private int currRound;

    // The highest proposal number yet seen
    // TODO: should this be mapped from the round number? opinion?
    private int maxSeqNum;
    
    // Proposer resource
    // The next proposal number for it to use
    private int currentProposalNum;
    
    // Proposer resource
    // The counter used to updated the currentProposalNum
    private int counter;

    // TODO: DAVID isn't this a proposer resource? So they can remember what they sent?
    // Acceptor resource
    // PREPAREd proposal (but not chosen yet, waiting for ACCEPT request)
    private PaxosProposal preparedProposal;
    
    // Proposer resource
    // The value of the highest-number proposal received from prepare responders
    // TODO this is also probably per round....
    private PaxosProposal highestNumberPrepareResponseProposal = null;

    // Proposer resource
    // Acceptors that responded with a PROMISE
    private Set<Integer> promisingAcceptors;
    
    // Acceptor resource
    // ACCEPTED proposal (unknown if it is chosen - accepted by anyone else)
    // Initially null when no proposal has been accepted
    // TODO: should this also be mapped from the round? Think on this. DAVID: thoughts?
    private PaxosProposal acceptedProposal = null;
    
    // Proposer resource
    // Acceptors that responded with an ACCEPT
    // When this is a majority, broadcast DECIDED information.
    private Set<Integer> acceptingAcceptors;
    
    // Acceptor resource
    // The proposal number that node has promised to not accept anything less than
    // TODO: I think this will probably be end up mapped from the round.
    private int promisedNum = -1;

    // Learner resource
    // Map of round numbers to chosen proposal
    private SortedMap<Integer, PaxosProposal> decidedProposals;



    PaxosManager() {
      // TODO: cool stuff, bro
      this.decidedProposals = new TreeMap<Integer, PaxosProposal>();
      this.maxSeqNum = 0;
      this.preparedProposal = null;
      this.currRound = 1;
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
      // This will bypass all in-progress Paxos code
      onRPCCommitRequest(from, updateMsg);

      // Actual code

      // add this update message to the queue of messages needing to be voted on

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
      Log.i(TAG, String.format("request from %d with %s", from, msg));

      PaxosMsgType type = msg.msgType;
      switch(type) {
        case PREPARE: // node told to PREPARE for a proposal
          receivePrepareRequest(from, msg);
        	
//          // if no previous promise or if the promise number is less than this msg
//          // then we can re-promise on this msg
//          if(preparedProposal == null ||
//             preparedProposal.proposalNum < msg.proposal.proposalNum) {
//
//            RPCSendPaxosResponse(from, new PaxosMsg(PaxosMsgType.PROMISE, 
//                                                    currRound, msg.proposal));
//            preparedProposal = msg.proposal;
//          } // else ignore
//          break;

        // case PROMISE: // should never get this
        case ACCEPT: // node directed to ACCEPT a new value

          // only accept of the proposal matches our promised proposal
          if(msg.proposal.equals(preparedProposal)) {
            RPCSendPaxosResponse(from, new PaxosMsg(PaxosMsgType.DECIDED,
                                                    currRound, msg.proposal));
          } // else ignore
          break;
          
        case DECIDED: // node notified that a new value has been DECIDED upon

          PaxosProposal decidedProposal = decidedProposals.get(msg.roundNum);
          // only proceed if we haven't already committed the proposal for
          // this round
          if(decidedProposal == null) {
            // pass the new value up a layer to be formally committed locally
            onRPCCommitRequest(from, msg.proposal.updateMsg);
            decidedProposals.put(msg.roundNum, msg.proposal);
          } // else ignore
          break;
          
        case UPDATE: // node queried to provide all updates committed since the
                     // attached proposal
                     // NOTE: this is how a node will update state after a restart
        default:
          Log.w(TAG, "Invalid PaxosMsgType on a request");
      }

      maxSeqNum = Math.max(maxSeqNum, msg.proposal.proposalNum);
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
      Log.i(TAG, String.format("response from %d with %s", from, msg));

      PaxosMsgType type = msg.msgType;
      switch(type) {
        // case PREPARE: should never get this
        case PROMISE: // node heard back with a PROMISE in response to a PREPARE

          // TODO: How to distinguish PROMISEs from different voting rounds?
        	// TODO: How to tell if the response is promising or rejecting the promise???
        	receivePrepareResponse(from, msg);
        	
//          promisingAcceptors.add(from);
//          // if we have a majority of ACCEPTS
//          if(promisingAcceptors.size() > servers.size() / 2) {
//            // send an ACCEPT request to all other servers
//            for(Integer address : servers) {
//              // except myself
//              if(address != addr) {
//                RPCSendPaxosRequest(address, 
//                                    new PaxosMsg(PaxosMsgType.ACCEPT,
//                                                 currRound, msg.proposal));
//              }
//            }
//          } // else wait for more promises
//          break;
          
        // case ACCEPT: should never get this
        // case DECIDED: should never get this
        case UPDATE: // node receiving a catch-up proposal update
                     // NOTE: this is how a node will update state after a restart
        default:
          Log.w(TAG, "Invalid PaxosMsgType on a response");
      }
      maxSeqNum = Math.max(maxSeqNum, msg.proposal.proposalNum);
    }

    //-------------------- Paxos State Machine --------------------------------
    
    private void updateCurrentProposalNumber() {
    	currentProposalNum = counter * servers.size() + addr;  /// TODO: ASSUMES N servers numbered 0 to N-1  !!!!
    	counter++;
    }
    
    // send of proposal
    private void sendPrepareRequest(RPCMsg rpcmsg) {    	
    	
    	updateCurrentProposalNumber();
    	PaxosProposal preparedProposal = new PaxosProposal(currentProposalNum, addr, rpcmsg);
    	for(int address: servers) {
        // except myself
        if(address != addr) {
	    		RPCSendPaxosResponse(address, new PaxosMsg(PaxosMsgType.PROMISE, 
	          currRound, preparedProposal));
        }
    	}
    }  
    
    //
    private void sendPrepareResponse(int from, boolean promised) {

    	// TODO: DAVID!! IMPORTANT!!! I want to also sent back
    	// Promised/Rejected
    	// The current accepted proposal value (includes proposal num) (could be null)
      RPCSendPaxosResponse(from, new PaxosMsg(PaxosMsgType.PROMISE, 
                                              currRound, acceptedProposal));
    }
    
    // recieve proposal
    private void receivePrepareRequest(int from, PaxosMsg msg) {
    	// Do stuff
	  	if (msg.proposal.proposalNum < promisedNum) {
	  		// Reject
	  		sendPrepareResponse(from, false);
	  	} else if (msg.proposal.proposalNum > promisedNum) {
	  		// Promise
  	 		promisedNum = msg.proposal.proposalNum;
  	    sendPrepareResponse(from, true);
	  	} else { 
	  		// do nothing for duplicates
	  	}
    }
    
    // recieve proposal
    private void receivePrepareResponse(int from, PaxosMsg msg) {
    	if (msg.proposal != null) { // it has already accepted some sort of proposal
    		if (highestNumberPrepareResponseProposal == null) {
    			highestNumberPrepareResponseProposal = msg.proposal;
    		} else if (highestNumberPrepareResponseProposal.proposalNum < msg.proposal.proposalNum) {
    			highestNumberPrepareResponseProposal = msg.proposal;
    		}
    	}
      if (true) { // TODO: accepted
	      promisingAcceptors.add(from);
	      // if we have a majority of ACCEPTS
	      if(promisingAcceptors.size() > servers.size() / 2) {
	        // send an ACCEPT request to all other servers
	      	sendAcceptRequest();
	      } // else wait for more promises
      } else { // TODO: rejected
      	
      	// TODO Not sure what to do here for rejects... ??
      }
    	
    }

    // send accept request to all servers
    private void sendAcceptRequest() {
    	
    	PaxosProposal toSend;
    	if (highestNumberPrepareResponseProposal != null) {
    		// TODO: DAVID should the client be my address?? or the client that originally sent the transaction?
    		toSend = new PaxosProposal(currentProposalNum, addr, highestNumberPrepareResponseProposal.updateMsg);
    	} else {
    		toSend = preparedProposal;
    	}
      for(Integer address : servers) {
        // except myself
        if(address != addr) {
          RPCSendPaxosRequest(address, 
                              new PaxosMsg(PaxosMsgType.ACCEPT,
                                           currRound, toSend));
        }
      }
    }
    
    private void receiveAcceptRequest(/* args */) {
      // Do stuff
      sendAcceptRepsonse();
    }
    
    private void sendAcceptRepsonse() {
      
    }
    
    private void receiveAcceptResponse() {
    	
    }
    
    // receive the agreed value
    private void sendLearnValueRequest()	 {
      
    }
    
    // receive the value
    private void receiveLearnValueResponse() {
      
    }
    //-------------------- Paxos Methods End ---------------------------------//


    /**
     * Return the next proposal sequence number for this node based on the
     * specified max sequence number seen.
     */
    private int nextSeqNum(int maxSeqNum) {
      // TODO: get this number from a real source
      int numServers = 5;
      return ((maxSeqNum / numServers) * numServers) + addr;
    }

  }

  //-------------------------------------------------------------------------
  // Paxos system constructs
  //-------------------------------------------------------------------------

  /**
   * Enum to specify the Paxos message type.
   */
  private static enum PaxosMsgType {
    PREPARE("PREPARE"),
    PROMISE("PROMISE"),
    ACCEPT("ACCEPT"),
    DECIDED("DECIDED"),
    UPDATE("UPDATE");

    private String value;

    private PaxosMsgType(String value) {
      this.value = value;
    }

    public static PaxosMsgType fromString(String value) {
      if(value.equals(PREPARE.value)) { return PREPARE; }
      else if(value.equals(PROMISE.value)) { return PROMISE; }
      else if(value.equals(ACCEPT.value)) { return ACCEPT; }
      else if(value.equals(DECIDED.value)) { return DECIDED; }
      else if(value.equals(UPDATE.value)) { return UPDATE; }
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

    public boolean equals(Object o) {
      if(!(o instanceof PaxosMsg)) {
        return false;
      }
      PaxosMsg p = (PaxosMsg)o;

      return getId() == p.getId();
    }

    public String toString() {
      return String.format("PaxosMsg{%s, round %d, %s}",
                            msgType, roundNum, proposal);
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

    public boolean equals(Object o) {
      if(!(o instanceof PaxosProposal)) {
        return false;
      }
      PaxosProposal p = (PaxosProposal)o;
      
      // TODO: DAVID, I don't think we can include proposalNum in equals
      // proposal equality depends only on the <value> of the proposal, not the number.
      // If 3/5 nodes have accepted 3 proposals with different numbers, but the same <value>, 
      // then that value is still chosen. 
      return proposalNum == p.proposalNum && 
             clientId == p.clientId &&
             updateMsg.getId() == p.updateMsg.getId();
    }

    public String toString() {
      return String.format("PaxosProposal{num %d, client %d, %s}", 
                            proposalNum, clientId, updateMsg);
    }
  }

}
