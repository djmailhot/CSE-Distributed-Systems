import edu.washington.cs.cse490h.lib.Utility;
import edu.washington.cs.cse490h.lib.ServerList;
import edu.washington.cs.cse490h.lib.Node.NodeCrashException;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.logging.Logger;

import plume.Pair;

/**
 * Extension to the RPCNode class that adds support for ensuring a serial 
 * ordering of RPC messages.
 *
 * The PaxosNode maintains the PAXOS voting state machine.
 */
public abstract class PaxosNode extends RPCNode {
  public static final String TAG = "PaxosNode";

  //---------------------------------------------------------------------------
  //---------------------------------------------------------------------------
  //---------------------------------------------------------------------------
  //
  // *fanfare* PAXOS *fanfare*
  //
  //---------------------------------------------------------------------------
  //---------------------------------------------------------------------------
  //---------------------------------------------------------------------------

  /*
   * Very useful resources to use.
   *
   * RPCSendPaxosRequest(to, msg)
   *    Call this to send a Paxos voting request to a specified node.
   *    (i.e. starting a proposal, send an update to learn)
   *
   * RPCSendPaxosResponse(to, msg)
   *    Call this to send a Paxos voting response to a specified node.
   *    (i.e. responding to a proposal, send the result of learning an update)
   *
   * onCommitRequest(from, msg)
   *    Call this when this node has verified that an update has been 
   *    accepted and should be committed to this local node.
   *    This can be used by both the Proposer when it decides an proposal
   *    has been accepted, as well as by a Learner when it is told to
   *    learn an update.
   */

  // The current round of voting
  private int currRound;
    
  // Learner resource
  // Map of round numbers to chosen proposal
  private SortedMap<Integer, RPCMsg> decidedUpdates;



  private SortedMap<Integer, PaxosInstance> instances;

  private class PaxosInstance {
    public final Proposer proposer;
    public final Acceptor acceptor;
    public final Learner learner;

    PaxosInstance() {
      proposer = new Proposer();
      acceptor = new Acceptor();
      learner = new Learner();
    }
  }


  // Concurrency constructs for buffering updates
  private final Lock queueLock;
  private final Condition newUpdate;
  // queue of updates yet to be sent with corresponding client addresses
  private final Deque<Pair<Integer, RPCMsg>> queuedUpdates;

  PaxosNode() {
    this.instances = new TreeMap<Integer, PaxosInstance>();
    this.decidedUpdates = new TreeMap<Integer, RPCMsg>();


    // keep a queue of updates yet to be sent
    this.queueLock = new ReentrantLock();
    this.newUpdate = queueLock.newCondition();
    this.queuedUpdates = new LinkedList<Pair<Integer, RPCMsg>>();

    Thread updateConsumer = new Thread(new Runnable() {
      public void run() {
        queueLock.lock();
        try {
          while(true) {
            newUpdate.await();
            // if nothing to broadcast or in the middle of proposing, wait
            if(queuedUpdates.isEmpty()) {
              continue;
            }

            // get the next update
            Pair<Integer, RPCMsg> nextUpdate = queuedUpdates.poll();

            // get the next round to spinup.  For any instance we already have
            // spun up, we know for sure there is at least one Proposer for
            // that round because a Proposer always starts a new round.
            int nextRound = instances.lastKey() + 1;

            // spin up a new paxos instance
            PaxosInstance instance = spinupInstance(nextRound);

            // have the Proposer make a new proposal
            instance.proposer.broadcastPrepareRequest(nextUpdate.a, nextUpdate.b);
          }

        } catch(InterruptedException e) {
          e.printStackTrace();
          Log.w(TAG, "Interrupted while attempting to pull the next update");
        } finally {
          queueLock.unlock();
        }
      }
    });
    updateConsumer.start();
  }


  /**
   * Spin up a Paxos instance for the specified round if it doesn't already
   * exist.
   */
  private PaxosInstance spinupInstance(int roundNum) {
    PaxosInstance instance = instances.get(roundNum);
    if(instance == null) {
      instance = new PaxosInstance();
      instances.put(roundNum, instance);
    }
    return instance;
  }

  /**
   * Spin down a Paxos instance for the specified round.
   */
  private void spindownInstance(int roundNum) {
    instances.remove(roundNum);
  }

  /**
   * Will ensure that a commit is serializable over this distributed
   * system.
   *
   * Intercepts a commit request and first uses the Paxos voting system 
   * to ensure this update is ordered correctly.
   */
  @Override
  public void onRPCCommitRequest(Integer from, RPCMsg message) {
    // TODO: This will bypass all in-progress Paxos code
    onCommitRequest(from, message);

    // ACTUAL CODE
    queueLock.lock();
    try {
      // put it on the queue
      queuedUpdates.addLast(new Pair<Integer, RPCMsg>(from, message));
      // signal that there is a new update to propose
      newUpdate.signal();
    } finally {
      queueLock.unlock();
    }
  }

	/**
	 * Method that is called by the Paxos layer when an RPC Requent is ready to be
   * processed.
   * 
   * Request messages are RPC invocations on a remote node.
	 * 
	 * @param from
	 *            The address from which the message was received
   * @param message 
   *            The RPCMsg data message
	 */
  public abstract void onCommitRequest(Integer from, RPCMsg message);

  @Override
  public void onRPCCommitResponse(Integer from, RPCMsg message) {
    // don't intercept commit responses
    onCommitResponse(from, message);
  }

	/**
	 * Method that is called by the Paxos layer when an RPC Response transaction is 
   * received.
   *
   * Response messages are replies to Request messages.
	 * 
	 * @param from
	 *            The address from which the message was received
   * @param message 
   *            The RPCMsg data message
	 */
  public abstract void onCommitResponse(Integer from, RPCMsg message);

  /**
   * Called when another Paxos node has submitted a Paxos voting request
   * to this node.
   *
   * @param from
   *            The id of the node this request originated from.
   * @param message
   *            The message of the request.
   */
  public void onRPCPaxosRequest(Integer from, RPCMsg message) {
    PaxosMsg msg = (PaxosMsg)message;
    Log.i(TAG, String.format("request from %d with %s", from, msg));

    if(decidedUpdates.containsKey(msg.roundNum)) {
      // this is an old message for an old round, so let the node know
      // that there are updates it needs to catch up on.
      // TODO: catch up the other node
      return;
    }

    PaxosInstance instance = spinupInstance(msg.roundNum);

    PaxosMsgType type = msg.msgType;
    switch(type) {
      case PROPOSER_PREPARE: // Acceptor told to PREPARE for a proposal
        instance.acceptor.receivePrepareRequest(from, msg);
        break;

      case PROPOSER_ACCEPT: // Acceptor directed to ACCEPT a new value
        instance.acceptor.receiveAcceptRequest(from, msg);
        break;
        
      // case ACCEPTOR_PROMISE: // should never get this
      // case ACCEPTOR_REJECT: // should never get this
      case ACCEPTOR_ACCEPTED: // Learner notifed that an Acceptor ACCEPTED
        instance.learner.receiveLearnRequest(from, msg);
        break;

      case LEARNER_DECIDED: // notified that a new value has been DECIDED upon
        // log the decided value
        decidedUpdates.put(msg.roundNum, msg.proposal.updateMsg);
        // spin down the Paxos instance
        spindownInstance(msg.roundNum);
      case UPDATE: // Learner queried to provide all updates committed since the
                   // attached proposal
                   // NOTE: this is how a node will update state after a restart
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
  public void onRPCPaxosResponse(Integer from, RPCMsg message) {
    PaxosMsg msg = (PaxosMsg)message;
    Log.i(TAG, String.format("response from %d with %s", from, msg));

    PaxosInstance instance = spinupInstance(msg.roundNum);

    PaxosMsgType type = msg.msgType;
    switch(type) {
      // case PROPOSER_PREPARE: // should never get this
      // case PROPOSER_ACCEPT: // should never get this
      case ACCEPTOR_PROMISE: // Proposer heard back with a PROMISE in response to a PREPARE
        // TODO: How to distinguish PROMISEs from different voting rounds?
        instance.proposer.receivePrepareResponse(from, msg, true);
        break;
      case ACCEPTOR_IGNORE: // Proposer heard back with a REJECT in response to a PREPARE
        instance.proposer.receivePrepareResponse(from, msg, false);
        break;
        
      // case ACCEPTOR_ACCEPTED: // should never get this
      // case ACCEPTOR_IGNORE: // should never get this
      // case LEARNER_DECIDED: // should never get this
      case UPDATE: // node receiving a catch-up proposal update
                   // NOTE: this is how a node will update state after a restart
      default:
        Log.w(TAG, "Invalid PaxosMsgType on a response");
    }
  }

  //-------------------- Paxos State Machine --------------------------------
  

  //////////////////////////////////////////////////////////////////////////
  // Paxos Proposer 
  //////////////////////////////////////////////////////////////////////////

  private class Proposer {

    // The maximum accepted proposal number seen by this node
    private int maxProposalNum;

    // PREPAREd proposal (but not chosen yet, waiting for ACCEPT requests)
    private PaxosProposal currProposal;
    
    // Acceptors that responded with an ACCEPT
    // When this is a majority, broadcast LEARNER_DECIDED information.
    private Set<Integer> promisingAcceptors;
    
    // Acceptors that responded with an rejecting ACCEPT
    // When this is a majority, propose something else
    private Set<Integer> rejectingAcceptors;
    

    Proposer() {
      this.maxProposalNum = 0;
      this.currProposal = null;
      this.promisingAcceptors = new HashSet<Integer>();
      this.rejectingAcceptors = new HashSet<Integer>();
    }

    public void reset() {
      this.currProposal = null;
      this.promisingAcceptors.clear();
      this.rejectingAcceptors.clear();
    }

    public boolean isProposing() {
      return currProposal != null;
    }

    /**
     * Return the next proposal number for this node based on the
     * specified max proposal number seen.
     *
     * The new proposal number is guaranteed to be higher than the passed in
     * max number.
     *
     * @modifies maxProposalNum 
     *                   if this new proposal number is higher than the current
     *                   max proposal number, then maxProposalNum is updated.
     */
    private int nextProposalNum(int maxProposalNum) {
      int numServers = ServerList.serverNodes.size();
      // The +1 is to ensure that our math works out with 0-indexed server
      // addresses.  We want to ensure that the proposal number will always
      // grow and not be stuck at a specific number due to this calculation.
      int count = (int)Math.ceil((double)(maxProposalNum + 1) / (double)numServers);
      int nextNum = (count * numServers) + addr;

      assert(nextNum > maxProposalNum); // sanity check

      // update the max proposal number to be this new proposal number... maybe
      maxProposalNum = Math.max(nextNum, maxProposalNum);
      return nextNum;
    }


    // broadcast of proposal to all Acceptors
    public void broadcastPrepareRequest(int from, RPCMsg updateMsg) {
      int proposalNum = nextProposalNum(maxProposalNum);
      PaxosProposal proposal = new PaxosProposal(proposalNum, from, updateMsg);
      currProposal = proposal;

      PaxosProposal prepareProposal = new PaxosProposal(proposalNum);
      for(int address: ServerList.serverNodes) {
        // send it even to myself
        // only send the proposal num
        RPCSendPaxosRequest(address, 
                new PaxosMsg(PaxosMsgType.PROPOSER_PREPARE, 
                             currRound, prepareProposal));
      }
    }

    // recieve prepare results
    public void receivePrepareResponse(int from, PaxosMsg msg, boolean accepted) {
    	// it has already accepted some sort of proposal
    	// we need to keep the value consistent.
    	if (msg.proposal != null) {
    		if (maxProposalNum < msg.proposal.proposalNum) {
    			maxProposalNum = msg.proposal.proposalNum;
      		int proposalNum = nextProposalNum(maxProposalNum);
    			currProposal = new PaxosProposal(proposalNum, currProposal.clientId, msg.proposal.updateMsg);
    		}
    	}

      if(msg.proposal.proposalNum == currProposal.proposalNum) {
        // if this message is for the current proposal
        if(accepted) {
          promisingAcceptors.add(from);
          if(promisingAcceptors.size() > ServerList.serverNodes.size() / 2) {
            // send an ACCEPT request to all other servers
            broadcastAcceptRequests(currProposal);
          } // else wait for more promises
        } else {
          rejectingAcceptors.add(from);
          if(rejectingAcceptors.size() > ServerList.serverNodes.size() / 2) {
            // abort the proposal and re-propose
            queueLock.lock();
            try {
              // put back in the front of the queue
              queuedUpdates.addFirst(
                          new Pair<Integer, RPCMsg>(currProposal.clientId,
                                                    currProposal.updateMsg));
              // forget the old proposal
              currProposal = null;

              // signal that there is a new update to propose
              newUpdate.signal();
            } finally {
              queueLock.unlock();
            }
          } // else wait for more rejects
        }
      } else if(msg.proposal.proposalNum > currProposal.proposalNum) {
        // else, this message was for a more current proposal
        // TODO: do something?

      } // else, this message was for an older proposal
      
    }


    // send accept requests to all servers
    private void broadcastAcceptRequests(PaxosProposal proposal) {
      // Send Accepts to the majority of nodes that promised.
      for(int address: promisingAcceptors) {
        // send even to myself
        RPCSendPaxosRequest(address, 
                new PaxosMsg(PaxosMsgType.PROPOSER_ACCEPT,
                             currRound, proposal));
      }
    }
      

    // TODO:  Shouldn't the Learner deal with this?
    // get accepted responses from acceptors
    public void receiveAcceptIgnoreResponse(int from, PaxosMsg msg) {
    	// TODO: ask the Paxos mangager to try for another round.          
    }
  }

  //////////////////////////////////////////////////////////////////////////
  // Paxos Acceptor
  //////////////////////////////////////////////////////////////////////////

  private class Acceptor {

    // The proposal number that this node has promised to not accept anything
    // less than
    private int promisedNum;

    // ACCEPTED proposal (unknown if it is chosen - accepted by anyone else)
    // Initially null when no proposal has been accepted
    // TODO: Don't think we actually need this...
    // TODO: I think we do, because when we reject a promise, we have to respond
    // with the highest proposal number that we have promised AND the value of the proposal that we
    // have previously accepted. Then the proposer is constrainted to propose with a higher number AND a consistent value.
    private PaxosProposal acceptedProposal;

    Acceptor() {
      this.promisedNum = -1;
      this.acceptedProposal = null;
    }
    
    /**
     * This Acceptor received a request to prepare for a proposal.
     */
    public void receivePrepareRequest(int from, PaxosMsg msg) {
    	RPCMsg value = acceptedProposal == null ? null : acceptedProposal.updateMsg;
      if (msg.proposal.proposalNum < promisedNum) {
        // Reject
        // Send a response proposal with the current promise number
      	
      	// The accepted proposal number may not match the highest promised number... 
      	// in the case that there have been other promise requests after the first accept
      	PaxosProposal toSend = new PaxosProposal(promisedNum, acceptedProposal.clientId, value);
      	PaxosMsg sendMsg = new PaxosMsg(PaxosMsgType.ACCEPTOR_IGNORE, msg.roundNum, acceptedProposal);
        RPCSendPaxosResponse(from, sendMsg);
                //new PaxosMsg(msg, PaxosMsgType.ACCEPTOR_REJECT, promisedNum));
      } else if (msg.proposal.proposalNum > promisedNum) {
        // Promise
        promisedNum = msg.proposal.proposalNum;
      	PaxosProposal toSend = new PaxosProposal(promisedNum, acceptedProposal.clientId, value);
      	PaxosMsg sendMsg = new PaxosMsg(PaxosMsgType.ACCEPTOR_PROMISE, msg.roundNum, acceptedProposal);
        RPCSendPaxosResponse(from, sendMsg);
                //new PaxosMsg(msg, PaxosMsgType.ACCEPTOR_PROMISE, promisedNum));
      } else { 
        // do nothing for duplicates
      }
    }


    /**
     * This Acceptor received a request to accept a proposal.
     */
    public void receiveAcceptRequest(int from, PaxosMsg msg) {
      if (msg.proposal.proposalNum == promisedNum) {
      	// TODO: Should this be greater-than-or-equal?
      	// TODO: Can greater-than even ever happen? Maybe not....
        // if the accept request is for the proposal we promised
        // broadcast to all Learners that we've Accepted a value
        for(int address: ServerList.serverNodes) {
          RPCSendPaxosRequest(address, 
                  new PaxosMsg(msg, PaxosMsgType.ACCEPTOR_ACCEPTED));
        }
      } else {
        // TODO: don't do anything?
        // TODO: DAVID!! IMPORTANT!!! I want to also sent back
        // Accepted/Rejected
        // The current accepted proposal value (includes proposal num) (could be null)
      	
      	
      	// TODO: do anything else?
      	RPCSendPaxosResponse(from, 
      												new PaxosMsg(msg, PaxosMsgType.ACCEPTOR_IGNORE));
      }
    }

  }

  //////////////////////////////////////////////////////////////////////////
  // Paxos Learner
  //////////////////////////////////////////////////////////////////////////

  private class Learner {

    // Learner that responded with an ACCEPTED
    // When this is a majority, broadcast LEARNER_DECIDED information.
    private Map<PaxosProposal, Set<Integer>> acceptedAcceptors;
    
    // Learner resource
    // Map of round numbers to chosen proposal
    private SortedMap<Integer, PaxosProposal> decidedProposals;

    Learner() {
      decidedProposals = new TreeMap<Integer, PaxosProposal>();
    }
    
    // receive the agreed value
    public void receiveAcceptorAcceptedRequest(int from, PaxosMsg msg) {
      if(acceptedAcceptors.containsKey(msg.proposal)) {
      	acceptedAcceptors.get(msg.proposal.updateMsg).add(from);
      } else {
      	Set<Integer> acceptors = new HashSet<Integer>();
      	acceptors.add(from);
      	acceptedAcceptors.put(msg.proposal, acceptors);
      }
      if(acceptedAcceptors.get(msg.proposal).size() > ServerList.serverNodes.size() / 2) {
      	// broadcast decided
      	broadcastDecidedRequests(msg.proposal);
      }
    }    
    
    // send accept requests to all servers
    private void broadcastDecidedRequests(PaxosProposal proposal) {
      for(int address: ServerList.serverNodes) {
        // send even to myself
        RPCSendPaxosRequest(address, 
                new PaxosMsg(PaxosMsgType.LEARNER_DECIDED,
                             currRound, proposal));
      }
    }
    
    // receive the value
    private void receiveDecidedRequest() {
      // TODO: DAVID, how do I bubble it up to the PaxosManager?
    }
  }
  //-------------------- Paxos Methods End ---------------------------------//


  //-------------------------------------------------------------------------
  // Paxos system constructs
  //-------------------------------------------------------------------------

  /**
   * Enum to specify the Paxos message type.
   */
  private static enum PaxosMsgType {
    PROPOSER_PREPARE("PROPOSER_PREPARE"),
    PROPOSER_ACCEPT("PROPOSER_ACCEPT"),
    ACCEPTOR_PROMISE("ACCEPTOR_PROMISE"),
    ACCEPTOR_IGNORE("ACCEPTOR_IGNORE"),
    ACCEPTOR_ACCEPTED("ACCEPTOR_ACCEPTED"),
    LEARNER_DECIDED("LEARNER_DECIDED"),
    UPDATE("UPDATE");

    private String value;

    private PaxosMsgType(String value) {
      this.value = value;
    }

    public static PaxosMsgType fromString(String value) {
      if(value.equals(PROPOSER_PREPARE.value)) { return PROPOSER_PREPARE; }
      else if(value.equals(PROPOSER_ACCEPT.value)) { return PROPOSER_ACCEPT; }
      else if(value.equals(ACCEPTOR_PROMISE.value)) { return ACCEPTOR_PROMISE; }
      else if(value.equals(ACCEPTOR_IGNORE.value)) { return ACCEPTOR_IGNORE; }
      else if(value.equals(ACCEPTOR_ACCEPTED.value)) { return ACCEPTOR_ACCEPTED; }
      else if(value.equals(LEARNER_DECIDED.value)) { return LEARNER_DECIDED; }
      else if(value.equals(UPDATE.value)) { return UPDATE; }
      else { return null; }
    }
  }

  /**
   * An RPC message for the Paxos system.
   */
  private static class PaxosMsg implements RPCMsg {
    public static final long serialVersionUID = 0L;

    public final int id;
    // msg type
    public final PaxosMsgType msgType;
    // round number
    public final int roundNum;
    // Paxos proposal
    public final PaxosProposal proposal;

    private PaxosMsg(int id, PaxosMsgType msgType, int roundNum,
                     PaxosProposal proposal) {
      this.id = id;
      this.msgType = msgType;
      this.roundNum = roundNum;
      this.proposal = proposal;
    }

    PaxosMsg(PaxosMsgType msgType, int roundNum, PaxosProposal proposal) {
      this(Math.abs(Utility.getRNG().nextInt()), msgType, 
           roundNum, proposal);
    }

    PaxosMsg(PaxosMsg originalRequest, PaxosMsgType msgType) {
      this(originalRequest.id, msgType, 
           originalRequest.roundNum, originalRequest.proposal);
    }

    PaxosMsg(PaxosMsg originalRequest, PaxosMsgType msgType,
             PaxosProposal newProposal) {
      this(originalRequest.id, msgType, 
           originalRequest.roundNum, newProposal);
    }

    /**
     * Returns an unique id that is shared between Request and Response
     * messages.
     */
    public int getId() {
      return id;
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

    PaxosProposal(int proposalNum) {
      this(proposalNum, -1, null);
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
      // TODO: STEPH, what about multiple identical updates submitted?  I try
      // to append the same value to the same file multiple times?
      // TODO: DAVID, can you use the updateMsg.getId()?
      // or check for it in the map (round->chosenValue)      
      return //proposalNum == p.proposalNum && 
             clientId == p.clientId &&
             updateMsg.getId() == p.updateMsg.getId();
    }
    
    @Override
    public int hashCode() {
    	return updateMsg.getId()*17 + clientId;
    }

    public String toString() {
      return String.format("PaxosProposal{num %d, client %d, %s}", 
                            proposalNum, clientId, updateMsg);
    }
  }
}
