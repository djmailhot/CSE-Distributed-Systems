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


  private static final String PAXOSFILE = "PAXOSFILE";
  private static final String INDICATOR_DECIDED = "D:\t";
  private static final String INDICATOR_SUBMITTED = "S:\t";
  private static final String PAXOSFILE_DELIMITER = "\t";

  private String TAG;

  // Other things

  protected final NFSService nfsService;
  private final Set<Integer> receivedRequestMsgIds;
  private final Set<Integer> receivedResponseMsgIds;


  // Persistently logged state of the Paxos system

  // The last round who's update was processed (i.e. passed up a layer)
  // Updates should be applied in order
  private int lastProcessedRound; 
  // Map of round numbers to chosen proposal
  private SortedMap<Integer, Pair<Integer, RPCMsg>> decidedUpdates;


  // Tracking the Paxos State Machine

  // Map of round numbers to Instances of Paxos managing that round
  private SortedMap<Integer, PaxosInstance> instances;

  PaxosNode() {
    super();

    this.nfsService = new NFSService(this);
    this.receivedRequestMsgIds = new HashSet<Integer>();
    this.receivedResponseMsgIds = new HashSet<Integer>();

    this.lastProcessedRound = -1;
    this.decidedUpdates = new TreeMap<Integer, Pair<Integer, RPCMsg>>();
    this.instances = new TreeMap<Integer, PaxosInstance>();
  }

  public void start() {
    this.TAG = String.format("PaxosNode.%d", addr);

    receivedRequestMsgIds.clear();
    receivedResponseMsgIds.clear();

    lastProcessedRound = -1;
    decidedUpdates.clear();
    instances.clear();

    try {
      if(nfsService.exists(PAXOSFILE)) {
        // We know we are recovering!

        List<String> lines = nfsService.read(PAXOSFILE);

        lastProcessedRound = Integer.parseInt(lines.get(0));

        // Query all other nodes for updates
        for(Integer address : ServerList.serverNodes) {
          if(address != addr) {
            RPCSendPaxosRequest(address, new PaxosMsg(PaxosMsgType.UPDATE, lastProcessedRound, null));
          }
        }

      }
    } catch(IOException e) {
      e.printStackTrace();
      throw new IllegalStateException("Paxosfile read failure");
    } catch(IndexOutOfBoundsException e) {
      throw new IllegalStateException("Paxosfile format corrupted");
    } catch(NumberFormatException e) {
      throw new IllegalStateException("Paxosfile format corrupted");
    }

    Log.i(TAG, String.format("read in PAXOSFILE, setting to round %d",
                             lastProcessedRound));

    super.start();
  }


  private void writeState() throws IOException {
    Log.i(TAG, "PAXOSFILE commit attempt");
    StringBuilder data = new StringBuilder();

    data.append(String.format("%d\n", lastProcessedRound));
/*
    for(Integer roundNum : decidedUpdates.keySet()) {
      Pair<Integer, RPCMsg> update = decidedUpdates.get(roundNum);

      data.append(String.format("%s%d%s%d\n", INDICATOR_DECIDED, roundNum,
                                PAXOSFILE_DELIMITER, update.a));

      byte[] bytes = RPCMsg.serialize(update.b);
      data.append(String.format("%s\n", Arrays.toString(bytes)));
    }
    for(Integer roundNum : submittedUpdates.keySet()) {
      Pair<Integer, RPCMsg> update = submittedUpdates.get(roundNum);

      data.append(String.format("%s%d%s%d\n", INDICATOR_SUBMITTED, roundNum,
                                PAXOSFILE_DELIMITER, update.a));

      byte[] bytes = RPCMsg.serialize(update.b);
      data.append(String.format("%s\n", Arrays.toString(bytes)));
    }
    */

    nfsService.write(PAXOSFILE, data.toString());
  }


  //////////////////////////////////////////////////////////////////////////////
  // Paxos node external API
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Will ensure that a commit is serializable over this distributed
   * system.
   *
   * Intercepts a commit request and first uses the Paxos voting system 
   * to ensure this update is ordered correctly.
   */
  @Override
  public void onRPCCommitRequest(Integer from, RPCMsg message) {
  	// Does this keep getting called?
  	System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! " + from + " " + message);
    int msgId = message.getId();
    Log.d(TAG, String.format("already received requests %s", receivedRequestMsgIds));
    if(receivedRequestMsgIds.contains(msgId)) {
      // Don't process duplicate messages
      Log.d(TAG, String.format("Received duplicate Commit Request message from %d of %s", from, message));
      return;
    }
    receivedRequestMsgIds.add(msgId);

    Log.v(TAG, String.format("New update to vote on from %d of %s", from, message));
    tryUpdate(from, message);
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
  	System.out.println("!!!!!!!! passingBack " + 0 + " " + message);

    int msgId = message.getId();
    Log.d(TAG, String.format("already received responses %s", receivedResponseMsgIds));
    if(receivedResponseMsgIds.contains(msgId)) {
      // Don't process duplicate messages
      Log.d(TAG, String.format("Received duplicate Commit Response message from %d of %s", from, message));
      return;
    }
    receivedResponseMsgIds.add(msgId);

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


  //////////////////////////////////////////////////////////////////////////////
  // Paxos node internal API
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Spin up a Paxos instance for the specified round if it doesn't already
   * exist.
   */
  private PaxosInstance spinupInstance(int roundNum) {
    PaxosInstance instance = instances.get(roundNum);
    if(instance == null) {
      Log.i(TAG, String.format("Spun up Paxos instance for round %d", roundNum));
      instance = new PaxosInstance(roundNum);
      instances.put(roundNum, instance);
    }
    return instance;
  }

  /**
   * Spin down a Paxos instance for the specified round.
   */
  private void spindownInstance(int roundNum) {
    Log.i(TAG, String.format("Spun down Paxos instance for round %d", roundNum));
    Log.v(TAG, String.format("Decided value for round %d was %s", 
                              roundNum, decidedUpdates.get(roundNum)));
    instances.remove(roundNum);
  }


  /**
   * Try to propose an update.
   */
  private void tryUpdate(int from, RPCMsg updateMsg) {
    Log.i(TAG, String.format("IT'S VOTIN' TIME ON BEHALF OF %d", from));
    Log.v(TAG, String.format("Let's vote on update %s", updateMsg));

    Log.d(TAG, String.format("Previously decided updates %s", decidedUpdates));
    Log.d(TAG, String.format("Current rounds of paxos running %s", instances.keySet()));

    // get the next round to spinup.  For any instance we already have
    // spun up, we know for sure there is at least one Proposer for
    // that round because a Proposer always starts a new round.
    int nextRound = -1;
    nextRound = Math.max(nextRound, (instances.isEmpty()) ? -1 : instances.lastKey());
    nextRound = Math.max(nextRound, (decidedUpdates.isEmpty()) ? -1 : decidedUpdates.lastKey());
    nextRound++;

    // spin up a new paxos instance
    PaxosInstance instance = spinupInstance(nextRound);

    // have the Proposer make a new proposal
    instance.proposer.broadcastPrepareRequest(from, updateMsg);
  }

  /**
   * Retry proposing an update.
   */
  private void retryUpdate(int from, RPCMsg updateMsg, int oldRoundNum) {
    tryUpdate(from, updateMsg);
  }

  /**
   * Save a decided update message.
   */
  private void saveUpdate(int roundNum, int clientId, RPCMsg updateMsg) {
    decidedUpdates.put(roundNum, new Pair<Integer, RPCMsg>(clientId, updateMsg));

    // spin down the Paxos instance
    spindownInstance(roundNum);

    // save the update locally, but don't pass it up a layer yet.
    // must first process every update before this, put in a buffer and take
    // out once the processed round number reaches this update's round number
    // actually commit the update
    int currRound = lastProcessedRound + 1;
    while(decidedUpdates.containsKey(currRound)) {
      Pair<Integer, RPCMsg> bufferedUpdate = decidedUpdates.get(currRound);

      onCommitRequest(bufferedUpdate.a, bufferedUpdate.b);
      lastProcessedRound = currRound;

      currRound++;
    }

    try {
      // write our new state
      writeState();
    } catch(IOException e) {
      Log.e(TAG, "File system failure on saving update processing");
      e.printStackTrace();
      throw new RuntimeException("File system failure on saving update processing");
    }
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
  public void onRPCPaxosRequest(Integer from, RPCMsg message) {
    PaxosMsg msg = (PaxosMsg)message;
    Log.i(TAG, String.format("%s request from %d", msg.msgType, from));
    Log.v(TAG, String.format("%s", msg));

    if(decidedUpdates.containsKey(msg.roundNum)) {

      if(msg.msgType == PaxosMsgType.UPDATE) {
        // queried to provide all updates committed since the attached proposal num
        // catch up the other node
        Log.v(TAG, "Request was an update request.  GIVE THEM ALLLL THE UPDATES");

        int lastProcessedRound = msg.roundNum;
        for(Integer roundNum : decidedUpdates.keySet()) {
          if(roundNum > lastProcessedRound) {
            Pair<Integer, RPCMsg> update = decidedUpdates.get(roundNum);
            PaxosProposal proposal = new PaxosProposal(-1, update.a, update.b);

            Log.d(TAG, String.format("Sending UPDATE of %s", proposal));
            RPCSendPaxosResponse(from, 
                    new PaxosMsg(msg, roundNum, proposal));
          }
        }

        return;
      } else {
        Log.i(TAG, "Request was an old message.");
        // this is an old message for an old round
        return;
      }
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
        instance.learner.receiveAcceptorAcceptedRequest(from, msg);
        break;

      case LEARNER_DECIDED: // Proposer notified that a new value has been DECIDED upon
        instance.proposer.receiveDecidedRequest(msg.proposal);
        break;

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
    Log.i(TAG, String.format("%s response from %d", msg.msgType, from));
    Log.v(TAG, String.format("%s", msg));

    if(msg.msgType == PaxosMsgType.UPDATE) {
      // node receiving a catch-up update
      saveUpdate(msg.roundNum, msg.proposal.clientId, msg.proposal.updateMsg);
      return;
    }

    PaxosInstance instance = spinupInstance(msg.roundNum);

    PaxosMsgType type = msg.msgType;
    switch(type) {
      // case PROPOSER_PREPARE: // should never get this
      // case PROPOSER_ACCEPT: // should never get this
      case ACCEPTOR_PROMISE: // Proposer heard back with a PROMISE in response to a PREPARE
      case ACCEPTOR_REJECT: // Proposer heard back with a REJECT in response to a PREPARE
        instance.proposer.receivePrepareResponse(from, msg);
        break;
        
      // case ACCEPTOR_ACCEPTED: // should never get this
      // case LEARNER_DECIDED: // should never get this
      default:
        Log.w(TAG, "Invalid PaxosMsgType on a response");
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  //-------------------- Paxos State Machine --------------------------------
  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  

  private class PaxosInstance {
    public final Proposer proposer;
    public final Acceptor acceptor;
    public final Learner learner;

    PaxosInstance(int roundNum) {
      proposer = new Proposer(roundNum);
      acceptor = new Acceptor(roundNum);
      learner = new Learner(roundNum);
    }
  }

  private abstract class AbstractActor {
    protected int currRound; 

    public String toString() {
      return String.format("<<< %s[%d] >>> :", this.getClass().getSimpleName(), currRound);
    }
  }

  //////////////////////////////////////////////////////////////////////////
  // Paxos Proposer 
  //////////////////////////////////////////////////////////////////////////

  private static enum ProposerState {
    NONE, PROMISE_WAIT, ACCEPTED_WAIT;
  }

  private class Proposer extends AbstractActor {

    // The maximum accepted proposal number seen by this node
    private int maxProposalNum;

    // PREPAREd proposal (but not chosen yet, waiting for ACCEPT requests)
    private PaxosProposal currProposal;
    
    private PaxosProposal originalProposal;
    
    // Acceptors that responded with an ACCEPT
    // When this is a majority, broadcast LEARNER_DECIDED information.
    private Set<Integer> promisingAcceptors;
    
    // Acceptors that responded with an rejecting ACCEPT
    // When this is a majority, propose something else
    private Set<Integer> rejectingAcceptors;


    private ProposerState state;
    

    Proposer(int currRound) {
      this.currRound = currRound;
      this.maxProposalNum = 0;
      this.currProposal = null;
      this.originalProposal = null;
      this.promisingAcceptors = new HashSet<Integer>();
      this.rejectingAcceptors = new HashSet<Integer>();
      this.state = ProposerState.NONE;
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
     * current max proposal number seen.
     *
     * The new proposal number is guaranteed to be higher than the passed in
     * max number.
     *
     * @modifies maxProposalNum 
     *                   if this new proposal number is higher than the current
     *                   max proposal number, then maxProposalNum is updated.
     */
    private int nextProposalNum() {
      int numServers = ServerList.serverNodes.size();
      // The +1 is to ensure that our math works out with 0-indexed server
      // addresses.  We want to ensure that the proposal number will always
      // grow and not be stuck at a specific number due to this calculation.
      int count = (int)Math.ceil((double)(maxProposalNum + 1) / (double)numServers);
      int nextNum = (count * numServers) + addr;

      assert(nextNum > maxProposalNum); // sanity check

      // update the max proposal number to be this new proposal number
      maxProposalNum = nextNum;
      Log.v(TAG, String.format("%s new proposal num %d", this, nextNum));
      return nextNum;
    }


    // broadcast of proposal to all Acceptors
    public void broadcastPrepareRequest(int from, RPCMsg updateMsg) {
      Log.i(TAG, String.format("%s broadcast prepare requests", this));
      int proposalNum = nextProposalNum();
      PaxosProposal proposal = new PaxosProposal(proposalNum, from, updateMsg);
      currProposal = proposal;
      if (originalProposal == null) {
      	originalProposal = proposal;
      }
      state = ProposerState.PROMISE_WAIT;

      for(int address: ServerList.serverNodes) {
        // send it even to myself
        // only send the proposal num
        RPCSendPaxosRequest(address, 
                new PaxosMsg(PaxosMsgType.PROPOSER_PREPARE, 
                             currRound, proposal));
      }
    }

    // recieve prepare results
    public void receivePrepareResponse(int from, PaxosMsg msg) {
      Log.i(TAG, String.format("%s receive prepare response", this));

      if(currProposal == null) {
        // we have no current proposal, a response is BOGUS
        Log.w(TAG, String.format("%s got BOGUS prepare response for proposal "+
                                 "they never proposed", this));
        return;
      }

    	// it has already accepted some sort of proposal
    	// we need to keep the value consistent.
    	if (msg.proposal != null) {
    		if (maxProposalNum < msg.proposal.proposalNum) {
    			maxProposalNum = msg.proposal.proposalNum;
      		int proposalNum = nextProposalNum();
          // TODO: STEPH we definitely do not want to switch the value of our
          // proposal's update message, otherwise we lose that update and start
          // trying to propose a different value.... right?
      		// TODO: DAVID, we do want to switch. If two or more proposals are made in the 
      		// same round and no one switches, then it's automatically a standstill. 
      		// When we learn a value is decided, then we have to compare it to the trasaction in our
      		// queue to see if it was ours or not. If our original proposal was accepted, pass it up to be
      		// processed for commit and remove it from the queue. If it wasn't, start another round and propose it agains.
    			currProposal = new PaxosProposal(proposalNum, msg.proposal.clientId, msg.proposal.updateMsg);
    			//currProposal = new PaxosProposal(proposalNum, currProposal.clientId, currProposal.updateMsg);
    		}
    	}

      if(msg.proposal.proposalNum == currProposal.proposalNum) {
        Log.d(TAG, String.format("%s %d promises out of %s servers", 
                                  this, promisingAcceptors.size(),
                                  ServerList.serverNodes.size()));
        Log.d(TAG, String.format("%s %d rejects out of %s servers", 
                                  this, rejectingAcceptors.size(),
                                  ServerList.serverNodes.size()));
        // if this message is for the current proposal
        switch(msg.msgType) {
          case ACCEPTOR_PROMISE:
            Log.v(TAG, String.format("ACCEPTED!  CHALK ANOTHER ONE UP"));

            promisingAcceptors.add(from);
            // if a majority promised and we were waiting for that
            if(state == ProposerState.PROMISE_WAIT && 
               promisingAcceptors.size() > ServerList.serverNodes.size() / 2) {
              // send an ACCEPT request to all other servers
              broadcastAcceptRequests(currProposal);
            } // else wait for more promises
            break;

          case ACCEPTOR_REJECT:
            Log.v(TAG, String.format("REJECTED!  GET REAL BRO"));

            rejectingAcceptors.add(from);

            // If a majority rejected and we were waiting for promises
            if(state == ProposerState.PROMISE_WAIT &&
               rejectingAcceptors.size() > ServerList.serverNodes.size() / 2) {
              Log.v(TAG, String.format("%s Retrying in a new round message update %s", 

                                        this, currProposal.updateMsg));

              // abort the proposal and re-propose
              broadcastPrepareRequest(currProposal.clientId, currProposal.updateMsg);

            } // else wait for more rejects
            break;
					
          default:
            Log.w(TAG, "Invalid message type received for a prepare response");
            break;
        }
      } else if(msg.proposal.proposalNum > currProposal.proposalNum) {
        // else, this message was for a more current proposal
        // TODO: do something?

      } // else, this message was for an older proposal
      
    }


    // send accept requests to all servers
    private void broadcastAcceptRequests(PaxosProposal proposal) {
      Log.i(TAG, String.format("%s broadcast accept requests", this));

      state = ProposerState.ACCEPTED_WAIT;
      // Send Accepts to the majority of nodes that promised.
      for(int address: promisingAcceptors) {
        // send even to myself
        RPCSendPaxosRequest(address, 
                new PaxosMsg(PaxosMsgType.PROPOSER_ACCEPT,
                             currRound, proposal));
      }
    }

    // receive the value
    public void receiveDecidedRequest(PaxosProposal proposal) {
      Log.i(TAG, String.format("%s receive decided request", this));

      // the decided proposal is not our own
      if(originalProposal != null && !originalProposal.equals(proposal)) {
        // so retry our own proposal
        retryUpdate(originalProposal.clientId, originalProposal.updateMsg, currRound);
      }
      state = ProposerState.NONE;

      saveUpdate(currRound, proposal.clientId, proposal.updateMsg);
    }
  }

  //////////////////////////////////////////////////////////////////////////
  // Paxos Acceptor
  //////////////////////////////////////////////////////////////////////////

  private class Acceptor extends AbstractActor {

    // The proposal number that this node has promised to not accept anything
    // less than
    private int promisedNum;

    // ACCEPTED proposal (unknown if it is chosen - accepted by anyone else)
    // Initially null when no proposal has been accepted
    // TODO: I think we do, because when we reject a promise, we have to respond
    // with the highest proposal number that we have promised AND the value of the proposal that we
    // have previously accepted. Then the proposer is constrainted to propose with a higher number AND a consistent value.
    // TODO: What do you mean by consistent value?  I thought Paxos was supposed
    // to be value-agnostic?
    private PaxosProposal acceptedProposal;

    Acceptor(int currRound) {
      this.currRound = currRound;
      this.promisedNum = -1;
      this.acceptedProposal = null;
    }
    
    /**
     * This Acceptor received a request to prepare for a proposal.
     */
    public void receivePrepareRequest(int from, PaxosMsg msg) {
      Log.i(TAG, String.format("%s receive prepare request", this));
      if (msg.proposal.proposalNum < promisedNum) {
        // Ignore
        // Send a response proposal with the current promise number
      	
        /*
          The accepted proposal number may not match the highest promised number... 
          in the case that there have been other promise requests after the first accept
          TODO: STEPH Why does the Acceptor promise proposal numbers
          that are different from it's accepted proposal?
          TODO: DAVID, Imagine the scenario where the acceptor promises 5, accepts on that promise, so its accepted proposal
          has number 5, then receives prepare 8. It will promise 8. Then suppose it receives yet another prepare
          request before it accepts anything else. It's accepted proposal has number 5, but the promised number is
          8. It needs to return 8 and the value of the accepted proposal. 

          TODO: STEPH, I'm still in the dark about why the Proposer needs to
          know what the Acceptor's previously-accepted proposal is.  I dug up
          another comment of yours:

          "DAVID, I don't think we can include proposalNum in equals
          proposal equality depends only on the <value> of the proposal, not the number.
          If 3/5 nodes have accepted 3 proposals with different numbers, but the same <value>, 
          then that value is still chosen."

          So when you say 'consistent' value, are you talking about this optimization?
          On the surface, I am imagining correctness issues with this optimization.
          Please convince me otherwise!
          
          One thing you can do for now:  actually set and manage the 
          acceptedProposal variable in the Acceptor.  Remember, an Acceptor may 
          be asked to accept multiple proposals.  The acceptedProposal variable
          must be updated accordingly.
          
          ---------
          
          Yes, the acceptor accepts as many proposals as it is send accept requests. 
          
          From the paper:
          1. A proposer chooses a new proposal number n and sends a request to
						each member of some set of acceptors, asking it to respond with:
						(a) A promise never again to accept a proposal numbered less than
						n, and
						(b) The proposal with the highest number less than n that it has
						accepted, if any.
						I will call such a request a prepare request with number n.
					2. If the proposer receives the requested responses from a majority of
						the acceptors, then it can issue a proposal with number n and value
						v, where v is the value of the highest-numbered proposal among the
						responses, or is any value selected by the proposer if the responders
						reported no proposals
						
        */
      	PaxosProposal toSend;
        if(acceptedProposal == null) {
          toSend = new PaxosProposal(promisedNum);
        } else {
          toSend = new PaxosProposal(promisedNum, acceptedProposal.clientId,
                                     acceptedProposal.updateMsg);
        }
      	PaxosMsg sendMsg = new PaxosMsg(PaxosMsgType.ACCEPTOR_REJECT, msg.roundNum, toSend);
        RPCSendPaxosResponse(from, sendMsg);

      } else if (msg.proposal.proposalNum > promisedNum) {
        // Promise
        promisedNum = msg.proposal.proposalNum;

      	PaxosProposal toSend = new PaxosProposal(promisedNum);
      	PaxosMsg sendMsg = new PaxosMsg(PaxosMsgType.ACCEPTOR_PROMISE, msg.roundNum, toSend);
        RPCSendPaxosResponse(from, sendMsg);

      } else { 
        // do nothing for duplicates
      }
    }


    /**
     * This Acceptor received a request to accept a proposal.
     */
    public void receiveAcceptRequest(int from, PaxosMsg msg) {
      Log.i(TAG, String.format("%s receive accept request", this));
      if (msg.proposal.proposalNum >= promisedNum) {
        // if the accept request is for a proposal newer than our
        // promisedNum
        // broadcast to all Learners that we've Accepted a value
        for(int address: ServerList.serverNodes) {
          RPCSendPaxosRequest(address, 
                  new PaxosMsg(msg, PaxosMsgType.ACCEPTOR_ACCEPTED));
        }
      } else {
        // TODO: DAVID!! IMPORTANT!!! I want to also sent back
        // Accepted/Rejected
        // The current accepted proposal value (includes proposal num) (could be null)
      	
        // TODO: STEPH:
        // ACCEPTOR_REJECT messages are negative responses to Prepare requests.
        // Mixing them with Accept requests is bad news

        // I don't know why you want negative responses to Accept requests.
        // But if you want them, make a new ACCEPTOR_REJECT message
      	//RPCSendPaxosResponse(from, 
        //        new PaxosMsg(msg, PaxosMsgType.ACCEPTOR_IGNORE));
      }
    }

  }

  //////////////////////////////////////////////////////////////////////////
  // Paxos Learner
  //////////////////////////////////////////////////////////////////////////

  private class Learner extends AbstractActor {

    // Acceptors that responded with an ACCEPTED
    // When this is a majority, broadcast LEARNER_DECIDED information.
    private Map<PaxosProposal, Set<Integer>> acceptedAcceptors;
    // Dear god please stop broadcasting.
    private boolean decided = false;
    
    Learner(int currRound) {
      this.currRound = currRound;
      acceptedAcceptors = new HashMap<PaxosProposal, Set<Integer>>();
    }
    
    // receive the agreed value
    public void receiveAcceptorAcceptedRequest(int from, PaxosMsg msg) {
      Log.i(TAG, String.format("%s receive accepted request", this));
      Log.i(TAG, "smg: " + msg);
      if(acceptedAcceptors.containsKey(msg.proposal)) {
      	acceptedAcceptors.get(msg.proposal).add(from);
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
      Log.i(TAG, String.format("%s broadcast decided requests", this));
      if(decided) {
        // if we already sent out decided requests
        return;
      }
      decided = true;
      for(int address: ServerList.serverNodes) {
        // send even to myself
        RPCSendPaxosRequest(address, 
                new PaxosMsg(PaxosMsgType.LEARNER_DECIDED,
                             currRound, proposal));
      }
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
    ACCEPTOR_REJECT("ACCEPTOR_REJECT"),
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
      else if(value.equals(ACCEPTOR_REJECT.value)) { return ACCEPTOR_REJECT; }
      else if(value.equals(ACCEPTOR_ACCEPTED.value)) { return ACCEPTOR_ACCEPTED; }
      else if(value.equals(LEARNER_DECIDED.value)) { return LEARNER_DECIDED; }
      else if(value.equals(UPDATE.value)) { return UPDATE; }
      else { return null; }
    }
  }

  /**
   * An RPC message for the Paxos system.
   */
  private static class PaxosMsg extends RPCMsg {
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

    PaxosMsg(PaxosMsg originalRequest, int roundNum,
             PaxosProposal newProposal) {
      this(originalRequest.id, originalRequest.msgType, 
           roundNum, newProposal);
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
  private static class PaxosProposal implements Serializable {
    public static final long serialVersionUID = 0L;

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
      
      return clientId == p.clientId &&
             (updateMsg != null ? updateMsg.getId() == p.updateMsg.getId() : true);
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
