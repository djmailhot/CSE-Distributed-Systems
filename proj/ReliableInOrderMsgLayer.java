import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.UUID;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;
import edu.washington.cs.cse490h.lib.Node.NodeCrashException;

/**
 * Layer above the basic messaging layer that provides reliable, in-order
 * delivery in the absence of faults. This layer does not provide much more than
 * the above.
 * 
 * At a minimum, the student should extend/modify this layer to provide
 * reliable, in-order message delivery, even in the presence of node failures.
 */
public class ReliableInOrderMsgLayer {
	public static int TIMEOUT = 3;
	
	private HashMap<Integer, InChannel> inConnections;
	private HashMap<Integer, OutChannel> outConnections;
	private RIONode n;
	private MsgLogger msl;
	private SeqNumLogger snl;
	private HashMap<Integer,SeqLogEntries.AddrSeqPair> responseMap;
	private HashMap<Integer,SeqLogEntries.AddrSeqPair> responseRecvdMap;
	private LinkedList<DeliveryObject> tempDelivery;
	
	private class DeliveryObject{
		public DeliveryObject(int addr2, RIOPacket p2) {
			addr=addr2;
			p=p2;
		}
		int addr;
		RIOPacket p;
	}

	/**
	 * Constructor.
	 * 
	 * @param destAddr
	 *            The address of the destination host
	 * @param msg
	 *            The message that was sent
	 * @param timeSent
	 *            The time that the ping was sent
	 */
	public ReliableInOrderMsgLayer(RIONode n) {
		inConnections = new HashMap<Integer, InChannel>();
		outConnections = new HashMap<Integer, OutChannel>();
		responseMap = new HashMap<Integer, SeqLogEntries.AddrSeqPair>();
		responseRecvdMap = new HashMap<Integer,SeqLogEntries.AddrSeqPair>();
		this.n = n;
		this.msl = new MsgLogger(n);
		this.snl = new SeqNumLogger(n);
		this.tempDelivery = new LinkedList<DeliveryObject>();
		
		SeqLogEntries sle = this.snl.getSeqLog();
		
		//Recovering responseMap:
		PriorityQueue<MsgLogEntry> recvLogsAll = this.msl.getLogs(MsgLogger.RECV);
		PriorityQueue<MsgLogEntry> sendLogsAll = this.msl.getLogs(MsgLogger.SEND);
		boolean deletedSomeRecvs = false;
		for(MsgLogEntry mle: recvLogsAll){
			RIOPacket rp = new RIOPacket(Protocol.DATA, mle.seqNum(), mle.msg());
			System.out.print("rp: " );
			for(int i = 0;i<rp.getPayload().length;i++){
	    		System.out.print((char)rp.getPayload()[i]);
	    	}
			System.out.println("length: " + new String(rp.getPayload()).length());
			RPCNode.MessageType mt = RPCNode.extractMessageType(rp.getPayload());

			//if we have a matching ID, then we crashed between deleting the recv log, and making the send log for the response.
			//We have a send log, but the recv log hasn't been deleted.  So we do that now, and don't add this to the responseMap.
			boolean alreadyResponded = false;
			for(MsgLogEntry mle2: sendLogsAll){
				RIOPacket rp2 = new RIOPacket(Protocol.DATA, mle2.seqNum(), mle2.msg());
				if(RPCNode.extractMessageId(rp.getPayload()) == RPCNode.extractMessageId(rp2.getPayload())){
					alreadyResponded = true;
					deletedSomeRecvs = true;
					this.msl.deleteLog(mle2.addr(), mle2.seqNum(), MsgLogger.RECV);
				}
			}
			
			if(!alreadyResponded){
				if(mt==RPCNode.MessageType.REQUEST) responseMap.put(RPCNode.extractMessageId(rp.getPayload()), new SeqLogEntries.AddrSeqPair(mle.addr(), mle.seqNum()));
				else responseRecvdMap.put(RPCNode.extractMessageId(rp.getPayload()), new SeqLogEntries.AddrSeqPair(mle.addr(), mle.seqNum()));
			}
			
		}

		
		//Recovering recvd/in side:
		// If we have no recv log files, then the number in sle is correct since we finished processing the last msg and therefore set this properly.
		// If we have recv log files but the number on this file is less than the min sequence number on all log files - 1, then this file is correct (we crashed with packets in out-of-order delivery queue, but this file has the correct number since we processed it successfully with the last delivery).
		// If we have recv log files and this equals min sequence number of log files - 1, or min sequence number of log files, then set this to the min of the sequence number of log files - 1 since we are about to deliver them.
		// If logs exist and its greater than all log values, then we have an error: we processed something out of order in an upper layer probably.
		
		if(deletedSomeRecvs) recvLogsAll = this.msl.getLogs(MsgLogger.RECV);
		
		LinkedList<SeqLogEntries.AddrSeqPair> last_recvs = sle.seq_recv();		
		for(SeqLogEntries.AddrSeqPair pair: last_recvs){
			InChannel inC = new InChannel(this.msl, this.snl, pair.addr());
			
			PriorityQueue<MsgLogEntry> recvLogs = this.msl.getChannelLogs(pair.addr(), MsgLogger.RECV);
			
			int currentLast_recv = pair.seq();
			if(!recvLogs.isEmpty()){
				int minLogSeqNum = recvLogs.peek().seqNum();
				/*if(currentLast_recv == minLogSeqNum-1 || currentLast_recv == minLogSeqNum) currentLast_recv = minLogSeqNum -1;
				else if(currentLast_recv > minLogSeqNum){
					throw new RuntimeException("RIOML constructor: Messages have been processed out of order. minSeq: " + Integer.toString(minLogSeqNum) + ", currentLast:" + Integer.toString(currentLast_recv));
				}*/
				if(currentLast_recv > minLogSeqNum-1) currentLast_recv = minLogSeqNum-1; 
				
				//these transactions were not completed.  Add them to the delivery queue, then consider delivery.
				for(MsgLogEntry mle: recvLogs){
					inC.outOfOrderMsgs.put(mle.seqNum(), new RIOPacket(Protocol.DATA, mle.seqNum(), mle.msg()));
				}
				inC.lastSeqNumDelivered = currentLast_recv;
				
				LinkedList<RIOPacket> toBeDelivered = new LinkedList<RIOPacket>();
				inC.deliverSequence(toBeDelivered);
				for(RIOPacket p: toBeDelivered) this.tempDelivery.add(new DeliveryObject(pair.addr(),p));   //will deliver after construction
			}
			else inC.lastSeqNumDelivered = currentLast_recv;
			
			inConnections.put(pair.addr(), inC);
		}
		
		//Recovering last sent index:
		// We have one such index for each out channel, so we have (seq_num, destAddr) tuples.
		// If version on file >= max of sequence numbers on log, take the version on file.  This means we successfully processed a message at least as high as the last one logged.
		// If version on file < max sequence numbers on log, take the max sequence number on logs.  That means we logged but then crashed before updating the pointer.  In this case, just take the last one on the logs.  Logging happens first.
		LinkedList<SeqLogEntries.AddrSeqPair> last_sends = sle.seq_send();
		
		for(SeqLogEntries.AddrSeqPair pair: last_sends){
			
			OutChannel outC = new OutChannel(this.snl, this.msl, this, pair.addr());
			
			PriorityQueue<MsgLogEntry> sendLogs = this.msl.getChannelLogs(pair.addr(), MsgLogger.SEND);
			
			int maxLogSeqNum = -1;
			if(!sendLogs.isEmpty()){
				for (MsgLogEntry e : sendLogs) maxLogSeqNum = Math.max(maxLogSeqNum,e.seqNum());
			}
			
			if(pair.seq() < maxLogSeqNum) outC.lastSeqNumSent = maxLogSeqNum;
			else outC.lastSeqNumSent = pair.seq();
			
			//these transactions were not ACKd and possibly not sent.  Add them to the resend cycles and unACKd queue.
			for(MsgLogEntry mle: sendLogs){
				try{					
					Method onTimeoutMethod = Callback.getMethod("onTimeout", this, new String[]{ "java.lang.Integer", "java.lang.Integer" });
								
					RIOPacket newPkt = new RIOPacket(Protocol.DATA, mle.seqNum(), mle.msg());
					outC.unACKedPackets.put(mle.seqNum(), newPkt);
					
					n.send(pair.addr(), Protocol.DATA, newPkt.pack());
					n.addTimeout(new Callback(onTimeoutMethod, this, new Object[]{ pair.addr(), mle.seqNum() }), ReliableInOrderMsgLayer.TIMEOUT);
				}catch(NoSuchMethodException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SecurityException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			outConnections.put(pair.addr(), outC);
		}
	}
	
	public void cleanUpConstruction(){
		for(DeliveryObject o: this.tempDelivery) {
			// deliver in-order the next sequence of packets
			System.out.println("cleanup: " + Integer.toString(o.addr));
			System.out.println("cleanup: " + Integer.toString(o.p.getProtocol()));
			System.out.println("cleanup: " + new String(o.p.getPayload()));
			n.onRIOReceive(o.addr, o.p.getProtocol(), o.p.getPayload());
		}
	}
	
	/**
	 * Receive a data packet.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void RIODataReceive(int from, byte[] msg) {
		//log before ACKing.  Guarantees that the msg is always actively being re-sent or
		//  logged on the server.  This will help us guarantee at-least-once semantics on
		//  the msg itself.  Note if a log file already exists for this from/seqNumber combination
		//  we will not log it again.
		RIOPacket riopkt = RIOPacket.unpack(msg);
		InChannel in = inConnections.get(from);
		if(in == null) {
			in = new InChannel(this.msl, this.snl, from);
			inConnections.put(from, in);
		}
	
		//network corruption won't let us make a packet
		if(riopkt==null) return;
	
		
		if(riopkt.getSeqNum() <= in.lastSeqNumDelivered){
			// ACK - will re-send if we have already seen this packet
			byte[] seqNumByteArray = Utility.stringToByteArray("" + riopkt.getSeqNum());
			n.send(from, Protocol.ACK, seqNumByteArray);
			return;
		}

		RPCNode.MessageType mt = RPCNode.extractMessageType(riopkt.getPayload());
		if(mt == RPCNode.MessageType.REQUEST) responseMap.put(RPCNode.extractMessageId(riopkt.getPayload()), new SeqLogEntries.AddrSeqPair(from, riopkt.getSeqNum()));
		else responseRecvdMap.put(RPCNode.extractMessageId(riopkt.getPayload()), new SeqLogEntries.AddrSeqPair(from, riopkt.getSeqNum()));
		
		boolean alreadyLogged = this.msl.logMsg(from, riopkt.getPayload(), riopkt.getSeqNum(), MsgLogger.RECV);		
		
		
					

		// ACK - will re-send if we have already seen this packet
		byte[] seqNumByteArray = Utility.stringToByteArray("" + riopkt.getSeqNum());
		n.send(from, Protocol.ACK, seqNumByteArray);
		
		// we have already seen this packet and logged it.  Its also possible we have received it before
		//  and already processed it fully.  In that case, we will fall through here and the inChannel
		//  will reject it below instead of delivering it.
		if(alreadyLogged) return;
		
		
		
		LinkedList<RIOPacket> toBeDelivered = in.gotPacket(from, riopkt);
		for(RIOPacket p: toBeDelivered) {
			// deliver in-order the next sequence of packets
			n.onRIOReceive(from, p.getProtocol(), p.getPayload());
		}
	}
	
	/**
	 * Receive an acknowledgment packet.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void RIOAckReceive(int from, byte[] msg) {
		int seqNum = Integer.parseInt( Utility.byteArrayToString(msg) );
		outConnections.get(from).gotACK(from,seqNum);
	}

	/**
	 * Send a packet using this reliable, in-order messaging layer. Note that
	 * this method does not include a reliable, in-order broadcast mechanism.
	 * 
	 * @param destAddr
	 *            The address of the destination for this packet
	 * @param protocol
	 *            The protocol identifier for the packet
	 * @param payload
	 *            The payload to be sent
	 */
	public void RIOSend(int destAddr, int protocol, byte[] payload) {		
		OutChannel out = outConnections.get(destAddr);
		if(out == null) {
			out = new OutChannel(this.snl, this.msl, this, destAddr);
			outConnections.put(destAddr, out);
		}
		
		//log before sending.  Guarantees that the msg is always actively being re-sent, in the unACKd queue,
		//  on the server.  We can recover from these logs upon recovery.  This will help us guarantee at-least-once 
		//  semantics on the msg itself.  Note if a log file already exists for this from/seqNumber combination
		//  we will not log it again.
		
		//NOTE: if we move to concurrency, will need to synchronize here on out channel to prevent TOC/TOU bug
		// on next seq number.
		if(protocol != Protocol.ACK){
			this.msl.logMsg(destAddr, payload, out.getNextSeqNum(), MsgLogger.SEND);
			
			/* Delete the recv log if this is a response */
			int currentID = RPCNode.extractMessageId(payload);
			SeqLogEntries.AddrSeqPair asp = responseMap.get(currentID);
			if(asp != null){
				this.msl.deleteLog(asp.addr(), asp.seq(), MsgLogger.RECV);
				responseMap.remove(currentID);
			}
		}
		
		out.sendRIOPacket(n, protocol, payload);
	}

	/**
	 * Callback for timeouts while waiting for an ACK.
	 * 
	 * This method is here and not in OutChannel because OutChannel is not a
	 * public class.
	 * 
	 * @param destAddr
	 *            The receiving node of the unACKed packet
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	public void onTimeout(Integer destAddr, Integer seqNum) {
		outConnections.get(destAddr).onTimeout(n, seqNum);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for(Integer i: inConnections.keySet()) {
			sb.append(inConnections.get(i).toString() + "\n");
		}
		
		return sb.toString();
	}

	public void responseFinalized(int id) {
		SeqLogEntries.AddrSeqPair asp = this.responseRecvdMap.get(id);
		if(asp != null){
			this.msl.deleteLog(asp.addr(), asp.seq(), MsgLogger.RECV);
			this.responseRecvdMap.remove(id);
		}
		
	}
}

/**
 * Representation of an incoming channel to this node
 */
class InChannel {
	protected int lastSeqNumDelivered;
	private int fromAddr;
	private SeqNumLogger snl;
	private MsgLogger msl;
	protected HashMap<Integer, RIOPacket> outOfOrderMsgs;
	
	InChannel(MsgLogger msl, SeqNumLogger snl, int fromAddr){
		lastSeqNumDelivered = -1;
		this.snl = snl;
		this.fromAddr = fromAddr;
		outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
		this.msl = msl;
	}
	

	InChannel(MsgLogger msl, SeqNumLogger snl, int fromAddr, int lsnd){
		lastSeqNumDelivered = lsnd;
		this.fromAddr = fromAddr;
		this.snl = snl;
		outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
		this.msl = msl;
	}

	/**
	 * Method called whenever we receive a data packet.
	 * 
	 * @param pkt
	 *            The packet
	 * @return A list of the packets that we can now deliver due to the receipt
	 *         of this packet
	 */
	public LinkedList<RIOPacket> gotPacket(int from, RIOPacket pkt) {
		LinkedList<RIOPacket> pktsToBeDelivered = new LinkedList<RIOPacket>();
		int seqNum = pkt.getSeqNum();
		
		if(seqNum == lastSeqNumDelivered + 1) {
			// We were waiting for this packet
			pktsToBeDelivered.add(pkt);
			this.snl.updateSeq(++lastSeqNumDelivered, this.fromAddr, SeqNumLogger.RECV);
			deliverSequence(pktsToBeDelivered);
		}else if(seqNum > lastSeqNumDelivered + 1){
			// We received a subsequent packet and should store it
			outOfOrderMsgs.put(seqNum, pkt);
		}
		// Duplicate packets are ignored, should delete a log at this point if it exists
		else{
			this.msl.deleteLog(from, pkt.getSeqNum(), MsgLogger.RECV);
		}
		
		return pktsToBeDelivered;
	}

	/**
	 * Helper method to grab all the packets we can now deliver.
	 * 
	 * @param pktsToBeDelivered
	 *            List to append to
	 */
	protected void deliverSequence(LinkedList<RIOPacket> pktsToBeDelivered) {
		while(outOfOrderMsgs.containsKey(lastSeqNumDelivered + 1)) {
			++lastSeqNumDelivered;
			pktsToBeDelivered.add(outOfOrderMsgs.remove(lastSeqNumDelivered));
		}
		this.snl.updateSeq(lastSeqNumDelivered, this.fromAddr, SeqNumLogger.RECV);
		
	}
	
	public int currentSeqNumber(){
		return this.lastSeqNumDelivered;
	}
	
	@Override
	public String toString() {
		return "last delivered: " + lastSeqNumDelivered + ", outstanding: " + outOfOrderMsgs.size();
	}
}

/**
 * Representation of an outgoing channel from this node
 */
class OutChannel {
	protected HashMap<Integer, RIOPacket> unACKedPackets;
	protected int lastSeqNumSent;
	private ReliableInOrderMsgLayer parent;
	private int destAddr;
	private SeqNumLogger snl;
	private MsgLogger msl;
	
	OutChannel(SeqNumLogger snl, MsgLogger msl, ReliableInOrderMsgLayer parent, int destAddr){
		lastSeqNumSent = -1;
		this.snl = snl;
		this.msl = msl;
		unACKedPackets = new HashMap<Integer, RIOPacket>();
		this.parent = parent;
		this.destAddr = destAddr;
	}
	
	OutChannel(SeqNumLogger snl, MsgLogger msl, ReliableInOrderMsgLayer parent, int destAddr, int lsn){
		lastSeqNumSent = lsn;
		this.snl = snl;
		this.msl = msl;
		unACKedPackets = new HashMap<Integer, RIOPacket>();
		this.parent = parent;
		this.destAddr = destAddr;
	}
	
	/**
	 * Send a new RIOPacket out on this channel.
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param protocol
	 *            The protocol identifier of this packet
	 * @param payload
	 *            The payload to be sent
	 */
	protected void sendRIOPacket(RIONode n, int protocol, byte[] payload) {
		try{
			this.snl.updateSeq(++lastSeqNumSent, this.destAddr, SeqNumLogger.SEND);
			
			Method onTimeoutMethod = Callback.getMethod("onTimeout", parent, new String[]{ "java.lang.Integer", "java.lang.Integer" });
						
			RIOPacket newPkt = new RIOPacket(protocol, lastSeqNumSent, payload);
			unACKedPackets.put(lastSeqNumSent, newPkt);
			
			n.send(destAddr, Protocol.DATA, newPkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[]{ destAddr, lastSeqNumSent }), ReliableInOrderMsgLayer.TIMEOUT);
		}catch(NoSuchMethodException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected int getNextSeqNum(){
		return lastSeqNumSent + 1;
	}
	
	/**
	 * Called when a timeout for this channel triggers
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	public void onTimeout(RIONode n, Integer seqNum) {
		if(unACKedPackets.containsKey(seqNum)) {
			resendRIOPacket(n, seqNum);
		}
	}
	
	/**
	 * Called when we get an ACK back. Removes the outstanding packet if it is
	 * still in unACKedPackets.
	 * 
	 * @param seqNum
	 *            The sequence number that was just ACKed
	 */
	protected void gotACK(int dest, int seqNum) {
		//added a check here since now we might get multiple ACKS for safety
		if(unACKedPackets.containsKey(seqNum)) unACKedPackets.remove(seqNum);
		
		//remove corresponding send log
		this.msl.deleteLog(dest, seqNum, MsgLogger.SEND);
		
	}
	
	/**
	 * Resend an unACKed packet.
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	private void resendRIOPacket(RIONode n, int seqNum) {
		try{
			Method onTimeoutMethod = Callback.getMethod("onTimeout", parent, new String[]{ "java.lang.Integer", "java.lang.Integer" });
			RIOPacket riopkt = unACKedPackets.get(seqNum);
			
			n.send(destAddr, Protocol.DATA, riopkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[]{ destAddr, seqNum }), ReliableInOrderMsgLayer.TIMEOUT);
		}catch(NoSuchMethodException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
