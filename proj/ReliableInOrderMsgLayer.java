import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;

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
		this.n = n;
		this.msl = new MsgLogger(n);
		this.snl = new SeqNumLogger(n);
		
		SeqLogEntries sle = this.snl.getSeqLog();		
		
		//Recovering recvd/in side:
		// If we have no recv log files, then the number in sle is correct since we finished processing the last msg and therefore set this properly.
		// If we have recv log files but the number on this file is less than the min sequence number on all log files - 1, then this file is correct (we crashed with packets in out-of-order delivery queue, but this file has the correct number since we processed it successfully with the last delivery).
		// If we have recv log files and this equals min sequence number of log files - 1, or min sequence number of log files, then set this to the min of the sequence number of log files - 1 since we are about to deliver them.
		// If logs exist and its greater than all log values, then we have an error: we processed something out of order in an upper layer probably.
		LinkedList<SeqLogEntries.AddrSeqPair> last_recvs = sle.seq_recv();
		
		for(SeqLogEntries.AddrSeqPair pair: last_recvs){
			InChannel inC = new InChannel(this.snl, pair.addr());
			
			PriorityQueue<MsgLogEntry> recvLogs = this.msl.getChannelLogs(pair.addr(), MsgLogger.RECV);
			int currentLast_recv = pair.seq();
			if(!recvLogs.isEmpty()){
				int minLogSeqNum = recvLogs.peek().seqNum();
				if(currentLast_recv == minLogSeqNum-1 || currentLast_recv == minLogSeqNum) currentLast_recv = minLogSeqNum -1;
				else if(currentLast_recv > minLogSeqNum){
					throw new RuntimeException("RIOML constructor: Messages have been processed out of order.");
				}
				
				//these transactions were not completed.  Add them to the delivery queue, then consider delivery.
				for(MsgLogEntry mle: recvLogs){
					inC.outOfOrderMsgs.put(mle.seqNum(), new RIOPacket(Protocol.DATA, mle.seqNum(), mle.msg().getBytes()));
				}
				inC.lastSeqNumDelivered = currentLast_recv;
				
				LinkedList<RIOPacket> toBeDelivered = new LinkedList<RIOPacket>();
				inC.deliverSequence(toBeDelivered);
				for(RIOPacket p: toBeDelivered) {
					// deliver in-order the next sequence of packets
					n.onRIOReceive(pair.addr(), p.getProtocol(), p.getPayload());
				}
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
								
					RIOPacket newPkt = new RIOPacket(Protocol.DATA, mle.seqNum(), mle.msg().getBytes());
					outC.unACKedPackets.put(mle.seqNum(), newPkt);
					
					n.send(pair.addr(), Protocol.DATA, newPkt.pack());
					n.addTimeout(new Callback(onTimeoutMethod, this, new Object[]{ pair.addr(), mle.seqNum() }), ReliableInOrderMsgLayer.TIMEOUT);
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
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
		boolean alreadyLogged = this.msl.logMsg(from, new String(msg), riopkt.getSeqNum(), MsgLogger.RECV);		
		
		
					

		// ACK - will re-send if we have already seen this packet
		byte[] seqNumByteArray = Utility.stringToByteArray("" + riopkt.getSeqNum());
		n.send(from, Protocol.ACK, seqNumByteArray);
		
		// we have already seen this packet and logged it.  Its also possible we have received it before
		//  and already processed it fully.  In that case, we will fall through here and the inChannel
		//  will reject it below instead of delivering it.
		if(alreadyLogged) return;
		
		InChannel in = inConnections.get(from);
		if(in == null) {
			in = new InChannel(this.snl, from);
			inConnections.put(from, in);
		}
		
		LinkedList<RIOPacket> toBeDelivered = in.gotPacket(riopkt);
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
		this.msl.logMsg(destAddr, new String(payload), out.getNextSeqNum(), MsgLogger.SEND);
		
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
}

/**
 * Representation of an incoming channel to this node
 */
class InChannel {
	protected int lastSeqNumDelivered;
	private int fromAddr;
	private SeqNumLogger snl;
	protected HashMap<Integer, RIOPacket> outOfOrderMsgs;
	
	InChannel(SeqNumLogger snl, int fromAddr){
		lastSeqNumDelivered = -1;
		this.snl = snl;
		this.fromAddr = fromAddr;
		outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
	}
	

	InChannel(SeqNumLogger snl, int fromAddr, int lsnd){
		lastSeqNumDelivered = lsnd;
		this.fromAddr = fromAddr;
		this.snl = snl;
		outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
	}

	/**
	 * Method called whenever we receive a data packet.
	 * 
	 * @param pkt
	 *            The packet
	 * @return A list of the packets that we can now deliver due to the receipt
	 *         of this packet
	 */
	public LinkedList<RIOPacket> gotPacket(RIOPacket pkt) {
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
		// Duplicate packets are ignored
		
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
		}catch(Exception e) {
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
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
