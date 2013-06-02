import java.util.ArrayList;
import java.util.List;


public class SerializeTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		NFSTransaction.Builder b = new NFSTransaction.Builder(5);
		List<MCCNode.MCCFileData> list = new ArrayList<MCCNode.MCCFileData>();
		list.add(new MCCNode.MCCFileData(0, null, null, false));
    RPCNode.RPCMsg msg = new MCCNode.MCCMsg(list, b.build(), true);
		RPCNode.RPCCallBundle bundle = 
            new RPCNode.RPCCallBundle(msg.getId(), RPCNode.RPCCallType.REQUEST,
                                      RPCNode.RPCMsgType.COMMIT, msg);
		System.out.println("Before: " + bundle);
		byte[] serialized = RPCNode.RPCCallBundle.serialize(bundle);
		RPCNode.RPCCallBundle deserialized = RPCNode.RPCCallBundle.deserialize(serialized);
		System.out.println("After: " + deserialized);
	}

}
