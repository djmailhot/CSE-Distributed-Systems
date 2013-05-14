
public class SerializeTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		NFSTransaction.Builder b = new NFSTransaction.Builder(5);
		RPCNode.RPCBundle bundle = new RPCNode.RPCBundle(RPCNode.MessageType.REQUEST, true, null, b.build());
		System.out.println("Before: " + bundle);
		byte[] serialized = RPCNode.RPCBundle.serialize(bundle);
		RPCNode.RPCBundle deserialized = RPCNode.RPCBundle.deserialize(serialized);
		System.out.println("After: " + deserialized);
	}

}
