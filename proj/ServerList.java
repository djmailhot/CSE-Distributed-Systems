import java.util.HashSet;

//Holds the list of node addresses which are our servers

public class ServerList {
	static HashSet<Integer> serverNodes = new HashSet<Integer>();
	static {
        serverNodes.add(0);
        serverNodes.add(1);
        serverNodes.add(2);
    }
	/*
	 * Returns true iff n is an address in our server list.
	 */
	public boolean in(int n){
		return serverNodes.contains(n);
	}
}
