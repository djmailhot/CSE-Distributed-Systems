package edu.washington.cs.cse490h.lib;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

//Holds the list of node addresses which are our servers

public class ServerList {
	public static final Set<Integer> serverNodes = new HashSet<Integer>();
	static {
        //serverNodes.add(0);
        serverNodes.add(1);
        serverNodes.add(2);
        serverNodes.add(3);
  }
	/*
	 * Returns true iff n is an address in our server list.
	 */
	public static boolean in(int n){
		return serverNodes.contains(n);
	}

	/**
	 * @return the total number of servers
	 */
  public static int numServers() {
    return serverNodes.size();
  }

	/**
	 * @return an address of a single server
	 */
  public static int getAServerAddr() {
    int randomNum = Utility.getRNG().nextInt(numServers());
    List<Integer> list = new ArrayList<Integer>(serverNodes);
    return list.get(randomNum);
  }
}
