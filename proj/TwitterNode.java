import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;

import plume.Pair;



/*
 * I decided to collapse the client and server into the same node. Since the server is really lightweight
 * and I didnt want to spend time changing the tests to bew able to start different kinds of nodes.
 * 
 * If this is a problem, it will be easy enough to split them apart again. 
 */
public class TwitterNode extends MCCNode {
	private String username = null;  // TODO: change back to null
	private int DEST_ADDR = addr == 0? 1 : 0; // Copied from TwoGenerals.java
	private ClientCommandLogger ccl;
	
	
	boolean waitingForResponse = false;
	Queue<String> commandQueue = new LinkedList<String>();
	
	// Ignore disk crashes
/*
  public static double getFailureRate() { return 20/100.0; }
	public static double getRecoveryRate() { return 100/100.0; }
	public static double getDropRate() { return 10/100.0; }
	public static double getDelayRate() { return 10/100.0; }
*/
	public TwitterNode() {
		super();
		this.ccl = new ClientCommandLogger(this);
	}
	
	private enum TwitterOp {
		LOGIN,
		LOGOUT,	
		CREATE,		
		TWEET,		
		READTWEETS,	
		FOLLOW,		
		UNFOLLOW,		
		BLOCK;
	}
	
	public void start() {	
		System.out.println("TwitterNode " + addr + " starting.");
		List<String> file;
		try {
			file = nfsService.read("username.txt");
			
			List<String> loggedCommands = this.ccl.loadLogs();
			for(String s : loggedCommands){
				this.commandQueue.add(s);
			}
			
			//MATT's TODO: load mapping of ID, log sequence number
			
		} catch (IOException e) {
			file = null;
		}
		username = (file == null || file.size() == 0) ? null : file.get(0);
		System.out.println("username: " + username);
		super.start();
	}

	@Override
	public void onCommand(String command) {
		if(command != null && knownCommand(command.toLowerCase())){
			return;
		}

		System.err.println("Unrecognized command: " + command);
	}
	
	private boolean knownCommand(String command) {
		if (command == null) { return false; }
		
		String[] parsedCommand = command.split(" ");
		String commandName = parsedCommand[0];
		if(commandName.equals("login")) {
			this.ccl.logCommand(command);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			} else {
				if (waitingForResponse) {
					System.out.println("Please wait!!");
					commandQueue.offer(command);
				} else {
					login(parsedCommand[1]);
				}
			}
			return true;
		} else if (commandName.equals("logout") && username != null) {
			this.ccl.logCommand(command);
			if (waitingForResponse) {
				System.out.println("Please wait!!");
				commandQueue.offer(command);
			} else {
				logout();	
			}
			return true;
		} else if (commandName.equals("create")) {
			this.ccl.logCommand(command);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			} else {
				if (waitingForResponse) {
					System.out.println("Please wait!!");
					commandQueue.offer(command);
				} else {
					create(parsedCommand[1]);
				}
			}
			return true;
		} else if (commandName.equals("tweet") && username != null) {
			this.ccl.logCommand(command);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a tweet.");
			}
			if (waitingForResponse) {
				System.out.println("Please wait!!");
				commandQueue.offer(command);
			} else {
				tweet(command.substring(5).trim());
			}
			return true;
		} else if (commandName.equals("readtweets") && username != null) {
			this.ccl.logCommand(command);
			if (waitingForResponse) {
				System.out.println("Please wait!!");
				commandQueue.offer(command);
			} else {
				readTweets();
			}
			return true;
		} else if (commandName.equals("follow") && username != null) {
			this.ccl.logCommand(command);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			} else {
				if (waitingForResponse) {
					System.out.println("Please wait!!");
					commandQueue.offer(command);
				} else {
					follow(parsedCommand[1]);
				}
			} 
			return true;
		} else if (commandName.equals("unfollow") && username != null) {
			this.ccl.logCommand(command);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			} else {
				if (waitingForResponse) {
					System.out.println("Please wait!!");
					commandQueue.offer(command);
				} else {
					unfollow(parsedCommand[1]);
				}
			}
			return true;
		} else if (commandName.equals("block") && username != null) {
			this.ccl.logCommand(command);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			} else {
				if (waitingForResponse) {
					System.out.println("Please wait!!");
					commandQueue.offer(command);
				} else {
					block(parsedCommand[1]);
				}
			}
			return true;
		}
		return false;
	}
	
	
	private void create(String user) {//done
		waitingForResponse = true;
		try {
			int transactionId = edu.washington.cs.cse490h.lib.Utility.getRNG().nextInt();
			String filename = user + "_followers.txt";

			nfsService.create(filename);
			mapUUIDs(transactionId, TwitterOp.CREATE, Arrays.asList(user));
			
			NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId);
			b.createFile(filename);
			
			commitTransaction(DEST_ADDR, b.build());
			System.out.println("create user commit sent");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private void login(String user) {
		waitingForResponse = true;
		// CHECK_EXISTENCE of user_followers.txt
		//JSONObject existance = transactionExist(user + "_followers.txt");
		//System.out.println(existance.toString());
		// TODO how do I know the address?
		//List<UUID> uuids = RPCSend(DEST_ADDR, new ArrayList<JSONObject>(Arrays.asList(existance)));
		//mapUUIDs(uuids, TwitterOp.LOGIN, user);
	}
	
	private void logout() {
		try {
			nfsService.delete("username.txt");
      username = null;
		} catch (IOException e) {
		}
		System.out.println("Logout successful.");
	}
	
	private void tweet(String tweet){//done
		waitingForResponse = true;
		try {
			int transactionId = edu.washington.cs.cse490h.lib.Utility.getRNG().nextInt();
			String filename = username + "_followers.txt";
			List<String> followers = nfsService.read(filename); // read the cached copy of the followers
	
			NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId);
			b.touchFile(filename);
			
			for (String follower : followers) {
				nfsService.append(follower + "_stream.txt", username + ": " + tweet);
			}
			mapUUIDs(transactionId, TwitterOp.TWEET, Arrays.asList(tweet));
			
			commitTransaction(DEST_ADDR, b.build());
			System.out.println("read tweets commit sent");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private void readTweets() {//done
		waitingForResponse = true;
		try {
			int transactionId = edu.washington.cs.cse490h.lib.Utility.getRNG().nextInt();
			String filename = username + "_stream.txt";
			List<String> tweets = nfsService.read(filename); // read the cached copy of the tweets
			nfsService.delete(filename);
			mapUUIDs(transactionId, TwitterOp.READTWEETS, tweets);
			
			NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId);
			b.touchFile(filename);
			b.deleteFile(filename);
			
			commitTransaction(DEST_ADDR, b.build());
			System.out.println("read tweets commit sent");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private void follow(String followUserName) {// done
		waitingForResponse = true;
		try {
			int transactionId = edu.washington.cs.cse490h.lib.Utility.getRNG().nextInt();
			String filename = followUserName + "_followers.txt";
			if (nfsService.exists(filename)) {
				nfsService.append(filename, username); // append to the cache copy
			}

			NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId);
			b.touchFile(filename); // will this fail if the file does not exist??? I hope so!
			b.appendLine(filename, followUserName);
			
			mapUUIDs(transactionId, TwitterOp.FOLLOW, Arrays.asList(followUserName));
			
			commitTransaction(DEST_ADDR, b.build());
			System.out.println("follow " + followUserName + " commit sent");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private void unfollow(String unfollowUserName) {
		waitingForResponse = true;
		// tell server to delete unfollowUserName from following
		// DELETE_LINE username, unfollowUserName_followers
		//JSONObject delete = transactionDeleteLine(unfollowUserName + "_followers.txt", username);
		//List<UUID> uuids = RPCSend(DEST_ADDR, new ArrayList<JSONObject>(Arrays.asList(delete)));
		//mapUUIDs(uuids, TwitterOp.UNFOLLOW, unfollowUserName);
		System.out.println("unfollow delete RPC sent");
	}
	
	private void block(String blockUserName) {
		waitingForResponse = true;
		// tell server to delete username from blockUserName's following list
		// DELETE_LINE blockUserName, username_followers
		//JSONObject delete = transactionDeleteLine(username + "_followers.txt", blockUserName);
		//List<UUID> uuids = RPCSend(DEST_ADDR, new ArrayList<JSONObject>(Arrays.asList(delete)));
		//mapUUIDs(uuids, TwitterOp.BLOCK, blockUserName);
		System.out.println("block delete RPC sent");
	}
	
	private Map<Integer, Pair<TwitterOp, List<String>>> uuidmap = new HashMap<Integer, Pair<TwitterOp, List<String>>>();
	//private Map<UUID, List<UUID>> transactionsmap = new HashMap<UUID, List<UUID>>(); // no longer needed
	
	private void mapUUIDs(Integer uuid, TwitterOp op, List<String> extraInfo){
		uuidmap.put(uuid, Pair.of(op, extraInfo));
	}


	// Assumes cache is up to date
	@Override
	public void onMCCResponse(Integer from, int tid, boolean success) {
		waitingForResponse = false;
		Pair<TwitterOp, List<String>> p = uuidmap.remove(tid);
		TwitterOp op = p.a;
		List<String> extraInfo = p.b;
		
		if (success) {
			switch(op){		
			case CREATE: 
				System.out.println("You created user " + extraInfo.get(0));
				pollCommand();
				break;
			case LOGIN: 
			case TWEET: 
				System.out.println("You tweeted " + extraInfo.get(0));
				pollCommand();
				break;
			case READTWEETS: {
				// We successfully read and deleted the stream file, so now display the tweets to the user.
				if (extraInfo != null) {
					for (String tweet : extraInfo) {
						System.out.println(tweet);
					}
				} else {
					System.out.println("You have no unread tweets.");
				}
				pollCommand();
				break;
			}
			case FOLLOW: {
				updateAllFiles(DEST_ADDR); // TODO: DAVID is this ok??????????????????
				System.out.println("You are now following " + extraInfo.get(0));
				pollCommand();
				break;
				
			}
			case UNFOLLOW: 
			case BLOCK: 
			case LOGOUT:
			default:
				break;
			}
			
		} else { // NOT SUCCESSFUL
			switch(op){		
			case CREATE: 
				String user = extraInfo.get(0);
				try {
					if (nfsService.exists(user + "_followers.txt")) {
						System.out.println("User " + user + " already exists.");
					} else {
						knownCommand("create " + extraInfo.get(0));
					}
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			case LOGIN: 
			case TWEET: 
				knownCommand("tweet " + extraInfo.get(0));
				break;
			case READTWEETS: {
				knownCommand("readtweets"); // Retry the transaction.
				break;
			}
			case FOLLOW: {
				String followUserName = extraInfo.get(0);
				try {
					if (nfsService.exists(followUserName + "_followers.txt")) {
						knownCommand("follow " + followUserName + "_followers.txt");
					} else {
						System.out.println("The user " + followUserName + " does not exist.");
						pollCommand();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			}
			case UNFOLLOW: 
			case BLOCK: 
			case LOGOUT:
			default:
				break;
			}
		}
		
	}	

	public void pollCommand() {
		if (commandQueue.size() > 0) {
			knownCommand(commandQueue.poll());
		}
	}
}
