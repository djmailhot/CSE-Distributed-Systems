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
	
	
	private void create(String user) {
		waitingForResponse = true;
		// tell server to create files for user.
		// append users, user 
		// create user_followers // those who are following this user
		// create user_stream    // this user's unread tweets
		//JSONObject append = transactionAppend("users.txt", user);
		//JSONObject cfollowers = transactionCreate(user + "_followers.txt");
		//JSONObject cstream = transactionCreate(user + "_stream.txt");
		// TODO how do I know the address?
		//List<UUID> uuids = RPCSend(DEST_ADDR, new ArrayList<JSONObject>(Arrays.asList(cfollowers, cstream)));
		//mapUUIDs(uuids, TwitterOp.CREATE, user);
		System.out.println("create user RPC sent");
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
	
	private void tweet(String tweet){
		waitingForResponse = true;
		// send tweet to server
		// READ the file user_followers
		//JSONObject read = transactionRead(username + "_followers.txt");
		//List<UUID> uuids = RPCSend(DEST_ADDR, new ArrayList<JSONObject>(Arrays.asList(read)));
		//mapUUIDs(uuids, TwitterOp.TWEET, tweet);
		System.out.println("read followers RPC sent");
	}
	
	private void readTweets() {
		waitingForResponse = true;
		// read tweets from server
		// READ username_stream
		// DELETE username_stream // holds only unread tweets
		//JSONObject read = transactionRead(username + "_stream.txt");
		//JSONObject delete = transactionDelete(username + "_stream.txt");
		// TODO how do I know the address?
		//List<UUID> uuids = RPCSend(DEST_ADDR, new ArrayList<JSONObject>(Arrays.asList(read, delete)));
		//mapUUIDs(uuids, TwitterOp.READTWEETS, null);
		
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
	
	private void follow(String followUserName) {
		waitingForResponse = true;
		// tell server to follow followUserName
		// APPEND username, followUserName_followers
		//JSONObject append = transactionAppend(followUserName + "_followers.txt", username);
		//List<UUID> uuids = RPCSend(DEST_ADDR, new ArrayList<JSONObject>(Arrays.asList(append)));
		//mapUUIDs(uuids, TwitterOp.FOLLOW, followUserName);
		System.out.println("follow append RPC sent");
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
		//for (Integer uuid : uuids) {
			uuidmap.put(uuid, Pair.of(op, extraInfo));
		//}
		//if (uuids.size() > 1) {
		//	for (UUID uuid : uuids) {
		//		transactionsmap.put(uuid, uuids); 
		//	}
		//}
	}

	//public void onRPCResponse(Integer from, JSONObject transaction) {
		/*//UUID uuid = extractUUID(transaction);
		//Pair<TwitterOp, String> p = uuidmap.remove(uuid);
		//if (p == null) { 
			//this.RIOLayer.responseFinalized(uuid, MsgLogger.RECV); 
			return;
		//} // We are not expecting this response.
		
		//TwitterOp op = p.a;
		//String extraInfo = p.b;
		boolean success;
		try {
			success = transaction.getBoolean("success");
		} catch (JSONException e) {
			success = false;
		}
		
		// process the response
		switch(op) {
		case CREATE: {
			List<UUID> transactionuuids = transactionsmap.remove(uuid);
			if (transactionuuids != null) {
				// check if the other RPCs in this bundle have finished.
				boolean finished = true;
				for (UUID other : transactionuuids) {
					if (transactionsmap.containsKey(other)) { // TODO: do I need locking here?
						finished = false;
					}
				}
				if (finished) {
					waitingForResponse = false;
					op.display(extraInfo, success);
					if (commandQueue.size() > 0) {
						knownCommand(commandQueue.poll());
					}
				}
			}			
			break;
		}
		case LOGIN: {
			Boolean exists;
			try {
				exists = transaction.getBoolean("exists");
			} catch (JSONException e) {
				exists = false;
			}
			username = exists ? extraInfo : username;
			if (username != null) {
				try {
					nfsService.write("username.txt", username);
				} catch (IOException e) {
				}
			}
			waitingForResponse = false;
			op.display(extraInfo, exists);
			if (commandQueue.size() > 0) {
				knownCommand(commandQueue.poll());
			}
			break;
		}			
		case TWEET: {
			/* // if we just read the list of followers, we still have to post to their streams.
      //NFSOperation operation = extractNFSOperation(transaction);
			//if (operation == NFSOperation.READ) {
				//List<String> followers = extractFilelines(transaction);
				//if (followers.size() > 0) {
					ArrayList<JSONObject> appends = new ArrayList<JSONObject>();
					//for (String follower : followers) {
						//JSONObject append = transactionAppend(follower + "_stream.txt", username + ": " + extraInfo);
						//appends.add(append);
					//}
					//List<UUID> uuids = RPCSend(DEST_ADDR, appends);
					mapUUIDs(uuids, TwitterOp.TWEET, extraInfo);
				//} else { // no followers
					waitingForResponse = false;
					op.display(extraInfo, success);
					if (commandQueue.size() > 0) {
						knownCommand(commandQueue.poll());
					}
				//}
			//} else { // We heard back from a tweet append. Check if all the appends are back.
				List<UUID> transactionuuids = transactionsmap.remove(uuid);
				boolean finished = true;
				if (transactionuuids != null) {
					// check if the other RPCs in this bundle have finished.
					for (UUID other : transactionuuids) {
						if (transactionsmap.containsKey(other)) { // TODO: do I need locking here?
							finished = false;
						}
					}
				}
				if (finished) {
					waitingForResponse = false;
					op.display(extraInfo, success);
					if (commandQueue.size() > 0) {
						knownCommand(commandQueue.poll());
					}
				}			
			}
			break;
		}
		case READTWEETS: {
			System.out.println("OMG I AM READING TWEETS RIGHT NOWWWWW");
			//NFSOperation operation = extractNFSOperation(transaction);
			//if (operation == NFSOperation.READ) {
			//List<String> file = extractFilelines(transaction);
				StringBuilder sb = new StringBuilder();
				//for (String s : file) {
				//	sb.append(s + "\n");
				//}
				waitingForResponse = false;
				//String tweets = sb.toString();
				//if (tweets.equals("")) {
				//	tweets = "You have no unread tweets";
				//}
				//op.display(tweets, success);
				if (commandQueue.size() > 0) {
					knownCommand(commandQueue.poll());
				}
			//}
			break;
		}
		case FOLLOW:
		case UNFOLLOW: 
		case BLOCK: {
			waitingForResponse = false;
			op.display(extraInfo, success);
			if (commandQueue.size() > 0) {
				knownCommand(commandQueue.poll());
			}
			break;
		}
		case LOGOUT:
		default:
			break;
		}
		
		this.RIOLayer.responseFinalized(uuid, MsgLogger.RECV);
		*/
	//}


	// Assumes cache is up to date
	@Override
	public void onMCCResponse(Integer from, int tid, boolean success) {
		Pair<TwitterOp, List<String>> p = uuidmap.remove(tid);
		TwitterOp op = p.a;
		List<String> extraInfo = p.b;
		
		if (success) {
			switch(op){		
			case CREATE: 
			case LOGIN: 
			case TWEET: 
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
					waitingForResponse = false;
					if (commandQueue.size() > 0) {
						knownCommand(commandQueue.poll());
					}
				break;
			}
			case FOLLOW:
			case UNFOLLOW: 
			case BLOCK: 
			case LOGOUT:
			default:
				break;
			}
			
		} else { // not successful
			switch(op){		
			case CREATE: 
			case LOGIN: 
			case TWEET: 
				break;
			case READTWEETS: {
				knownCommand("readtweets"); // Retry the transaction.
				break;
			}
			case FOLLOW:
			case UNFOLLOW: 
			case BLOCK: 
			case LOGOUT:
			default:
				break;
			}
		}
		
	}
}
