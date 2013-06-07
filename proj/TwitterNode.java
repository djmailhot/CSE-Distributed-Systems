import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import edu.washington.cs.cse490h.lib.Utility;
import plume.Pair;

/*
 * Assumptions
 * 
 * - The cache is always up to date when on MCCResponseIsCalled
 * - There are no malicious clients. (I.E. no one will delete the currently logged in user.)
 */
public class TwitterNode extends MCCNode {
	private String TAG;
	private String username = null;
	private byte[] userToken = null;
	private ClientCommandLogger ccl;
	
	private String TWEET_FILE = "current_tweets.txt";
	private String USER_FILE = "username.txt";
	
	boolean waitingForResponse = false;
	private Map<Integer, Pair<TwitterOp, List<String>>> idMap = new HashMap<Integer, Pair<TwitterOp, List<String>>>();
	Queue<Pair<String, Integer>> commandQueue = new LinkedList<Pair<String, Integer>>();
	
	// Ignore disk crashes
  public static double getFailureRate() { return 0/100.0; }
	public static double getDropRate() { return 0/100.0; }
	//public static double getDelayRate() { return 0/100.0; }
	public static double getRecoveryRate() { return 100/100.0; }


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
	
	int count = 0;
	
	public void start() {	
    this.TAG = String.format("TwitterNode.%d", addr);

		System.out.println("TwitterNode " + addr + " starting.");
		idMap = new HashMap<Integer, Pair<TwitterOp, List<String>>>();
		List<String> file;
		try {
			file = nfsService.read(USER_FILE);
			if (file == null) {
				file = nfsService.read("_cow_" + USER_FILE);  // HACK
			}
			
			List<Pair<String, Integer>> loggedCommands = this.ccl.loadLogs();
			for(Pair<String, Integer> s : loggedCommands){
				this.commandQueue.add(s);
			}
			
		} catch (IOException e) {
			file = null;
		}
		
		//load username and login token from file, if we have already logged in
		username = (file == null || file.size() == 0) ? null : file.get(0);
		userToken = (file == null || file.size() == 0) ? null : Utility.hexStringToByteArray(file.get(1));
		
		if (username == null && count > 0) {
			throw new RuntimeException();
		}
		count++;
		super.start();
		
		System.out.println("username: " + username);
		if (commandQueue.size() > 0) {
			Pair<String, Integer> commandAndTid = commandQueue.peek();
			doCommand(commandAndTid.a, commandAndTid.b);
		}
	}

	@Override
	public void onCommand(String command) {
		int transactionId = edu.washington.cs.cse490h.lib.Utility.getRNG().nextInt();
    transactionId = Math.abs(transactionId);
		if(command != null && knownCommand(command.toLowerCase(), transactionId)){
			if (doCommand(command.toLowerCase(), transactionId)) {
				return;
			}
		}

		System.err.println("Unrecognized command: " + command + ", username: " + username);
	}
	
	private void offerIfUnique(Pair<String, Integer> transaction) {
		if(!commandQueue.contains(transaction)) {
			commandQueue.offer(transaction);
		}
	}
	
	// TODO change create user and login to require a password
	private boolean doCommand(String command, int transactionId) {
		RIOLayer.responseFinalized(transactionId); // If we're retrying, we're done with the old response.
		if (command == null) { return false; }
		
		String[] parsedCommand = command.split(" ");
		String commandName = parsedCommand[0];
		if(commandName.equals("login")) {
			offerIfUnique(new Pair(command, transactionId));
				if (waitingForResponse) {
					System.out.println("Please wait!!");
				} else {
					System.out.println("Logging in!!!!");
					login(parsedCommand[1], parsedCommand[2], transactionId);
				}
			
			return true;
		} else if (commandName.equals("logout") && username != null) {
			offerIfUnique(new Pair(command, transactionId));
			if (waitingForResponse) {
				System.out.println("Please wait!!");
			} else {
				logout(transactionId);	
			}
			return true;
		} else if (commandName.equals("create")) {
			Log.i(TAG, "DO COMMAND CREATE");
			offerIfUnique(new Pair(command, transactionId));
				if (waitingForResponse) {
					System.out.println("Please wait!!");
				} else {
					try{
						create(parsedCommand[1], parsedCommand[2], transactionId);
					} catch (ArrayIndexOutOfBoundsException e) {
						e.printStackTrace();
					}
				}
			return true;
		} else if (commandName.equals("tweet") && username != null) {
			offerIfUnique(new Pair(command, transactionId));
			if (waitingForResponse) {
				System.out.println("Please wait!!");
			} else {
				tweet(command.substring(5).trim(), transactionId);
			}
			return true;
		} else if (commandName.equals("readtweets") && username != null) {
			offerIfUnique(new Pair(command, transactionId));
			if (waitingForResponse) {
				System.out.println("Please wait!!");
			} else {
				readTweets(transactionId);
			}
			return true;
		} else if (commandName.equals("follow") && username != null) {
			offerIfUnique(new Pair(command, transactionId));
			if (waitingForResponse) {
				System.out.println("Please wait!!");
			} else {
				follow(parsedCommand[1], transactionId);
			}
			return true;
		} else if (commandName.equals("unfollow") && username != null) {
			offerIfUnique(new Pair(command, transactionId));
			if (waitingForResponse) {
				System.out.println("Please wait!!");
			} else {
				unfollow(parsedCommand[1], transactionId);
			}
			return true;
		} else if (commandName.equals("block") && username != null) {
			offerIfUnique(new Pair(command, transactionId));
			if (waitingForResponse) {
				System.out.println("Please wait!!");
			} else {
				block(parsedCommand[1], transactionId);
			}
			return true;
		}
		return false;
	}
	
	private boolean knownCommand(String command, int transactionId) {
		if (command == null) { return false; }
		
		String[] parsedCommand = command.split(" ");
		String commandName = parsedCommand[0];
		if(commandName.equals("login")) {
			this.ccl.logCommand(command, transactionId);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			}
			System.out.println("Login is a known command");
			return true;
		} else if (commandName.equals("logout") && username != null) {
			this.ccl.logCommand(command, transactionId);
			return true;
		} else if (commandName.equals("create")) {
			this.ccl.logCommand(command, transactionId);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			}
			return true;
		} else if (commandName.equals("tweet") && username != null) {
			this.ccl.logCommand(command, transactionId);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a tweet.");
			}
			return true;
		} else if (commandName.equals("readtweets") && username != null) {
			this.ccl.logCommand(command, transactionId);
			return true;
		} else if (commandName.equals("follow") && username != null) {
			this.ccl.logCommand(command, transactionId);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			}
			return true;
		} else if (commandName.equals("unfollow") && username != null) {
			this.ccl.logCommand(command, transactionId);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			}
			return true;
		} else if (commandName.equals("block") && username != null) {
			this.ccl.logCommand(command, transactionId);
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			}
			return true;
		}
		return false;
	}
	
	
	private void create(String user, String password, int transactionId) {
		waitingForResponse = true;
		String filename = user + "_followers.txt";

		//create(filename);
		mapUUIDs(transactionId, TwitterOp.CREATE, Arrays.asList(user));
		Log.i(TAG, "idMap: " + idMap);
		
		NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId, this.userToken);
		b.createFile_newUser(filename, user, password);
		
		submitTransaction(b.build());
		Log.i(TAG, "SENDING SUBMIT TRANSACTION FOR CREATE USER: 	" + user);
		System.out.println("create user commit sent"); 
	}
	
	private void login(String user, String password, int transactionId) {
		waitingForResponse = true;
		String filename = user + "_followers.txt";
    Log.v(TAG, "OMGZBBQMONSTAR LOGGING IN");
		
		ArrayList<String> args = new ArrayList<String>(2);
		args.add(user);
		args.add(password);
		mapUUIDs(transactionId, TwitterOp.LOGIN, args);
    Log.d(TAG, String.format("security token %s", args));
		
		NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId,(user + "|" + password).getBytes());
		b.touchFile(filename);
		
		submitTransaction(b.build());
		System.out.println("login user commit sent"); 
	}
	
	private void logout(int transactionId) {
		try {
			nfsService.delete("username.txt");
			username = null;
		} catch (IOException e) {
		}
		System.out.println("Logout successful.");
		pollCommand(transactionId); // Next command.
	}
	
	private void tweet(String tweet, int transactionId){//done
		waitingForResponse = true;
		try {
			String filename = username + "_followers.txt";
			List<String> followers = read(filename); // read the cached copy of the followers
	
			NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId, this.userToken);
			b.touchFile(filename);
			
			if (followers != null) {
				for (String follower : followers) {
					b.appendLine(follower + "_stream.txt", username + ": " + tweet);
				}
			}
			mapUUIDs(transactionId, TwitterOp.TWEET, Arrays.asList(tweet));
			
			submitTransaction(b.build());
			System.out.println("read tweets commit sent");
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	private void readTweets(int transactionId) {
		waitingForResponse = true;
		try {
			String filename = username + "_stream.txt";
			List<String> tweets = read(filename); // read the cached copy of the tweets
			if (tweets != null) {
				for (String tweet : tweets) {
					nfsService.append(TWEET_FILE, tweet); // save the tweets on disk. 
				}
			}
			mapUUIDs(transactionId, TwitterOp.READTWEETS, null);
			
			NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId, this.userToken);
			b.touchFile(filename);
			b.deleteFile(filename);
			
			submitTransaction(b.build());
			System.out.println("read tweets commit sent");
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	private void follow(String followUserName, int transactionId) {
		waitingForResponse = true;
			String filename = followUserName + "_followers.txt";

			NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId, this.userToken);
			b.touchFile(filename);
			b.appendLine(filename, username);
			
			mapUUIDs(transactionId, TwitterOp.FOLLOW, Arrays.asList(followUserName));
			
			submitTransaction(b.build());
			System.out.println("follow " + followUserName + " commit sent");
	}
	
	private void unfollow(String unfollowUserName, int transactionId) {
		waitingForResponse = true;
		String filename = unfollowUserName + "_followers.txt";

		NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId, this.userToken);
		b.deleteLine(filename, username);
		
		mapUUIDs(transactionId, TwitterOp.UNFOLLOW, Arrays.asList(unfollowUserName));
		
		submitTransaction(b.build());
		System.out.println("unfollow " + unfollowUserName + " commit sent"); 
	}
	
	private void block(String blockUserName, int transactionId) {
		waitingForResponse = true;
		String filename = username + "_followers.txt";

		NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId, this.userToken);
		b.deleteLine(filename, blockUserName);
		
		mapUUIDs(transactionId, TwitterOp.BLOCK, Arrays.asList(blockUserName));
		
		submitTransaction(b.build());
		System.out.println("block " + blockUserName + " commit sent"); 
	}
	
	private void mapUUIDs(Integer uuid, TwitterOp op, List<String> extraInfo){
		idMap.put(uuid, Pair.of(op, extraInfo));
	}

	// Assumes cache is up to date
	@Override
	public void onMCCResponse(Integer from, int tid, boolean success, Pair<Boolean,byte[]> securityResponse) {
		Log.i(TAG, "From: " + from + ", tid: "+ tid + ", success: "+ success + ", securityResponse");
    Log.d(TAG, idMap.toString());
		waitingForResponse = false;
		Pair<TwitterOp, List<String>> p = idMap.remove(tid);
		if (p == null) {
			// If we don't have the info, retry the command. 
			// If it already happened on the server, it will not do it twice.
			Pair<String, Integer> peek = commandQueue.peek();
			if (peek != null) {
				Log.i(TAG, "tid: " + tid);
				Log.i(TAG, "idMap: " + idMap);
				Log.i(TAG, "POOP");
				doCommand(peek.a, peek.b);
			}
			return;
		} 
		
		TwitterOp op = p.a;
		List<String> extraInfo = p.b;
		
		if (extraInfo != null && extraInfo.size() > 0 && extraInfo.get(0).equals("ALREADY_COMPLETED")) {
			return; // we have already displayed the results of this transaction to the user.
		}
		
		if (success && securityResponse.a) {
			switch(op){		
			case CREATE: 
				System.out.println("You created user " + extraInfo.get(0));
				Log.i(TAG, "Successfully created user. Waterbottle.");
				pollCommand(tid);
				break;
			case LOGIN: 
				username = extraInfo.get(0);

        Log.d(TAG, String.format("security response %s", securityResponse));
				userToken = securityResponse.b;
        String userTokenString = Utility.bytesToHexString(userToken);
				String filename = username + "_followers.txt";
				try {
					if (exists(filename)) {
						System.out.println("You are logged in as " + username);
						
            Log.d(TAG, String.format("SECURITY response %s", securityResponse));
						nfsService.delete(USER_FILE);
			            nfsService.append(USER_FILE, username + "\n" + userTokenString); 
						
					} else {
						nfsService.delete(USER_FILE);
						username = null;
						System.out.println("User " + extraInfo.get(0) + " does not exist.");
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				pollCommand(tid);
				break;
			case TWEET: 
				System.out.println("You tweeted " + extraInfo.get(0));
				pollCommand(tid);
				break;
			case READTWEETS: {
				// We successfully read and deleted the stream file, so now display the tweets to the user.
				List<String> tweets;
				try {
					tweets = nfsService.read(TWEET_FILE);
				if (tweets != null && tweets.size() > 0) {
					for (String tweet : tweets) {
						System.out.println(tweet);
					}
				} else {
					System.out.println("You have no unread tweets.");
				}
				nfsService.delete(TWEET_FILE);
				} catch (IOException e) {
					e.printStackTrace();
				}
				pollCommand(tid);
				break;
			}
			case FOLLOW: {
				System.out.println("You are now following " + extraInfo.get(0));
				pollCommand(tid);
				break;
				
			}
			case UNFOLLOW: 
				System.out.println("You are no longer following " + extraInfo.get(0));
				pollCommand(tid);
				break;
			case BLOCK: 
				System.out.println("You have blocked " + extraInfo.get(0));
				pollCommand(tid);
			case LOGOUT:
			default:
				break;
			}
		} else if (!securityResponse.a){//we tried something not secure; abort.
			System.out.println("Server responded that we are doing something not allowed by the security model!!");
			pollCommand(tid);
		} else { // NOT SUCCESSFUL for some non-security reason
			switch(op){		
			case CREATE: 
				String user = extraInfo.get(0);
				try {
					if (exists(user + "_followers.txt")) {
						System.out.println("User " + user + " already exists."); // Abort
						Log.i(TAG, "Unsuccessfully created user. Turtle.");
						pollCommand(tid);
					} else {
						doCommand("create " + extraInfo.get(0), tid); // Retry the transaction.
					}
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				break;
			case LOGIN: 
				user = username != null? username : extraInfo.get(0);
				try {
					if (exists(user + "_followers.txt")) {
						System.out.println("You are logged in as " + username);
					} else {
						System.out.println("User " + user + " does not exist.");
						nfsService.delete(USER_FILE);
						username = null;
					}
					pollCommand(tid);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				break;
			case TWEET: 
				doCommand("tweet " + extraInfo.get(0), tid); // Retry the transaction.
				break;
			case READTWEETS: {
				doCommand("readtweets", tid); // Retry the transaction.
				break;
			}
			case FOLLOW: {
				String followUserName = extraInfo.get(0);
				try {
					if (exists(followUserName + "_followers.txt")) {
						doCommand("follow " + followUserName, tid); // Retry the transaction. Just out of date.
					} else {
						System.out.println("The user " + followUserName + " does not exist."); // Abort.
						pollCommand(tid);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			}
			case UNFOLLOW: {
				String unFollowUserName = extraInfo.get(0);
				try {
					if (exists(unFollowUserName + "_followers.txt")) {
						doCommand("unfollow " + unFollowUserName, tid); // Retry the transaction. Just out of date.
					} else {
						System.out.println("The user " + unFollowUserName + " does not exist."); // Abort.
						pollCommand(tid);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;
			}
			case BLOCK: 
				knownCommand("block " + extraInfo.get(0), tid);
			case LOGOUT:
			default:
				break;
			}
		}
		
	}	

	private void pollCommand(int currentTid) {
		RIOLayer.responseFinalized(currentTid); // We're done with the old response.
		ccl.deleteLog(currentTid);
		Log.i(TAG, commandQueue.toString());
		commandQueue.poll(); // Dequeue the current command
		idMap.put(currentTid, new Pair(TwitterOp.LOGOUT, Arrays.asList("ALREADY_COMPLETED")));
		Log.i(TAG, "response finalized for: " + currentTid);
		Log.i(TAG, commandQueue.toString());
		if (commandQueue.size() > 0) {
			Pair<String, Integer> commandAndTid = commandQueue.peek();
			Log.i(TAG, commandAndTid.toString());
			doCommand(commandAndTid.a, commandAndTid.b);
		}
	}
}
