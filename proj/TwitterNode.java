import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import plume.Pair;

/*
 * Assumptions
 * 
 * - The cache is always up to date when on MCCResponseIsCalled
 * - There are no malicious clients. (I.E. no one will delete the currently logged in user.)
 */
public class TwitterNode extends MCCNode {
	private String username = null;  // TODO: change back to null
	private int DEST_ADDR = addr == 0? 1 : 0; // Copied from TwoGenerals.java
	private ClientCommandLogger ccl;
	
	
	boolean waitingForResponse = false;
	private Map<Integer, Pair<TwitterOp, List<String>>> idMap = new HashMap<Integer, Pair<TwitterOp, List<String>>>();
	Queue<Pair<String, Integer>> commandQueue = new LinkedList<Pair<String, Integer>>();
	
	// Ignore disk crashes
	/*
  public static double getFailureRate() { return 0/100.0; }
	public static double getDropRate() { return 0/100.0; }
	public static double getDelayRate() { return 0/100.0; }
	*/
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
		System.out.println("TwitterNode " + addr + " starting.");
		idMap = new HashMap<Integer, Pair<TwitterOp, List<String>>>();
		List<String> file;
		try {
			file = nfsService.read("username.txt");
			
			List<Pair<String, Integer>> loggedCommands = this.ccl.loadLogs();
			for(Pair<String, Integer> s : loggedCommands){
				this.commandQueue.add(s);
			}
			
		} catch (IOException e) {
			file = null;
		}
		username = (file == null || file.size() == 0) ? null : file.get(0);
		if (username == null && count > 0) {
			throw new RuntimeException();
		}
		count++;
		System.out.println("username: " + username);
		super.start();
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
	
	private boolean doCommand(String command, int transactionId) {
		RIOLayer.responseFinalized(transactionId); // If we're retrying, we're done with the old response.
		if (command == null) { return false; }
		
		String[] parsedCommand = command.split(" ");
		String commandName = parsedCommand[0];
		if(commandName.equals("login")) {
				if (waitingForResponse) {
					System.out.println("Please wait!!");
					commandQueue.offer(new Pair(command, transactionId));
				} else {
					System.out.println("Logging in!!!!");
					login(parsedCommand[1], transactionId);
				}
			
			return true;
		} else if (commandName.equals("logout") && username != null) {
			if (waitingForResponse) {
				System.out.println("Please wait!!");
				commandQueue.offer(new Pair(command, transactionId));
			} else {
				logout(transactionId);	
			}
			return true;
		} else if (commandName.equals("create")) {
				if (waitingForResponse) {
					System.out.println("Please wait!!");
					commandQueue.offer(new Pair(command, transactionId));
				} else {
					create(parsedCommand[1], transactionId);
				}
			return true;
		} else if (commandName.equals("tweet") && username != null) {
			if (waitingForResponse) {
				System.out.println("Please wait!!");
				commandQueue.offer(new Pair(command, transactionId));
			} else {
				tweet(command.substring(5).trim(), transactionId);
			}
			return true;
		} else if (commandName.equals("readtweets") && username != null) {
			if (waitingForResponse) {
				System.out.println("Please wait!!");
				commandQueue.offer(new Pair(command, transactionId));
			} else {
				readTweets(transactionId);
			}
			return true;
		} else if (commandName.equals("follow") && username != null) {
			if (waitingForResponse) {
				System.out.println("Please wait!!");
				commandQueue.offer(new Pair(command, transactionId));
			} else {
				follow(parsedCommand[1], transactionId);
			}
			return true;
		} else if (commandName.equals("unfollow") && username != null) {
			if (waitingForResponse) {
				System.out.println("Please wait!!");
				commandQueue.offer(new Pair(command, transactionId));
			} else {
				unfollow(parsedCommand[1], transactionId);
			}
			return true;
		} else if (commandName.equals("block") && username != null) {
			if (waitingForResponse) {
				System.out.println("Please wait!!");
				commandQueue.offer(new Pair(command, transactionId));
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
	
	
	private void create(String user, int transactionId) {//done
		waitingForResponse = true;
		String filename = user + "_followers.txt";
		//String streamFilename = user + "_stream.txt";

		//create(filename);
		mapUUIDs(transactionId, TwitterOp.CREATE, Arrays.asList(user));
		
		NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId);
		b.createFile(filename);
		//b.createFile(streamFilename); // ASSUME APPEND WILL CREATE THIS FILE
		
		submitTransaction(DEST_ADDR, b.build());
		System.out.println("create user commit sent"); 
	}
	
	private void login(String user, int transactionId) {//done
		waitingForResponse = true;
		String filename = user + "_followers.txt";
		List<String> exists = null;
		try {
			 exists = read(filename);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		mapUUIDs(transactionId, TwitterOp.LOGIN, Arrays.asList(user, exists.toString()));
		
		NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId);
		b.touchFile(filename);
		
		submitTransaction(DEST_ADDR, b.build());
		System.out.println("login user commit sent"); 
	}
	
	private void logout(int transactionId) {//done
		try {
			nfsService.delete("username.txt");
      username = null;
			//System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
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
	
			NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId);
			b.touchFile(filename);
			
			if (followers != null) {
				for (String follower : followers) {
					//append(follower + "_stream.txt", username + ": " + tweet);
					b.appendLine(follower + "_stream.txt", username + ": " + tweet);
				}
			}
			mapUUIDs(transactionId, TwitterOp.TWEET, Arrays.asList(tweet));
			
			submitTransaction(DEST_ADDR, b.build());
			System.out.println("read tweets commit sent");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private void readTweets(int transactionId) {//done
		waitingForResponse = true;
		try {
			String filename = username + "_stream.txt";
			List<String> tweets = read(filename); // read the cached copy of the tweets
			//nfsService.delete(filename);
			mapUUIDs(transactionId, TwitterOp.READTWEETS, tweets);
			
			NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId);
			b.touchFile(filename);
			b.deleteFile(filename);
			//b.createFile(filename); // ASSUME APPEND WILL CREATE THIS FILE
			
			submitTransaction(DEST_ADDR, b.build());
			System.out.println("read tweets commit sent");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private void follow(String followUserName, int transactionId) {// done
		waitingForResponse = true;
		try {
			String filename = followUserName + "_followers.txt";
			if (exists(filename)) {
				//nfsService.append(filename, username); // append to the cache copy
			}

			NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId);
			b.touchFile(filename); // will this fail if the file does not exist??? I hope so!
			b.appendLine(filename, username);
			
			mapUUIDs(transactionId, TwitterOp.FOLLOW, Arrays.asList(followUserName));
			
			submitTransaction(DEST_ADDR, b.build());
			System.out.println("follow " + followUserName + " commit sent");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private void unfollow(String unfollowUserName, int transactionId) {//done
		waitingForResponse = true;
		String filename = unfollowUserName + "_followers.txt";
		
		//nfsService.deleteLine(filename, username);

		NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId);
		b.deleteLine(filename, username);
		
		mapUUIDs(transactionId, TwitterOp.FOLLOW, Arrays.asList(unfollowUserName));
		
		submitTransaction(DEST_ADDR, b.build());
		System.out.println("unfollow " + unfollowUserName + " commit sent"); 
	}
	
	private void block(String blockUserName, int transactionId) {//done
		waitingForResponse = true;
		String filename = username + "_followers.txt";
		
		//nfsService.deleteLine(filename, blockUserName);

		NFSTransaction.Builder b = new NFSTransaction.Builder(transactionId);
		b.deleteLine(filename, blockUserName);
		
		mapUUIDs(transactionId, TwitterOp.FOLLOW, Arrays.asList(blockUserName));
		
		submitTransaction(DEST_ADDR, b.build());
		System.out.println("block " + blockUserName + " commit sent"); 
	}
	
	private void mapUUIDs(Integer uuid, TwitterOp op, List<String> extraInfo){
		idMap.put(uuid, Pair.of(op, extraInfo));
	}

	// Assumes cache is up to date
	@Override
	public void onMCCResponse(Integer from, int tid, boolean success) {
		waitingForResponse = false;
		Pair<TwitterOp, List<String>> p = idMap.remove(tid);
		TwitterOp op = p.a;
		List<String> extraInfo = p.b;
		
		if (op == TwitterOp.READTWEETS) {
			System.out.println("RESPONSE: " + tid + ", success: " + success + ", extraInfo: " + extraInfo);
		}
		
		if (success) {
			switch(op){		
			case CREATE: 
				System.out.println("You created user " + extraInfo.get(0));
				pollCommand(tid);
				break;
			case LOGIN: 
				username = extraInfo.get(0);
				String exists = extraInfo.get(1);
				try {
					if (!"null".equals(exists)) {
						nfsService.append("username.txt", username);
						System.out.println("You are logged in as " + username);
					} else {
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
				if (extraInfo != null && extraInfo.size() > 0) {
					for (String tweet : extraInfo) {
						System.out.println(tweet);
					}
				} else {
					System.out.println("You have no unread tweets.");
				}
				pollCommand(tid);
				break;
			}
			case FOLLOW: {
				// updateAllFiles(DEST_ADDR); // not needed anymore. probably. - David
				System.out.println("You are now following " + extraInfo.get(0));
				pollCommand(tid);
				break;
				
			}
			case UNFOLLOW: 
				// updateAllFiles(DEST_ADDR); // not needed anymore. probably. - David
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
			
		} else { // NOT SUCCESSFUL
			switch(op){		
			case CREATE: 
				String user = extraInfo.get(0);
				try {
					if (exists(user + "_followers.txt")) {
						System.out.println("User " + user + " already exists."); // Abort
						pollCommand(tid);
					} else {
						doCommand("create " + extraInfo.get(0), tid); // Retry the transaction.
					}
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			case LOGIN: 
				user = extraInfo.get(0);
				try {
					if (exists(user + "_followers.txt")) {
						doCommand("login " + user, tid);
					} else {
						System.out.println("User " + extraInfo.get(0) + " does not exist.");
						username = null;
						pollCommand(tid);
					}
				} catch (IOException e1) {
					// TODO Auto-generated catch block
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
					// TODO Auto-generated catch block
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
		if (commandQueue.size() > 0) {
			Pair<String, Integer> commandAndTid = commandQueue.poll();
			doCommand(commandAndTid.a, commandAndTid.b);
		}
	}
}
