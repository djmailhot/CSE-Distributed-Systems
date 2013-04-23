import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class TwitterNode extends RPCNode {
	private String username = null; 
	private int TEMP_TO_ADDRESS = 0; // NOT SURE HOW TO KNOW THE DESTINATION ADDRESS
	
	
	public TwitterNode() {
		super();
	}
	
	private enum TwitterOp {
		LOGIN {
			public void display(String param, boolean exists) {
				if (exists) {
					System.out.println("Login successful for " + param + ".");
				} else {
					System.out.println("Error! Please create the user.");
				}
			}
		},	
		LOGOUT {
			public void display(String param, boolean success) {
				System.out.println("Logout successful.");
			}
		},	
		CREATE {
			public void display(String param, boolean success) {
				System.out.println("Sucessfully created " + param + ".");				
			}
		},		
		TWEET {
			public void display(String param, boolean success) {
				System.out.println("You tweeted: " + param);
			}
		},		
		READTWEETS {
			public void display(String param, boolean success) {
					System.out.println(param);
			}
		},	
		FOLLOW {
			public void display(String param, boolean success) {
				System.out.println("You are now following " + param + ".");
			}
		},		
		UNFOLLOW {
			public void display(String param, boolean success) {
				System.out.println("You are no longer following " + param + ".");
			}
		},		
		BLOCK {
			public void display(String param, boolean success) {
				System.out.println(param + " is no longer following you.");
			}
		};
		
		public abstract void display(String param, boolean success);
	}
	
	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onCommand(String command) {
		if(command != null && knownCommand(command.toLowerCase())){
			return;
		}

		System.err.println("Unrecognized command: " + command);
	}
	
	private boolean knownCommand(String command) {
		String[] parsedCommand = command.split(" ");
		String commandName = parsedCommand[0];
		if(commandName.equals("login")) {
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			} else {
				login(parsedCommand[1]);
			}
			return true;
		} else if (commandName.equals("logout")) {
			logout();	
			return true;
		} else if (commandName.equals("create")) {
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			} else {
				create(parsedCommand[1]);
			}
			return true;
		} else if (commandName.equals("tweet")) {
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a tweet.");
			}
			tweet(command.substring(5).trim());
			return true;
		} else if (commandName.equals("readtweets")) {
			readTweets();
			return true;
		} else if (commandName.equals("follow")) {
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			} else {
				follow(parsedCommand[1]);
			} 
			return true;
		} else if (commandName.equals("unfollow")) {
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			} else {
				unfollow(parsedCommand[1]);
			}
			return true;
		} else if (commandName.equals("block")) {
			if (parsedCommand.length < 2) {
				System.err.println("Must supply a username.");
			} else {
				block(parsedCommand[1]);
			}
			return true;
		}
		return false;
	}
	
	private void create(String user) {
		// tell server to create files for user.
		// append users, user 
		// create user_followers // those who are following this user
		// create user_stream    // this user's unread tweets
		JSONObject append = transactionAppend("users.txt", user);
		JSONObject cfollowers = transactionCreate(user + "_followers.txt");
		JSONObject cstream = transactionCreate(user + "_stream.txt");
		// TODO how do I know the address?
		List<UUID> uuids = RPCSend(TEMP_TO_ADDRESS, new ArrayList<JSONObject>(Arrays.asList(append, cfollowers, cstream)));
		mapUUIDs(uuids, TwitterOp.CREATE, user);
		System.out.println("create user RPC sent");
	}
	
	private void login(String user) {
		// CHECK_EXISTENCE of user_followers.txt
		JSONObject existance = transactionExist(user + "_followers.txt");
		// TODO how do I know the address?
		List<UUID> uuids = RPCSend(TEMP_TO_ADDRESS, new ArrayList<JSONObject>(Arrays.asList(existance)));
		mapUUIDs(uuids, TwitterOp.LOGIN, user);
	}
	
	private void logout() {
		username = null;
		System.out.println("Logout successful.");
	}
	
	private void tweet(String tweet){
		// send tweet to server
		// READ the file user_followers
		JSONObject read = transactionRead(username + "_followers.txt");
		List<UUID> uuids = RPCSend(TEMP_TO_ADDRESS, new ArrayList<JSONObject>(Arrays.asList(read)));
		mapUUIDs(uuids, TwitterOp.TWEET, tweet);
		System.out.println("read followers RPC sent");
	}
	
	private void readTweets() {
		// read tweets from server
		// READ username_stream
		// DELETE username_stream // holds only unread tweets
		JSONObject read = transactionRead(username + "_stream.txt");
		JSONObject delete = transactionDelete(username + "_stream.txt");
		// TODO how do I know the address?
		List<UUID> uuids = RPCSend(TEMP_TO_ADDRESS, new ArrayList<JSONObject>(Arrays.asList(read, delete)));
		mapUUIDs(uuids, TwitterOp.READTWEETS, null);
		System.out.println("read tweets RPC sent");
	}
	
	private void follow(String followUserName) {
		// tell server to follow followUserName
		// APPEND username, followUserName_followers
		JSONObject append = transactionAppend(followUserName + "_followers.txt", username);
		List<UUID> uuids = RPCSend(TEMP_TO_ADDRESS, new ArrayList<JSONObject>(Arrays.asList(append)));
		mapUUIDs(uuids, TwitterOp.FOLLOW, followUserName);
		System.out.println("follow append RPC sent");
	}
	
	private void unfollow(String unfollowUserName) {
		// tell server to delete unfollowUserName from following
		// DELETE_LINE username, unfollowUserName_followers
		JSONObject delete = transactionDeleteLine(unfollowUserName + "_followers.txt", username);
		List<UUID> uuids = RPCSend(TEMP_TO_ADDRESS, new ArrayList<JSONObject>(Arrays.asList(delete)));
		mapUUIDs(uuids, TwitterOp.UNFOLLOW, unfollowUserName);
		System.out.println("unfollow delete RPC sent");
	}
	
	private void block(String blockUserName) {
		// tell server to delete username from blockUserName's following list
		// DELETE_LINE blockUserName, username_followers
		JSONObject delete = transactionDeleteLine(username + "_followers.txt", blockUserName);
		List<UUID> uuids = RPCSend(TEMP_TO_ADDRESS, new ArrayList<JSONObject>(Arrays.asList(delete)));
		mapUUIDs(uuids, TwitterOp.BLOCK, blockUserName);
		System.out.println("block delete RPC sent");
	}
	
	private Map<UUID, Pair<TwitterOp, String>> uuidmap = new HashMap<UUID, Pair<TwitterOp, String>>();
	private Map<UUID, List<UUID>> transactionsmap = new HashMap<UUID, List<UUID>>();
	
	private void mapUUIDs(List<UUID> uuids, TwitterOp op, String extraInfo){
		for (UUID uuid : uuids) {
			uuidmap.put(uuid, Pair.of(op, extraInfo));
		}
		if (uuids.size() > 1) {
			for (UUID uuid : uuids) {
				transactionsmap.put(uuid, uuids); 
			}
		}
	}

	@Override
	public void onRPCResponse(Integer from, JSONObject transaction) {
		UUID uuid = extractUUID(transaction);
		Pair<TwitterOp, String> p = uuidmap.remove(uuid);
		if (p == null) { return; } // We are not expecting this response.
		
		TwitterOp op = p.a;
		String extraInfo = p.b;
		boolean success;
		try {
			success = Boolean.parseBoolean(transaction.getString("success"));
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
					op.display(extraInfo, success);
				}
			}			
			break;
		}
		case LOGIN: {
			Boolean exists;
			try {
				exists = Boolean.parseBoolean(transaction.getString("data"));
			} catch (JSONException e) {
				exists = false;
			}
			username = exists ? extraInfo : username;
			op.display(extraInfo, exists);
			break;
		}			
		case TWEET: {
			// if we just read the list of followers, we still have to post to their streams.
      NFSOperation operation = extractNFSOperation(transaction);
			if (operation == NFSOperation.READ) {
				// TODO:
				String[] followers;
				try {
					followers = transaction.getString("data").split("\n");
				} catch (JSONException e) {
					followers = new String[0];
				}
				ArrayList<JSONObject> appends = new ArrayList<JSONObject>();
				for (String follower : followers) {
					JSONObject append = transactionAppend(follower + "_stream.txt", username + ": " + extraInfo);
					appends.add(append);
				}
				List<UUID> uuids = RPCSend(TEMP_TO_ADDRESS, appends);
				mapUUIDs(uuids, TwitterOp.TWEET, extraInfo);
			} else { // We heard back from a tweet append. Check if all the appends are back.
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
						op.display(extraInfo, success);
					}
				}			
			}
			break;
		}
		case READTWEETS: {
			String file;
			try {
				file = transaction.getString("data"); //Assume key "data", assume gives file as one string.
			} catch (JSONException e) {
				file = "You have no unread tweets.";
			} 
			op.display(file, success);
			break;
		}
		case FOLLOW:
		case UNFOLLOW: 
		case BLOCK: {
			op.display(extraInfo, success);
			break;
		}
		case LOGOUT:
		default:
			break;
		}
	}
}
