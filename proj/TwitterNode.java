

/*
 * I decided to collapse the client and server into the same node. Since the server is really lightweight
 * and I didnt want to spend time changing the tests to bew able to start different kinds of nodes.
 * 
 * If this is a problem, it will be easy enough to split them apart again. 
 */
// TODO: EVENTUALLY CHANGE TO  extends RPCCLIENTNODE
public class TwitterNode extends RIONode {
	private String username = null; 
	
	@Override
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		// TODO Auto-generated method stub

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
	
	public void create(String user) {
		// tell server to create files for user.
		// append users, user 
		// create user_followers // those who are following this user
		// create user_stream    // this user's unread tweets
		System.out.println("Sucessfully created " + user + ".");
	}
	
	public void login(String user) {
		// CHECK_EXISTENCE of user_stream
		boolean exists = true;
		if (exists) {
			username = user;
			System.out.println("Login successful for " + username + ".");
		} else {
			username = null;
			System.out.println("Error! Please create the user.");
		}
	}
	
	public void logout() {
		username = null;
		System.out.println("Logout successful.");
	}
	
	public void tweet(String tweet){
		// send tweet to server
		// READ the file user_followers
		// for each follower in user_followers
		//     APPEND tweet, follower_stream
		System.out.println("You tweeted: " + tweet);
	}
	
	public void readTweets() {
		// read tweets from server
		// READ username_stream
		// DELETE username_stream // holds only unread tweets
		// CREATE username_stream 
		System.out.println("It would be cool if you could read tweets");
	}
	
	public void follow(String followUserName) {
		// tell server to follow followUserName
		// APPEND username, followUserName_followers
		System.out.println("You are now following " + followUserName + ".");
	}
	
	public void unfollow(String unfollowUserName) {
		// tell server to delete unfollowUserName from following
		// DELETE_LINE username, unfollowUserName_followers
		System.out.println("You are no longer following " + unfollowUserName + ".");
	}
	
	public void block(String blockUserName) {
		// tell server to delete username from blockUserName's following list
		// DELETE_LINE blockUserName, username_followers
		System.out.println(blockUserName + " is no longer following you.");
	}

}
