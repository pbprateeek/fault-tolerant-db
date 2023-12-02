package server.faulttolerance;

import client.MyDBClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;
import server.AVDBReplicatedServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * This class should implement your replicated fault-tolerant database server if
 * you wish to use Zookeeper or other custom consensus protocols to order client
 * requests.
 * <p>
 * Refer to {@link server.ReplicatedServer} for a starting point for how to do
 * server-server messaging or to {@link server.AVDBReplicatedServer} for a
 * non-fault-tolerant replicated server.
 * <p>
 * You can assume that a single *fault-tolerant* Zookeeper server at the default
 * host:port of localhost:2181 and you can use this service as you please in the
 * implementation of this class.
 * <p>
 * Make sure that both a single instance of Cassandra and a single Zookeeper
 * server are running on their default ports before testing.
 * <p>
 * You can not store in-memory information about request logs for more than
 * {@link #MAX_LOG_SIZE} requests.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 */
	public static final int SLEEP = 5000;

	/**
	 * Set this to true if you want all tables drpped at the end of each run
	 * of tests by GraderFaultTolerance.
	 */
	public static final boolean DROP_TABLES_AFTER_TESTS=true;

	/**
	 * Maximum permitted size of any collection that is used to maintain
	 * request-specific state, i.e., you can not maintain state for more than
	 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
	 * ensure that your logs don't grow unbounded, which forces
	 * checkpointing to
	 * be implemented.
	 */
	public static final int MAX_LOG_SIZE = 400;

	public static final int DEFAULT_PORT = 2181;

	final private Session session;
	final private Cluster cluster;

	protected final String myID;
	protected final MessageNIOTransport<String,String> serverMessenger;

	protected String leader;

	// this is the message queue used to track which messages have not been sent yet
	private ConcurrentHashMap<Long, JSONObject> queue = new ConcurrentHashMap<Long, JSONObject>();
	private CopyOnWriteArrayList<String> notAcked;

	// the sequencer to track the most recent request in the queue
	private static long reqnum = 0;
	synchronized static Long incrReqNum() {
		return reqnum++;
	}

	// the sequencer to track the next request to be sent
	private static long expected = 0;
	synchronized static Long incrExpected() {
		return expected++;
	}
	private static final String ZOOKEEPER_HOST = "localhost:2181";
	private final String electionPath = "/election";

	boolean isLeader = false;
	private final String logDirectoryPath = System.getProperty("user.home") + "/mydb_server_logs";
	private String stateLogFilePath = "";

	private static MyNodeConfig myNodeConfig ;
	private static final ReentrantLock lock = new ReentrantLock();
	private static final ReentrantLock configLock = new ReentrantLock();

	private ZooKeeper zookeeper;
	private String leaderPath = "/leader";


	/**
	 * @param nodeConfig Server name/address configuration information read
	 *                      from
	 *                   conf/servers.properties.
	 * @param myID       The name of the keyspace to connect to, also the name
	 *                   of the server itself. You can not connect to any other
	 *                   keyspace if using Zookeeper.
	 * @param isaDB      The socket address of the backend datastore to which
	 *                   you need to establish a session.
	 * @throws IOException
	 */
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);

		session = (cluster = Cluster.builder().addContactPoint("127.0.0.1")
				.build()).connect(myID);
		System.out.println("Server " + myID + " added cluster contact point");

		this.myID = myID;

		// Initialize Zookeeper connection and set watch
		initializeZookeeper();

		// Load or initialize MyNodeConfig using Zookeeper
		myNodeConfig = new MyNodeConfig(zookeeper);
		try {
			myNodeConfig.loadFromZookeeper();
			 System.out.println("Loaded MyNodeConfig from Zookeeper: " + myNodeConfig.getNodeIDs());
		} catch (KeeperException | InterruptedException e) {
			 System.out.println("Error loading MyNodeConfig from Zookeeper: " + e.getMessage());
			// Handle exception or initialize MyNodeConfig if it doesn't exist in Zookeeper
		}

		lock.lock();
		try {
			if (!myNodeConfig.containsNodeID(myID)) {
				System.out.println("Server " + myID + " is adding itself to MyNodeConfig.");
				myNodeConfig.addNodeID(myID);
				try {
					myNodeConfig.saveToZookeeper();
					System.out.println("Updated MyNodeConfig saved to Zookeeper: " + myNodeConfig.getNodeIDs());
				} catch (KeeperException.NodeExistsException e) {
					System.out.println("Node already exists, will try updating: " + e.getMessage());
					myNodeConfig.saveToZookeeper(); // Retry saving, which will update the node
				}
			}
		} catch (KeeperException | InterruptedException e) {
			System.out.println("Error saving MyNodeConfig to Zookeeper: " + e.getMessage());
			// Handle other exceptions
		} finally {
			lock.unlock();
		}

		// Delayed Leader Election
		new Thread(() -> {
			try {
				Thread.sleep(5000); // Delay for a predefined period
				myNodeConfig.loadFromZookeeper();
				System.out.println("Reloaded MyNodeConfig from Zookeeper: " + myNodeConfig.getNodeIDs());
				this.leader = determineLeader(myID);
				System.out.println("Server " + myID + " determined leader as: " + this.leader);

				// Take action based on the leader determination
				if (this.leader.equals(myID)) {
					createLeaderEphemeralNode();
				} else {
					System.out.println("watchLeaderNode() called from constructor in " + myID);
					watchLeaderNode();
				}

				createServerEphemeralNode();
				// Set watch on other servers' nodes
				for (String serverID : myNodeConfig.getNodeIDs()) {
					if (!serverID.equals(myID)) {
						watchServerNode(serverID);
					}
				}


			} catch (InterruptedException | KeeperException e) {
				System.out.println("Leader election thread interrupted for server " + myID);
				Thread.currentThread().interrupt();
			}
		}).start();

		this.stateLogFilePath = logDirectoryPath + "/serverStateLogs_" + myID + ".log";

		this.serverMessenger = new MessageNIOTransport<>(myID, nodeConfig, new AbstractBytePacketDemultiplexer() {
			@Override
			public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
				handleMessageFromServer(bytes, nioHeader);
				return true;
			}
		}, true);

		System.out.println("Server " + myID + " started on " + this.clientMessenger.getListeningSocketAddress());
	}


	protected static enum Type {
		REQUEST, // a server forwards a REQUEST to the leader
		PROPOSAL, // the leader broadcast the REQUEST to all the nodes
		ACKNOWLEDGEMENT; // all the nodes send back acknowledgement to the leader
	}

	private String determineLeader(String myID) {
		lock.lock();
		try {
			String lowestNode = myNodeConfig.getNodeIDs().stream().sorted().findFirst().orElse(null);
			// If myID is the lowest or if there's no other node, this node becomes the leader
			return lowestNode != null ? lowestNode : myID;
		} finally {
			lock.unlock();
		}
	}

	private synchronized void createLeaderEphemeralNode() {
		try {
			String leaderPath = "/leader";
			if (zookeeper.exists(leaderPath, false) == null) {
				zookeeper.create(leaderPath, myID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				System.out.println(myID + " created ephemeral leader node at path " + leaderPath);
			}
		} catch (Exception e) {
			System.out.println("An error occurred while " + myID + " was trying to create leader node: " + e.getMessage());
		}
	}

	private void createServerEphemeralNode() {
		String serversParentPath = "/servers";
		String serverNodePath = serversParentPath + "/" + myID;

		try {
			// Ensure parent node exists
			if (zookeeper.exists(serversParentPath, false) == null) {
				zookeeper.create(serversParentPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}

			// Create the server's ephemeral node
			zookeeper.create(serverNodePath, myID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.println(myID + " created its ephemeral node at path " + serverNodePath);
		} catch (KeeperException.NodeExistsException e) {
			System.out.println("Node already exists: " + e.getMessage());
		} catch (Exception e) {
			System.out.println("An error occurred while " + myID + " was trying to create its ephemeral node: " + e.getMessage());
		}
	}



	private synchronized void initializeZookeeper() throws IOException {
		CountDownLatch connectionLatch = new CountDownLatch(1);

		zookeeper = new ZooKeeper(ZOOKEEPER_HOST, 3000, event -> {
			if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
				 System.out.println(myID + " has connected to Zookeeper successfully.");
				connectionLatch.countDown(); // Notify the latch that the connection is established
			} else {
				 System.out.println(myID + " Zookeeper connection state changed: " + event.getState());
			}

//			if (event.getType() == Watcher.Event.EventType.NodeDeleted && event.getPath().equals(leaderPath)) {
//				 System.out.println(myID + " detected leader crash.");
//				handleLeaderCrash();
//			}
		});

		try {
			System.out.println(myID + " waiting for Zookeeper connection...");
			connectionLatch.await(); // Wait for the connection to establish
			System.out.println(myID + " Zookeeper connection established.");
		} catch (InterruptedException e) {
			System.out.println(myID + " interrupted while waiting for Zookeeper connection.");
			Thread.currentThread().interrupt(); // Set the interrupt flag
		}
	}

	private void watchLeaderNode() {
		String leaderPath = "/leader";
		try {
			zookeeper.exists(leaderPath, event -> {
				if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
					System.out.println(myID + " detected leader crash. Initiating leader re-election.");
					handleLeaderCrash();
				}
				// Re-register the watch
				//watchLeaderNode();
			});
		} catch (Exception e) {
			System.out.println("An error occurred while setting watch on leader node: " + e.getMessage());
		}
	}



	private void watchServerNode(String serverID) {
		String serverNodePath = "/servers/" + serverID;
		try {
			zookeeper.exists(serverNodePath, event -> {
				if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
					System.out.println("Detected crash of server " + serverID + ".");
					handleServerCrash(serverID);
				}
				// Re-register the watch
				//watchServerNode(serverID);
			});
		} catch (Exception e) {
			System.out.println("An error occurred while " + myID + " was trying to watch server node: " + e.getMessage());
		}
	}





	private void handleLeaderCrash() {
		myNodeConfig.removeNodeID(leader);
		System.out.println("Leader " + leader + " removed from MyNodeConfig by " + myID);

		try {
			myNodeConfig.saveToZookeeper();
			System.out.println("MyNodeConfig updated in Zookeeper after removing leader " + leader);
		} catch (KeeperException | InterruptedException e) {
			System.out.println("Error updating MyNodeConfig in Zookeeper: " + e.getMessage());
		}
		attemptToBecomeLeader();
		 
	}

	private void handleServerCrash(String crashedServerID) {
		configLock.lock();
		try {
			myNodeConfig.removeNodeID(crashedServerID);
			System.out.println("Server " + crashedServerID + " removed from MyNodeConfig by " + myID);

			myNodeConfig.saveToZookeeper();
			attemptToBecomeLeader();
			System.out.println("MyNodeConfig updated in Zookeeper after removing " + crashedServerID);
		} catch (KeeperException | InterruptedException e) {
			System.out.println("Error updating MyNodeConfig in Zookeeper: " + e.getMessage());
		} finally {
			configLock.unlock();
		}
	}


	private void attemptToBecomeLeader() {
		System.out.println("Attempting to become leader by server: " + myID);

		// Ensure that this is synchronized to prevent race conditions
		String lowestNode = myNodeConfig.getNodeIDs().stream().sorted().findFirst().orElse(null);

		if (lowestNode != null && lowestNode.equals(myID)) {
			// This server has the lowest ID, so it becomes the leader
			leader = myID;
			System.out.println("Server " + myID + " is now the leader.");

			// Create the ephemeral leader node in Zookeeper
			createLeaderEphemeralNode();
		} else {
			if (lowestNode != null) {
				System.out.println("Server " + myID + " is not the leader. Current leader is: " + lowestNode);
				// Watch the leader node in Zookeeper
				System.out.println("watchLeaderNode() called from attemptToBecomeLeader in " + myID);
				watchLeaderNode();
			} else {
				System.out.println("No leader could be determined by server: " + myID);
			}
		}
	}



	public void handleNodeRecovery() {
		configLock.lock();
		try {
			System.out.println("Server " + myID + " adding itself back to MyNodeConfig.");
			myNodeConfig.addNodeID(myID);
			attemptToBecomeLeader();
		} finally {
			configLock.unlock();
		}
	}


	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {

		// this is a request sent by callbackSend method
		String request = new String(bytes);

		log.log(Level.INFO, "{0} received client message {1} from {2}",
				new Object[]{this.myID, request, header.sndr});
		JSONObject json = null;
		try {
			json = new JSONObject(request);
			request = json.getString(MyDBClient.Keys.REQUEST
					.toString());
		} catch (JSONException e) {
			//e.printStackTrace();
		}

		// forward the request to the leader as a proposal
		try {
			JSONObject packet = new JSONObject();
			packet.put(MyDBClient.Keys.REQUEST.toString(), request);
			packet.put(MyDBClient.Keys.TYPE.toString(), MyDBFaultTolerantServerZK.Type.REQUEST.toString());

			this.serverMessenger.send(leader, packet.toString().getBytes());
			log.log(Level.INFO, "{0} sends a REQUEST {1} to {2}",
					new Object[]{this.myID, packet, leader});
		} catch (IOException | JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}


		String response = "[success:"+new String(bytes)+"]";
		if(json!=null){
			try{
				json.put(MyDBClient.Keys.RESPONSE.toString(),
						response);
				response = json.toString();
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}

		try{
			// when it's done send back response to client
			serverMessenger.send(header.sndr, response.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		System.out.println("In handleMessageFromServer: " + myID + " sees " + leader + "as the leader");
//		System.out.println("In handleMessageFromServer" + myID + ": received the message");

		// deserialize the request
		JSONObject json = null;
		try {
			json = new JSONObject(new String(bytes));
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		log.log(Level.INFO, "{0} received relayed message {1} from {2}",
				new Object[]{this.myID, json, header.sndr}); // simply log

		// check the type of the request
		try {
			String type = json.getString(MyDBClient.Keys.TYPE.toString());
			if (type.equals(MyDBFaultTolerantServerZK.Type.REQUEST.toString())) {
				if (myID.equals(leader)) {

					// put the request into the queue
					Long reqId = incrReqNum();
					json.put(MyDBClient.Keys.REQNUM.toString(), reqId);
					queue.put(reqId, json);
					log.log(Level.INFO, "{0} put request {1} into the queue.",
							new Object[]{this.myID, json});

					if (isReadyToSend(expected)) {
						// retrieve the first request in the queue
						JSONObject proposal = queue.remove(expected);
						if (proposal != null) {
							proposal.put(MyDBClient.Keys.TYPE.toString(), MyDBFaultTolerantServerZK.Type.PROPOSAL.toString());
							enqueue();
							broadcastRequest(proposal);
						} else {
							log.log(Level.INFO, "{0} is ready to send request {1}, but the message has already been retrieved.",
									new Object[]{this.myID, expected});
						}

					}
				} else {
					log.log(Level.SEVERE, "{0} received REQUEST message from {1} which should not be here.",
							new Object[]{this.myID, header.sndr});
				}

				//Update state in Zookeeper upon receive of REQUEST type
				updateServerState(Type.REQUEST.toString(), json);
			}

			else if (type.equals(MyDBFaultTolerantServerZK.Type.PROPOSAL.toString())) {

				// execute the query and send back the acknowledgement
				String query = json.getString(MyDBClient.Keys.REQUEST.toString());
				long reqId = json.getLong(MyDBClient.Keys.REQNUM.toString());

				session.execute(query);

				JSONObject response = new JSONObject().put(MyDBClient.Keys.RESPONSE.toString(), this.myID)
						.put(MyDBClient.Keys.REQNUM.toString(), reqId)
						.put(MyDBClient.Keys.TYPE.toString(), MyDBFaultTolerantServerZK.Type.ACKNOWLEDGEMENT.toString());
				serverMessenger.send(header.sndr, response.toString().getBytes());

				//Update state in Zookeeper upon receive of PROPOSAL type
				updateServerState(Type.PROPOSAL.toString(), json);
			} else if (type.equals(MyDBFaultTolerantServerZK.Type.ACKNOWLEDGEMENT.toString())) {

				// only the leader needs to handle acknowledgement
				if (myID.equals(leader)) {
					// TODO: leader processes ack here
					String node = json.getString(MyDBClient.Keys.RESPONSE.toString());
					if (dequeue(node)) {
						// if the leader has received all acks, then prepare to send the next request
						expected++;
						if (isReadyToSend(expected)) {
							JSONObject proposal = queue.remove(expected);
							if (proposal != null) {
								proposal.put(MyDBClient.Keys.TYPE.toString(), MyDBFaultTolerantServerZK.Type.PROPOSAL.toString());
								enqueue();
								broadcastRequest(proposal);
							} else {
								log.log(Level.INFO, "{0} is ready to send request {1}, but the message has already been retrieved.",
										new Object[]{this.myID, expected});
							}
						}
					}
				} else {
					log.log(Level.SEVERE, "{0} received ACKNOWLEDEMENT message from {1} which should not be here.",
							new Object[]{this.myID, header.sndr});
				}

				//Update state in Zookeeper upon receive of ACKNOWLEDGEMENT type
				updateServerState(Type.ACKNOWLEDGEMENT.toString(), json);
			} else {
				log.log(Level.SEVERE, "{0} received unrecongonized message from {1} which should not be here.",
						new Object[]{this.myID, header.sndr});
			}

		} catch (JSONException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	private boolean isReadyToSend(long expectedId) {
		if (queue.size() > 0 && queue.containsKey(expectedId)) {
			return true;
		}
		return false;
	}

	private void broadcastRequest(JSONObject req) {
		for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()){
			try {
				this.serverMessenger.send(node, req.toString().getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		log.log(Level.INFO, "The leader has broadcast the request {0}", new Object[]{req});
	}

	private void enqueue(){
		notAcked = new CopyOnWriteArrayList<String>();
		for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()){
			notAcked.add(node);
		}
	}

	private boolean dequeue(String node) {
		if(!notAcked.remove(node)){
			log.log(Level.SEVERE, "The leader does not have the key {0} in its notAcked", new Object[]{node});
		}
		if(notAcked.size() == 0)
			return true;
		return false;
	}

	public void retrieveAndProcessStateLogs() {
		Path logDir = Paths.get(logDirectoryPath);
		try {
			if (Files.exists(logDir) && Files.isDirectory(logDir)) {
				try (DirectoryStream<Path> stream = Files.newDirectoryStream(logDir, "serverStateLogs_*.log")) {
					for (Path entry : stream) {
						List<String> logEntries = Files.readAllLines(entry);
						for (String logEntry : logEntries) {
							String[] parts = logEntry.split(":", 2);
							if (parts.length == 2) {
								String stateType = parts[0].trim();
								String stateData = parts[1].trim();
								forwardStateToHandler(stateData.getBytes());
							}
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			// Handle exceptions
		}
	}


	private void forwardStateToHandler(byte[] bytes) {
		String logEntry = new String(bytes);

		// Deserialize the log entry into a JSONObject
		JSONObject state;
		try {
			state = new JSONObject(logEntry);
		} catch (JSONException e) {
			// Log the problematic data for debugging
			System.err.println("Failed to parse JSON: " + logEntry);
			e.printStackTrace();
			return;
		}

		// Extract the state type from the JSON object
		String stateType;
		try {
			stateType = state.getString("TYPE");
		} catch (JSONException e) {
			System.err.println("State type missing in JSON: " + state);
			e.printStackTrace();
			return;
		}

		// Handle the state based on its type
		switch (stateType) {
			case "ACKNOWLEDGEMENT":
				handleAcknowledgement(state);
				break;
			case "PROPOSAL":
				handleProposal(state);
				break;
			case "REQUEST":
				handleRequest(state);
				break;
			default:
				System.err.println("Unknown state type: " + stateType);
		}
	}




	private void handleAcknowledgement(JSONObject acknowledgement) {
		// Implement logic to handle ACKNOWLEDGEMENT
		try {
			serverMessenger.send(leader, acknowledgement.toString().getBytes());
			log.log(Level.INFO, "{0} sends an ACKNOWLEDGEMENT {1} to {2}",
					new Object[]{this.myID, acknowledgement, leader});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void handleProposal(JSONObject proposal) {
		// Implement logic to handle PROPOSAL
		try {
			serverMessenger.send(leader, proposal.toString().getBytes());
			log.log(Level.INFO, "{0} sends a PROPOSAL {1} to {2}",
					new Object[]{this.myID, proposal, leader});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void handleRequest(JSONObject request) {
		// Implement logic to handle REQUEST
		try {
			serverMessenger.send(leader, request.toString().getBytes());
			log.log(Level.INFO, "{0} sends a REQUEST {1} to {2}",
					new Object[]{this.myID, request, leader});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void updateServerState(String stateType, JSONObject json) {
		try {

			Files.createDirectories(Paths.get(logDirectoryPath));
			String logEntry = stateType + ": " + json.toString() + System.lineSeparator();
			Files.write(Paths.get(stateLogFilePath), logEntry.getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/**
	 * TODO: Gracefully close any threads or messengers you created.
	 */
	public void close() {
		try {
			if (zookeeper != null) {
				zookeeper.close();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static enum CheckpointRecovery {
		CHECKPOINT, RESTORE;
	}

	/**
	 * @param args args[0] must be server.properties file and args[1] must be
	 *             myID. The server prefix in the properties file must be
	 *             ReplicatedServer.SERVER_PREFIX. Optional args[2] if
	 *             specified
	 *             will be a socket address for the backend datastore.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile
				(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
						.SERVER_PORT_OFFSET), args[1], args.length > 2 ? Util
				.getInetSocketAddressFromString(args[2]) : new
				InetSocketAddress("localhost", 9042));
	}

}