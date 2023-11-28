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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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
	public static final int SLEEP = 2000;

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

	private ZooKeeper zookeeper;
	private static final String ZOOKEEPER_HOST = "localhost:2181";
	private final String electionPath = "/election";

	boolean isLeader = false;

	// Create a unique path for this server
	String statePath = "/serverStates/";

	CountDownLatch connectedSignal = new CountDownLatch(1);


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
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String
			myID, InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer
						.SERVER_PORT_OFFSET), isaDB, myID);

		session = (cluster=Cluster.builder().addContactPoint("127.0.0.1")
				.build()).connect(myID);
		log.log(Level.INFO, "Server {0} added cluster contact point",	new
				Object[]{myID,});
		// leader is elected as the first node in the nodeConfig
		for(String node : nodeConfig.getNodeIDs()){
			this.leader = node;
			break;
		}
		this.myID = myID;
		this.statePath = "/serverStates/" + myID;

		this.serverMessenger =  new
				MessageNIOTransport<String, String>(myID, nodeConfig,
				new
						AbstractBytePacketDemultiplexer() {
							@Override
							public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
								handleMessageFromServer(bytes, nioHeader);
								return true;
							}
						}, true);

		log.log(Level.INFO, "Server {0} started on {1}", new Object[]{this
				.myID, this.clientMessenger.getListeningSocketAddress()});

		try {
			zookeeper = new ZooKeeper(ZOOKEEPER_HOST, 5000, new Watcher() {
				public void process(WatchedEvent we) {
					if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
						System.out.println("Connected to ZooKeeper");
						connectedSignal.countDown();
					}
				}
			});
			connectedSignal.await();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			ensureServerStatesPathExists();
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			// Handle exception
		}

		try {
			runForLeader();
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void runForLeader() throws KeeperException, InterruptedException {
		System.out.println("Leader Election started");

		// Ensure the election path exists
		Stat stat = zookeeper.exists(electionPath, false);
		if (stat == null) {
			// Create the election path if it does not exist
			zookeeper.create(electionPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		String path = zookeeper.create(electionPath + "/server-", myID.getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);

		List<String> nodes = zookeeper.getChildren(electionPath, false);
		Collections.sort(nodes);

		int index = nodes.indexOf(path.substring(electionPath.length() + 1));
		if (index == 0) {
			// This server is the leader
			isLeader = true;
			leader = myID;
			System.out.println(leader + ": is the Leader");
			handleNewLeader();
		} else {
			// This server is not the leader, watch the node with a smaller sequence number
			String nodeToWatch = nodes.get(index - 1);
			zookeeper.exists(electionPath + "/" + nodeToWatch, watchedEvent -> {
				if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted) {
					// The node we are watching is deleted, try to become the leader again
					try {
						runForLeader();
					} catch (KeeperException | InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
			});
		}
	}

	private void handleNewLeader() {
		retrieveAndProcessStateLogs();
	}

	public void retrieveAndProcessStateLogs() {
		try {
			List<String> serverIds = zookeeper.getChildren("/serverStates", false);

			for (String serverId : serverIds) {
				List<String> stateTypes = zookeeper.getChildren("/serverStates/" + serverId, false);

				for (String stateType : stateTypes) {
					String statePath = "/serverStates/" + serverId + "/" + stateType;
					byte[] stateData = zookeeper.getData(statePath, false, null);

					forwardStateToHandler(stateData); // Pass a dummy NIOHeader or relevant header
				}
			}
		} catch (KeeperException.NoNodeException e) {
			System.out.println("No state logs available in Zookeeper.");
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		} catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

	public void ensureServerStatesPathExists() throws KeeperException, InterruptedException {
		Stat stat = zookeeper.exists("/serverStates", false);
		if (stat == null) {
			zookeeper.create("/serverStates", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}


	private void forwardStateToHandler(byte[] bytes) throws JSONException {

		// deserialize the request
		JSONObject state = null;
		try {
			state = new JSONObject(new String(bytes));
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String type = state.getString("type");
		switch (type) {
			case "ACKNOWLEDGEMENT":
				handleAcknowledgement(state);
				break;
			case "PROPOSAL":
				handleProposal(state);
				break;
			case "REQUEST":
				handleRequest(state);
				break;
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

	private void updateZookeeperState(String stateType, JSONObject json) {
		String serverStatePath = "/serverStates/" + myID;
		String statePath = serverStatePath + "/" + stateType;

		try {
			// Ensure the parent nodes exist
			ensurePathExists(serverStatePath);

			byte[] stateData = json.toString().getBytes(); // Convert JSON to byte array
			Stat stat = zookeeper.exists(statePath, false);

			if (stat == null) {
				zookeeper.create(statePath, stateData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} else {
				zookeeper.setData(statePath, stateData, stat.getVersion());
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			// Handle exceptions, possibly retrying the operation
		}
	}

	private void ensurePathExists(String path) throws KeeperException, InterruptedException {
		if (zookeeper.exists(path, false) == null) {
			String[] pathSegments = path.split("/");
			StringBuilder currentPath = new StringBuilder();

			// Create each segment if it doesn't exist
			for (String segment : pathSegments) {
				if (!segment.isEmpty()) {
					currentPath.append("/").append(segment);
					if (zookeeper.exists(currentPath.toString(), false) == null) {
						zookeeper.create(currentPath.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
				}
			}
		}
	}

	protected static enum Type {
		REQUEST, // a server forwards a REQUEST to the leader
		PROPOSAL, // the leader broadcast the REQUEST to all the nodes
		ACKNOWLEDGEMENT; // all the nodes send back acknowledgement to the leader
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
				updateZookeeperState(Type.REQUEST.toString(), json);
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
				updateZookeeperState(Type.PROPOSAL.toString(), json);
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
				updateZookeeperState(Type.ACKNOWLEDGEMENT.toString(), json);
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