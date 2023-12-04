package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	private class TableQueryList{
		public String table;
		public List<String> queries;

		public TableQueryList(String table, List<String> queries){
			this.table = table;
			this.queries = queries;
		}
	}

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1000;
	final private Session session;
	final private Cluster cluster;
	final private String keyspace;

	private Queue<String> bufferQueries;



	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		keyspace = args[0];
		session = (cluster=Cluster.builder().addContactPoint("127.0.0.1")
				.build()).connect(keyspace);
		bufferQueries = new LinkedList<>();

		// TODO: setup connection to the data store and keyspace
		//throw new RuntimeException("Not yet implemented");
	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)} to
	 * understand what the boolean flag means.
	 * <p>
	 * You can assume that all requests will be of type {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		// TODO: submit request to data store
		return this.execute(request);
		//throw new RuntimeException("Not yet implemented");
	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
		// TODO: execute the request by sending it to the data store
		try{
			session.execute(((RequestPacket)request).getRequestValue());
			bufferQueries.add(((RequestPacket)request).getRequestValue());
			return true;
		}
		catch (Exception e){
			return false;
		}
		//throw new RuntimeException("Not yet implemented");
	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {
		// TODO:
		String cql = "SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name = ?;";
		PreparedStatement preparedStatement = session.prepare(cql);
		BoundStatement boundStatement = preparedStatement.bind(keyspace);

		ResultSet result = session.execute(boundStatement);
		List<TableQueryList> tableQueries = new ArrayList<>();
		for (Row row : result) {
			final String table = row.getString(1);
			ResultSet records = session.execute("SELECT * from " + keyspace + "." + table + ";");
			List<String> queries = new ArrayList<>();
			for (Row record : records) {
				List<ColumnDefinitions.Definition> columnDefs = record.getColumnDefinitions().asList();
				String columnNames = columnDefs.stream()
						.map(ColumnDefinitions.Definition::getName)
						.collect(Collectors.joining(","));
				String values = columnDefs.stream()
						.map(def -> "'" + record.getString(def.getName()) + "'")
						.collect(Collectors.joining(","));
				queries.add("INSERT INTO " + keyspace + "." + table + " (" + columnNames + ") VALUES (" + values + ");");
			}
			tableQueries.add(new TableQueryList(table, queries));
		}
		bufferQueries = new LinkedList<>();
		//throw new RuntimeException("Not yet implemented");
		return new JSONArray(tableQueries).toString();
	}

	/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param s
	 * @param s1
	 * @return
	 */
	@Override
	public boolean restore(String s, String s1) {
		// TODO:
		try {
			JSONArray jsonArray = new JSONArray(s1);
			for(int i=0;i<jsonArray.length();i++) {
				JSONObject tableQueryList = jsonArray.getJSONObject(i);
				String tableName = tableQueryList.getString("table");
				session.execute("TRUNCATE "+keyspace+"."+tableName+";");
				JSONArray queries = tableQueryList.getJSONArray("queries");
				for(int k=0;k<queries.length();k++) {
					session.execute(queries.getString(k));
				}
			}
			while(!bufferQueries.isEmpty()){
				session.execute(bufferQueries.peek());
				bufferQueries.remove();
			}
			return true;
		}
		catch (Exception e){
			return false;
		}
		//throw new RuntimeException("Not yet implemented");

	}


	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement
	 * this method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 * example of how to define your own IntegerPacketType enum, refer {@link
	 * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
	 * need to be implemented because the assignment Grader will only use
	 * {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}
}
