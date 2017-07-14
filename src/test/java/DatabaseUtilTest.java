import com.dataspec.DatabaseUtil;
import com.dataspec.cassandra.CassandraConfiguration;
import com.dataspec.cassandra.CassandraConnectionHandle;
import com.dataspec.connection.ConnectionHandle;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.dataspec.exception.ExpectationNeverMetException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class DatabaseUtilTest {
    private static final String SERVER_IP = "localhost";
    private static final int PORT = 9042;
    private static final String KEYSPACE = "test_keyspace";
    private DatabaseUtil databaseUtil;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void initializeKeyspace() {
        final Session session = getCluster().connect();
        final String createKeyspaceStatement = format("CREATE KEYSPACE if not exists %s " +
                "WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};", KEYSPACE);

        session.execute(createKeyspaceStatement);
    }

    @Before
    public void initializeDatabaseUtil() {
        CassandraConfiguration config = new CassandraConfiguration(9042, KEYSPACE, "localhost");
        ConnectionHandle handle = new CassandraConnectionHandle(config);
        databaseUtil = new DatabaseUtil(handle);
    }

    @Test
    public void execute_shouldWriteToDatabase() {
        insertTestTable();

        final String randomValue = UUID.randomUUID().toString();
        databaseUtil.execute(format("insert into test_table (key, value) values('test-key', '%s');", randomValue));

        Row result = getCluster().connect(KEYSPACE).execute("select * from test_table").one();
        assertThat(result.getString("key"), is("test-key"));
        assertThat(result.getString("value"), is(randomValue));
    }

    @Test
    public void execute_shouldReadFromDatabase() throws SQLException {
        insertTestTable();
        final Session session = getCluster().connect(KEYSPACE);
        String randomValue = UUID.randomUUID().toString();
        session.execute(format("insert into test_table (key, value) values('test-key', '%s');", randomValue));

        Iterable<Row> query = databaseUtil.query("select * from test_table");

        assertThat(query.iterator().hasNext(), is(true));
        Row row = query.iterator().next();
        assertThat(row.getString("key"), is("test-key"));
        assertThat(row.getString("value"), is(randomValue));
    }

    @Test
    public void waitForExpectedValue_whenExpectationMet_returnsSilently() throws Exception {
        insertTestTable();
        final Session session = getCluster().connect(KEYSPACE);
        String randomValue = UUID.randomUUID().toString();

        newFixedThreadPool(2).submit(() -> {
            waitFor(1, SECONDS);
            session.execute(format("insert into test_table (key, value) values('test-key', '%s');", randomValue));
        });

        databaseUtil.waitForExpectedValue(format("select * from test_table where key='test-key' and value='%s' ALLOW FILTERING", randomValue), (Iterable<Row> rows) -> rows.iterator().hasNext());
    }

    @Test
    public void waitForExpectedValue_whenTimeoutExpired_throwsException() throws Exception {
        insertTestTable();
        String randomValue = UUID.randomUUID().toString();

        expectedException.expect(ExpectationNeverMetException.class);
        databaseUtil.waitForExpectedValue(format("select * from test_table where key='test-key' and value='%s' ALLOW FILTERING", randomValue), (Iterable<Row> rows) -> rows.iterator().hasNext());
    }

    private void insertTestTable() {
        final Session session = getCluster().connect(KEYSPACE);
        session.execute("create table if not exists test_table(" +
                "key text," +
                "value text," +
                "primary key (key));");
        session.execute("truncate table test_table");
        session.close();
    }

    private void waitFor(int timeout, TimeUnit seconds) {
        try {
            seconds.sleep(timeout);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private static Cluster getCluster() {
        final InetSocketAddress cassandraUrl = new InetSocketAddress(SERVER_IP, PORT);
        return Cluster.builder().addContactPointsWithPorts(cassandraUrl).build();
    }
}
