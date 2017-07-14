import com.dataspec.connection.ConnectionHandle;

public class DatabaseUtil {

    private ConnectionHandle handle;

    public DatabaseUtil(ConnectionHandle handle) {
        this.handle = handle;
    }


    public void execute(String query) {
        handle.execute(query);
    }

    public <T> Iterable<T> query(String query, Class<T> type) {
        return handle.execute(query);
    }
}
