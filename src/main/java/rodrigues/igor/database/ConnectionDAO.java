package rodrigues.igor.database;

import rodrigues.igor.database.repository.E1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionDAO {
    private final String url = "jdbc:mysql://localhost:3306/%s";

    /**
     * Returns the connection to the E1 database
     */
    public Connection connectE1(String username, String password){
        return connect(E1.DB_NAME, username, password);
    }

    public Connection connect(String database, String username, String password){
        Connection connection = null;
        try{
            String url = String.format(this.url, database);
            connection = DriverManager.getConnection(url, username, password);
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
        return connection;
    }
}
