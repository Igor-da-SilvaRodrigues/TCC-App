package rodrigues.igor.database;

import rodrigues.igor.database.repository.ClassTableRepository;
import rodrigues.igor.database.repository.ConcreteTableRepository;
import rodrigues.igor.database.repository.E1Repository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionDAO {
    private final String url = "jdbc:mysql://localhost:3306/%s";

    /**
     * Returns the connection to the E1 database
     */
    public Connection connectE1(String username, String password){
        return connect(E1Repository.DB_NAME, username, password);
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

    public Connection connectE5(String username, String password) {
        return connect(ConcreteTableRepository.DB_NAME, username, password);
    }

    public Connection connectE6(String username, String password) {
        return connect(ClassTableRepository.DB_NAME, username, password);
    }
}
