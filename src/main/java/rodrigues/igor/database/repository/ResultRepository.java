package rodrigues.igor.database.repository;

import rodrigues.igor.test.result.Result;
import rodrigues.igor.test.result.StrategyResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class ResultRepository {
    public static final String DB_NAME = "result";
    private final Connection connection;

    public ResultRepository(Connection connection) {
        this.connection = connection;
    }

    public void createResult(Result result){
        insertCreateResult(result);
        insertSelectResult(result);
        insertUpdateResult(result);
        insertDeleteResult(result);
    }

    private void insertCreateResult(Result result) {
        String sql = "insert into CreateTests(E1,E2,E3,E4,E5,E6) values (?,?,?,?,?,?)";

        StrategyResult e1Result = result.getResultE1();
        StrategyResult e2Result = result.getResultE2();
        StrategyResult e3Result = result.getResultE3();
        StrategyResult e4Result = result.getResultE4();
        StrategyResult e5Result = result.getResultE5();
        StrategyResult e6Result = result.getResultE6();

        try (PreparedStatement statement = connection.prepareStatement(sql)){
            //sea of ifs. Insert value if present, null otherwise.
            if (e1Result != null){ statement.setDouble(1, e1Result.getCreateResult());} else { statement.setNull(1, Types.DOUBLE); }
            if (e2Result != null){ statement.setDouble(2, e2Result.getCreateResult());} else { statement.setNull(2, Types.DOUBLE); }
            if (e3Result != null){ statement.setDouble(3, e3Result.getCreateResult());} else { statement.setNull(3, Types.DOUBLE); }
            if (e4Result != null){ statement.setDouble(4, e4Result.getCreateResult());} else { statement.setNull(4, Types.DOUBLE); }
            if (e5Result != null){ statement.setDouble(5, e5Result.getCreateResult());} else { statement.setNull(5, Types.DOUBLE); }
            if (e6Result != null){ statement.setDouble(6, e6Result.getCreateResult());} else { statement.setNull(6, Types.DOUBLE); }

            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertSelectResult(Result result) {
        String sql = "insert into SelectTests(E1,E2,E3,E4,E5,E6) values (?,?,?,?,?,?)";

        StrategyResult e1Result = result.getResultE1();
        StrategyResult e2Result = result.getResultE2();
        StrategyResult e3Result = result.getResultE3();
        StrategyResult e4Result = result.getResultE4();
        StrategyResult e5Result = result.getResultE5();
        StrategyResult e6Result = result.getResultE6();

        try (PreparedStatement statement = connection.prepareStatement(sql)){
            if (e1Result != null){ statement.setDouble(1, e1Result.getSelectResult());} else { statement.setNull(1, Types.DOUBLE); }
            if (e2Result != null){ statement.setDouble(2, e2Result.getSelectResult());} else { statement.setNull(2, Types.DOUBLE); }
            if (e3Result != null){ statement.setDouble(3, e3Result.getSelectResult());} else { statement.setNull(3, Types.DOUBLE); }
            if (e4Result != null){ statement.setDouble(4, e4Result.getSelectResult());} else { statement.setNull(4, Types.DOUBLE); }
            if (e5Result != null){ statement.setDouble(5, e5Result.getSelectResult());} else { statement.setNull(5, Types.DOUBLE); }
            if (e6Result != null){ statement.setDouble(6, e6Result.getSelectResult());} else { statement.setNull(6, Types.DOUBLE); }

            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertUpdateResult(Result result) {
        String sql = "insert into UpdateTests(E1,E2,E3,E4,E5,E6) values (?,?,?,?,?,?)";

        StrategyResult e1Result = result.getResultE1();
        StrategyResult e2Result = result.getResultE2();
        StrategyResult e3Result = result.getResultE3();
        StrategyResult e4Result = result.getResultE4();
        StrategyResult e5Result = result.getResultE5();
        StrategyResult e6Result = result.getResultE6();

        try (PreparedStatement statement = connection.prepareStatement(sql)){

            if (e1Result != null){ statement.setDouble(1, e1Result.getUpdateResult());} else { statement.setNull(1, Types.DOUBLE); }
            if (e2Result != null){ statement.setDouble(2, e2Result.getUpdateResult());} else { statement.setNull(2, Types.DOUBLE); }
            if (e3Result != null){ statement.setDouble(3, e3Result.getUpdateResult());} else { statement.setNull(3, Types.DOUBLE); }
            if (e4Result != null){ statement.setDouble(4, e4Result.getUpdateResult());} else { statement.setNull(4, Types.DOUBLE); }
            if (e5Result != null){ statement.setDouble(5, e5Result.getUpdateResult());} else { statement.setNull(5, Types.DOUBLE); }
            if (e6Result != null){ statement.setDouble(6, e6Result.getUpdateResult());} else { statement.setNull(6, Types.DOUBLE); }

            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertDeleteResult(Result result) {
        String sql = "insert into DeleteTests(E1,E2,E3,E4,E5,E6) values (?,?,?,?,?,?)";

        StrategyResult e1Result = result.getResultE1();
        StrategyResult e2Result = result.getResultE2();
        StrategyResult e3Result = result.getResultE3();
        StrategyResult e4Result = result.getResultE4();
        StrategyResult e5Result = result.getResultE5();
        StrategyResult e6Result = result.getResultE6();

        try (PreparedStatement statement = connection.prepareStatement(sql)){

            if (e1Result != null){ statement.setDouble(1, e1Result.getDeleteResult());} else { statement.setNull(1, Types.DOUBLE); }
            if (e2Result != null){ statement.setDouble(2, e2Result.getDeleteResult());} else { statement.setNull(2, Types.DOUBLE); }
            if (e3Result != null){ statement.setDouble(3, e3Result.getDeleteResult());} else { statement.setNull(3, Types.DOUBLE); }
            if (e4Result != null){ statement.setDouble(4, e4Result.getDeleteResult());} else { statement.setNull(4, Types.DOUBLE); }
            if (e5Result != null){ statement.setDouble(5, e5Result.getDeleteResult());} else { statement.setNull(5, Types.DOUBLE); }
            if (e6Result != null){ statement.setDouble(6, e6Result.getDeleteResult());} else { statement.setNull(6, Types.DOUBLE); }

            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
