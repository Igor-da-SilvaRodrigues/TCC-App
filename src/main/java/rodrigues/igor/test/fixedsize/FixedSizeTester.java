package rodrigues.igor.test.fixedsize;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.database.ConnectionDAO;
import rodrigues.igor.database.repository.*;
import rodrigues.igor.test.*;
import rodrigues.igor.test.result.Result;
import rodrigues.igor.test.result.StrategyResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class FixedSizeTester {
    private int size;

    public FixedSizeTester(int size) {
        this.size = size;
    }
    public Result testAll(String name, String password, int n){
        Result result = new Result();
        result.setResultE6(testE6(name, password, n));
        result.setResultE5(testE5(name, password, n));
        result.setResultE4(testE4(name, password, n));
        result.setResultE3(testE3(name, password, n));
        result.setResultE2(testE2(name, password, n));
        result.setResultE1(testE1(name, password, n));
        return result;
    }
    public Result testAsync(String name, String password, int n){
        CompletableFuture<StrategyResult> futureE6 = CompletableFuture.supplyAsync(() -> testE6(name, password, n));
        CompletableFuture<StrategyResult> futureE5 = CompletableFuture.supplyAsync(() -> testE5(name, password, n));
        CompletableFuture<StrategyResult> futureE4 = CompletableFuture.supplyAsync(() -> testE4(name, password, n));
        CompletableFuture<StrategyResult> futureE3 = CompletableFuture.supplyAsync(() -> testE3(name, password, n));
        CompletableFuture<StrategyResult> futureE2 = CompletableFuture.supplyAsync(() -> testE2(name, password, n));
        CompletableFuture<StrategyResult> futureE1 = CompletableFuture.supplyAsync(() -> testE1(name, password, n));

        try {
            Result result = new Result();
            result.setResultE6(futureE6.get());
            result.setResultE5(futureE5.get());
            result.setResultE4(futureE4.get());
            result.setResultE3(futureE3.get());
            result.setResultE2(futureE2.get());
            result.setResultE1(futureE1.get());
            return result;
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public StrategyResult testE1(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE1(name, password)){
            FixedSizeTest<E1Test, E1Repository> fixedSizeTest = new FixedSizeTest<>(new E1Test(), size);

            double create = fixedSizeTest.createBatch(n, new E1Repository(connection));
            double createAverage = create/n;
            System.out.printf("the average query time for 'CREATE' in E1 was: %.4f ms, with a total of %.4f ms\n", createAverage, create);

            double select = fixedSizeTest.selectLimit(10*1000, n, new E1Repository(connection));
            double selectAverage = select/n;
            System.out.printf("the average query time for 'SELECT' in E1 was: %.4f ms, with a total of: %.4f ms\n", selectAverage, select);

            double update = fixedSizeTest.updateById(n, new E1Repository(connection));
            double updateAverage = update/n;
            System.out.printf("the average query time for 'UPDATE' in E1 was: %.4f ms, with a total of: %.4f ms\n", updateAverage, update);

            Pair<Integer, Double> delete = fixedSizeTest.delete(n, new E1Repository(connection));
            double deleteAverage = delete.getRight()/delete.getLeft();
            System.out.printf("The average query time for 'DELETE' in E1 was %.4f ms, with a total of : %.4f ms in %d operations\n", deleteAverage, delete.getRight(), delete.getLeft());

            if (delete.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", delete.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public StrategyResult testE2(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE2(name, password)){
            FixedSizeTest<E2Test, E2Repository> fixedSizeTest = new FixedSizeTest<>(new E2Test(), size);

            double create = fixedSizeTest.createBatch(n, new E2Repository(connection));
            double createAverage = create/n;
            System.out.printf("the average query time for 'CREATE' in E2 was: %.4f ms, with a total of %.4f ms\n", createAverage, create);

            double select = fixedSizeTest.selectLimit(10*1000, n, new E2Repository(connection));
            double selectAverage = select/n;
            System.out.printf("the average query time for 'SELECT' in E2 was: %.4f ms, with a total of: %.4f ms\n", selectAverage, select);

            double update = fixedSizeTest.updateById(n, new E2Repository(connection));
            double updateAverage = update/n;
            System.out.printf("the average query time for 'UPDATE' in E2 was: %.4f ms, with a total of: %.4f ms\n", updateAverage, update);

            Pair<Integer, Double> delete = fixedSizeTest.delete(n, new E2Repository(connection));
            double deleteAverage = delete.getRight()/delete.getLeft();
            System.out.printf("The average query time for 'DELETE' in E2 was %.4f ms, with a total of : %.4f ms in %d operations\n", deleteAverage, delete.getRight(), delete.getLeft());

            if (delete.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", delete.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public StrategyResult testE3(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE3(name, password)){
            FixedSizeTest<E3Test, E3Repository> fixedSizeTest = new FixedSizeTest<>(new E3Test(), size);

            double create = fixedSizeTest.createBatch(n, new E3Repository(connection));
            double createAverage = create/n;
            System.out.printf("the average query time for 'CREATE' in E3 was: %.4f ms, with a total of %.4f ms\n", createAverage, create);

            double select = fixedSizeTest.selectLimit(10*1000, n, new E3Repository(connection));
            double selectAverage = select/n;
            System.out.printf("the average query time for 'SELECT' in E3 was: %.4f ms, with a total of: %.4f ms\n", selectAverage, select);

            double update = fixedSizeTest.updateById(n, new E3Repository(connection));
            double updateAverage = update/n;
            System.out.printf("the average query time for 'UPDATE' in E3 was: %.4f ms, with a total of: %.4f ms\n", updateAverage, update);

            Pair<Integer, Double> delete = fixedSizeTest.delete(n, new E3Repository(connection));
            double deleteAverage = delete.getRight()/delete.getLeft();
            System.out.printf("The average query time for 'DELETE' in E3 was %.4f ms, with a total of : %.4f ms in %d operations\n", deleteAverage, delete.getRight(), delete.getLeft());

            if (delete.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", delete.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public StrategyResult testE4(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE4(name, password)){
            FixedSizeTest<E4Test, E4Repository> fixedSizeTest = new FixedSizeTest<>(new E4Test(), size);

            double create = fixedSizeTest.createBatch(n, new E4Repository(connection));
            double createAverage = create/n;
            System.out.printf("the average query time for 'CREATE' in E4 was: %.4f ms, with a total of %.4f ms\n", createAverage, create);

            double select = fixedSizeTest.selectLimit(10*1000, n, new E4Repository(connection));
            double selectAverage = select/n;
            System.out.printf("the average query time for 'SELECT' in E4 was: %.4f ms, with a total of: %.4f ms\n", selectAverage, select);

            double update = fixedSizeTest.updateById(n, new E4Repository(connection));
            double updateAverage = update/n;
            System.out.printf("the average query time for 'UPDATE' in E4 was: %.4f ms, with a total of: %.4f ms\n", updateAverage, update);

            Pair<Integer, Double> delete = fixedSizeTest.delete(n, new E4Repository(connection));
            double deleteAverage = delete.getRight()/delete.getLeft();
            System.out.printf("The average query time for 'DELETE' in E4 was %.4f ms, with a total of : %.4f ms in %d operations\n", deleteAverage, delete.getRight(), delete.getLeft());

            if (delete.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", delete.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public StrategyResult testE5(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE5(name, password)){
            FixedSizeTest<ConcreteTableTest, ConcreteTableRepository> fixedSizeTest = new FixedSizeTest<>(new ConcreteTableTest(), size);

            double create = fixedSizeTest.createBatch(n, new ConcreteTableRepository(connection));
            double createAverage = create/n;
            System.out.printf("the average query time for 'CREATE' in E5 was: %.4f ms, with a total of %.4f ms\n", createAverage, create);

            double select = fixedSizeTest.selectLimit(10*1000, n, new ConcreteTableRepository(connection));
            double selectAverage = select/n;
            System.out.printf("the average query time for 'SELECT' in E5 was: %.4f ms, with a total of: %.4f ms\n", selectAverage, select);

            double update = fixedSizeTest.updateById(n, new ConcreteTableRepository(connection));
            double updateAverage = update/n;
            System.out.printf("the average query time for 'UPDATE' in E5 was: %.4f ms, with a total of: %.4f ms\n", updateAverage, update);

            Pair<Integer, Double> delete = fixedSizeTest.delete(n, new ConcreteTableRepository(connection));
            double deleteAverage = delete.getRight()/delete.getLeft();
            System.out.printf("The average query time for 'DELETE' in E5 was %.4f ms, with a total of : %.4f ms in %d operations\n", deleteAverage, delete.getRight(), delete.getLeft());

            if (delete.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", delete.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public StrategyResult testE6(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE6(name, password)){
            FixedSizeTest<ClassTableTest, ClassTableRepository> fixedSizeTest = new FixedSizeTest<>(new ClassTableTest(), size);

            double create = fixedSizeTest.createBatch(n, new ClassTableRepository(connection));
            double createAverage = create/n;
            System.out.printf("the average query time for 'CREATE' in E6 was: %.4f ms, with a total of %.4f ms\n", createAverage, create);

            double select = fixedSizeTest.selectLimit(10*1000, n, new ClassTableRepository(connection));
            double selectAverage = select/n;
            System.out.printf("the average query time for 'SELECT' in E6 was: %.4f ms, with a total of: %.4f ms\n", selectAverage, select);

            double update = fixedSizeTest.updateById(n, new ClassTableRepository(connection));
            double updateAverage = update/n;
            System.out.printf("the average query time for 'UPDATE' in E6 was: %.4f ms, with a total of: %.4f ms\n", updateAverage, update);

            Pair<Integer, Double> delete = fixedSizeTest.delete(n, new ClassTableRepository(connection));
            double deleteAverage = delete.getRight()/delete.getLeft();
            System.out.printf("The average query time for 'DELETE' in E6 was %.4f ms, with a total of : %.4f ms in %d operations\n", deleteAverage, delete.getRight(), delete.getLeft());

            if (delete.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", delete.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
