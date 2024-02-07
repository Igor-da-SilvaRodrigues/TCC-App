package rodrigues.igor;

import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.database.ConnectionDAO;
import rodrigues.igor.database.repository.*;
import rodrigues.igor.test.*;
import rodrigues.igor.test.result.Result;
import rodrigues.igor.test.result.StrategyResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) {
        String name = askName();
        String password = askPassword();

        int n = 10 * 1000;//sample size


        for (int i = 0; i < 100; i++) {
            Result result = testAsync(name, password, n);
            try (Connection connection = new ConnectionDAO().connectResult(name, password)){
                new ResultRepository(connection).createResult(result);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            System.out.println("Completed loop number " + i+1);
        }

        System.out.println("Finished.");
    }

    private static Result testAsync(String name, String password, int n) {
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
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static Result test(String name, String password, int n){
        StrategyResult e6Result = testE6(name, password, n);
        StrategyResult e5Result = testE5(name, password, n);
        StrategyResult e4Result = testE4(name, password, n);
        StrategyResult e3Result = testE3(name, password, n);
        StrategyResult e2Result = testE2(name, password, n);
        StrategyResult e1Result = testE1(name, password, n);

        Result testResults = new Result();
        testResults.setResultE1(e1Result);
        testResults.setResultE2(e2Result);
        testResults.setResultE3(e3Result);
        testResults.setResultE4(e4Result);
        testResults.setResultE5(e5Result);
        testResults.setResultE6(e6Result);

        return testResults;
    }


    private static StrategyResult testE1(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE1(name, password)){
            double e1CreateBatchTotal = new E1Test().createBatch(n, new E1Repository(connection));
            double createAverage = e1CreateBatchTotal/n;
            System.out.printf("the average query time for 'CREATE' in E1 was: %.4f ms, with a total of %.4f ms\n", createAverage, e1CreateBatchTotal);

            double e1SelectBatchTotal = new E1Test().selectLimit(10*1000, n, new E1Repository(connection));
            double selectAverage = e1SelectBatchTotal/n;
            System.out.printf("the average query time for 'SELECT' in E1 was: %.4f ms, with a total of: %.4f ms\n", selectAverage, e1SelectBatchTotal);

            double e1UpdateBatchTotal = new E1Test().update(n, new E1Repository(connection));
            double updateAverage = e1UpdateBatchTotal/n;
            System.out.printf("the average query time for 'UPDATE' in E1 was: %.4f ms, with a total of: %.4f ms\n", updateAverage, e1UpdateBatchTotal);

            Pair<Integer, Double> e1DeleteBatchTotal = new E1Test().delete(n, new E1Repository(connection));
            double deleteAverage = e1DeleteBatchTotal.getRight()/e1DeleteBatchTotal.getLeft();
            System.out.printf("The average query time for 'DELETE' in E1 was %.4f ms, with a total of : %.4f ms in %d operations\n", deleteAverage, e1DeleteBatchTotal.getRight(), e1DeleteBatchTotal.getLeft());

            if (e1DeleteBatchTotal.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", e1DeleteBatchTotal.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
    private static StrategyResult testE2(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE2(name, password)){
            double e2Create = new E2Test().createBatch(n , new E2Repository(connection));
            double createAverage = e2Create/n;
            System.out.printf("The average query time for 'CREATE' in E2 was: %.4f ms with a total of %.4f\n", createAverage, e2Create );

            double e2Select = new E2Test().selectLimit(10*1000, n, new E2Repository(connection));
            double selectAverage = e2Select/n;
            System.out.printf("The average query time for 'SELECT' in E2 was: %.4f ms with a total of %.4f\n", selectAverage, e2Select );

            double e2Update = new E2Test().update(n, new E2Repository(connection));
            double updateAverage = e2Update/n;
            System.out.printf("The average query time for 'UPDATE' in E2 was: %.4f ms with a total of %.4f\n", updateAverage, e2Update );

            Pair<Integer, Double> e2Delete = new E2Test().delete(n, new E2Repository(connection));
            double deleteAverage = e2Delete.getRight()/e2Delete.getLeft();
            System.out.printf("The average query time for 'UPDATE' in E2 was: %.4f ms with a total of %.4f ms in %d operations\n", deleteAverage, e2Delete.getRight(), e2Delete.getLeft());

            if (e2Delete.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", e2Delete.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static StrategyResult testE3(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE3(name, password)){
            double e3Create = new E3Test().createBatch(n, new E3Repository(connection));
            double createAverage = e3Create/n;
            System.out.printf("The average query time for 'CREATE' in E3 was: %.4f ms with a total of %.4f\n", createAverage, e3Create);

            double e3Select = new E3Test().selectLimit(10*1000, n, new E3Repository(connection));
            double selectAverage = e3Select/n;
            System.out.printf("The average query time for 'SELECT' in E3 was: %.4f ms with a total of %.4f\n", selectAverage, e3Select);

            double e3Update = new E3Test().update(n, new E3Repository(connection));
            double updateAverage = e3Update/n;
            System.out.printf("The average query time for 'UPDATE' in E3 was: %.4f ms with a total of %.4f\n", updateAverage, e3Update);

            Pair<Integer, Double> e3Delete = new E3Test().delete(n, new E3Repository(connection));
            double deleteAverage = e3Delete.getRight()/e3Delete.getLeft();
            System.out.printf("The average query time for 'DELETE' in E3 was : %.4f ms with a total of %.3f ms in %d operations\n", deleteAverage, e3Delete.getRight(), e3Delete.getLeft());

            if (e3Delete.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", e3Delete.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static StrategyResult testE4(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE4(name, password)){
            double e4Create = new E4Test().createBatch(n, new E4Repository(connection));
            double createAverage = e4Create/n;
            System.out.printf("The average query time for 'CREATE' in E4 was: %.4f ms with a total of %.4f\n", createAverage, e4Create);

            double e4Select = new E4Test().select(10*1000, n, new E4Repository(connection));
            double selectAverage = e4Select/n;
            System.out.printf("The average query time for 'SELECT' in E4 was: %.4f ms with a total of %.4f\n", selectAverage, e4Select);

            double e4Update = new E4Test().update(n, new E4Repository(connection));
            double updateAverage = e4Update/n;
            System.out.printf("The average query time for 'UPDATE' in E4 was: %.4f ms with a total of %.4f\n", updateAverage, e4Update);

            Pair<Integer, Double> e4Delete = new E4Test().delete(n, new E4Repository(connection));
            double deleteAverage = e4Delete.getRight()/ e4Delete.getLeft();
            System.out.printf("The average query time for 'DELETE' in E4 was: %.4f ms with a total of %.4f ms in %d operations\n", deleteAverage, e4Delete.getRight(), e4Delete.getLeft());

            if (e4Delete.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", e4Delete.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static StrategyResult testE5(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE5(name, password)){
            double e5createTotal = new ConcreteTableTest().createBatch(n, new ConcreteTableRepository(connection));
            double createAverage = e5createTotal/n;
            System.out.printf("The average query time for 'CREATE' in E5 was: %.4f ms with a total of %.4f\n", createAverage, e5createTotal);

            double e5SelectBatchTotal = new ConcreteTableTest().selectLimit(10*1000, n, new ConcreteTableRepository(connection));
            double selectAverage = e5SelectBatchTotal/n;
            System.out.printf("The average query time for 'SELECT' in E5 was: %.4f ms with a total of: %.4f\n", selectAverage, e5SelectBatchTotal);

            double e5UpdateBatchTotal = new ConcreteTableTest().update(n, new ConcreteTableRepository(connection));
            double updateAverage = e5UpdateBatchTotal/n;
            System.out.printf("The average query time for 'UPDATE' in E5 was: %.4f ms with a total of: %.4f\n", updateAverage, e5UpdateBatchTotal);

            Pair<Integer, Double> e5DeleteBatchTotal = new ConcreteTableTest().deletePF(n, new ConcreteTableRepository(connection));
            double deleteAverage = e5DeleteBatchTotal.getRight()/ e5DeleteBatchTotal.getLeft();
            System.out.printf("The average query time for 'DELETE' in E5 was: %.4f ms with a total of: %.4f ms in %d operations\n", deleteAverage, e5DeleteBatchTotal.getRight(), e5DeleteBatchTotal.getLeft());

            if (e5DeleteBatchTotal.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", e5DeleteBatchTotal.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static StrategyResult testE6(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE6(name, password)){
            double e6Create = new ClassTableTest().createBatch(n, new ClassTableRepository(connection));
            double createAverage = e6Create/n;
            System.out.printf("The average query time for 'CREATE' in E6 was: %.4f ms with a total of %.4f\n", createAverage, e6Create);

            double e6Select = new ClassTableTest().selectLimit(10*1000, n, new ClassTableRepository(connection));
            double selectAverage = e6Select/n;
            System.out.printf("The average query time for 'SELECT' in E6 was: %.4f ms with a total of %.4f\n", selectAverage, e6Select);

            double e6Update = new ClassTableTest().update(n, new ClassTableRepository(connection));
            double updateAverage = e6Update/n;
            System.out.printf("The average query time for 'UPDATE' in E6 was: %.4f ms with a total of %.4f\n", updateAverage, e6Update);

            Pair<Integer, Double> e6Delete = new ClassTableTest().delete(n, new ClassTableRepository(connection));
            double deleteAverage = e6Delete.getRight()/e6Delete.getLeft();
            System.out.printf("The average query time for 'DELETE' in E6 was: %.4f ms with a total of: %.4f ms in %d operations\n", deleteAverage, e6Delete.getRight(), e6Delete.getLeft());

            if (e6Delete.getLeft() != n){
                System.out.printf("Unexpected operation size %d! was expecting %d. This result will be skipped.%n", e6Delete.getLeft(), n);
                return null;
            }

            return new StrategyResult(createAverage, selectAverage, updateAverage, deleteAverage);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }






    private static String askName(){
        System.out.print("\nEnter user name: ");
        return  new Scanner(System.in).nextLine();
    }

    private static String askPassword(){
        System.out.print("\nEnter password: ");
        return  new Scanner(System.in).nextLine();
    }


}