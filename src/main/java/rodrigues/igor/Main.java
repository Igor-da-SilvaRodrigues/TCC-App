package rodrigues.igor;

import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.database.ConnectionDAO;
import rodrigues.igor.database.repository.*;
import rodrigues.igor.generator.PessoaGenerator;
import rodrigues.igor.model.Pessoa;
import rodrigues.igor.test.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        String name = askName();
        String password = askPassword();

        int n = 1 * 1000;//sample size

        CompletableFuture<Void> futureE6 = CompletableFuture.runAsync(()-> testE6(name, password, n));
        CompletableFuture<Void> futureE5 = CompletableFuture.runAsync(()-> testE5(name, password, n));
        CompletableFuture<Void> futureE4 = CompletableFuture.runAsync(()-> testE4(name, password, n));
        CompletableFuture<Void> futureE3 = CompletableFuture.runAsync(()-> testE3(name, password, n));
        CompletableFuture<Void> futureE2 = CompletableFuture.runAsync(()-> testE2(name, password, n));
        CompletableFuture<Void> futureE1 = CompletableFuture.runAsync(()-> testE1(name, password, n));

        futureE6.get();
        futureE5.get();
        futureE4.get();
        futureE3.get();
        futureE2.get();
        futureE1.get();
    }

    private static void testE1(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE1(name, password)){
            double e1CreateBatchTotal = new E1Test().createBatch(n, new E1Repository(connection));
            System.out.printf("the average query time for 'CREATE' in E1 was: %.4f ms\n", e1CreateBatchTotal/n);

            double e1SelectBatchTotal = new E1Test().selectLimit(10*1000, n, new E1Repository(connection));
            System.out.printf("the average query time for 'SELECT' in E1 was: %.4f ms, with a total of: %.4f ms\n", e1SelectBatchTotal/n, e1SelectBatchTotal);

            double e1UpdateBatchTotal = new E1Test().update(n, new E1Repository(connection));
            System.out.printf("the average query time for 'UPDATE' in E1 was: %.4f ms, with a total of: %.4f ms\n", e1UpdateBatchTotal/n, e1UpdateBatchTotal);

            Pair<Integer, Double> e1DeleteBatchTotal = new E1Test().delete(n, new E1Repository(connection));
            System.out.printf("The average query time for 'DELETE' in E1 was %.4f ms, with a total of : %.4f ms in %d operations\n", e1DeleteBatchTotal.getRight()/e1DeleteBatchTotal.getLeft(), e1DeleteBatchTotal.getRight(), e1DeleteBatchTotal.getLeft());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
    private static void testE2(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE2(name, password)){
            double e2Create = new E2Test().createBatch(n , new E2Repository(connection));
            System.out.printf("The average query time for 'CREATE' in E2 was: %.4f ms with a total of %.4f\n", e2Create/n, e2Create );

            double e2Select = new E2Test().selectLimit(10*1000, n, new E2Repository(connection));
            System.out.printf("The average query time for 'SELECT' in E2 was: %.4f ms with a total of %.4f\n", e2Select/n, e2Select );

            double e2Update = new E2Test().update(n, new E2Repository(connection));
            System.out.printf("The average query time for 'UPDATE' in E2 was: %.4f ms with a total of %.4f\n", e2Update/n, e2Update );

            Pair<Integer, Double> e2Delete = new E2Test().delete(n, new E2Repository(connection));
            System.out.printf("The average query time for 'UPDATE' in E2 was: %.4f ms with a total of %.4f ms in %d operations\n", e2Delete.getRight()/e2Delete.getLeft(), e2Delete.getRight(), e2Delete.getLeft());

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static void testE3(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE3(name, password)){
            double e3Create = new E3Test().createBatch(n, new E3Repository(connection));
            System.out.printf("The average query time for 'CREATE' in E3 was: %.4f ms with a total of %.4f\n", e3Create/n, e3Create);

            double e3Select = new E3Test().selectLimit(10*1000, n, new E3Repository(connection));
            System.out.printf("The average query time for 'SELECT' in E3 was: %.4f ms with a total of %.4f\n", e3Select/n, e3Select);

            double e3Update = new E3Test().update(n, new E3Repository(connection));
            System.out.printf("The average query time for 'UPDATE' in E3 was: %.4f ms with a total of %.4f\n", e3Update/n, e3Update);

            Pair<Integer, Double> e3Delete = new E3Test().delete(n, new E3Repository(connection));
            System.out.printf("The average query time for 'DELETE' in E3 was : %.4f ms with a total of %.3f ms in %d operations\n", e3Delete.getRight()/e3Delete.getLeft(), e3Delete.getRight(), e3Delete.getLeft());

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static void testE4(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE4(name, password)){
            double e4Create = new E4Test().createBatch(n, new E4Repository(connection));
            System.out.printf("The average query time for 'CREATE' in E4 was: %.4f ms with a total of %.4f\n", e4Create/n, e4Create);

            double e4Select = new E4Test().select(10*1000, n, new E4Repository(connection));
            System.out.printf("The average query time for 'SELECT' in E4 was: %.4f ms with a total of %.4f\n", e4Select/n, e4Select);

            double e4Update = new E4Test().update(n, new E4Repository(connection));
            System.out.printf("The average query time for 'UPDATE' in E4 was: %.4f ms with a total of %.4f\n", e4Update/n, e4Update);

            Pair<Integer, Double> e4Delete = new E4Test().delete(n, new E4Repository(connection));
            System.out.printf("The average query time for 'DELETE' in E4 was: %.4f ms with a total of %.4f ms in %d operations\n", e4Delete.getRight()/e4Delete.getLeft(), e4Delete.getRight(), e4Delete.getLeft());

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static void testE5(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE5(name, password)){
            double e5createTotal = new ConcreteTableTest().createBatch(n, new ConcreteTableRepository(connection));
            System.out.printf("The average query time for 'CREATE' in E5 was: %.4f ms with a total of %.4f\n", e5createTotal/n, e5createTotal);

            double e5SelectBatchTotal = new ConcreteTableTest().selectLimit(10*1000, n, new ConcreteTableRepository(connection));
            System.out.printf("The average query time for 'SELECT' in E5 was: %.4f ms with a total of: %.4f\n", e5SelectBatchTotal/n, e5SelectBatchTotal);

            double e5UpdateBatchTotal = new ConcreteTableTest().update(n, new ConcreteTableRepository(connection));
            System.out.printf("The average query time for 'UPDATE' in E5 was: %.4f ms with a total of: %.4f\n", e5UpdateBatchTotal/n, e5UpdateBatchTotal);

            Pair<Integer, Double> e5DeleteBatchTotal = new ConcreteTableTest().deletePF(n, new ConcreteTableRepository(connection));
            System.out.printf("The average query time for 'DELETE' in E5 was: %.4f ms with a total of: %.4f ms in %d operations\n", e5DeleteBatchTotal.getRight()/e5DeleteBatchTotal.getLeft(), e5DeleteBatchTotal.getRight(), e5DeleteBatchTotal.getLeft());

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static void testE6(String name, String password, int n){
        try (Connection connection = new ConnectionDAO().connectE6(name, password)){
            double e6Create = new ClassTableTest().createBatch(n, new ClassTableRepository(connection));
            System.out.printf("The average query time for 'CREATE' in E6 was: %.4f ms with a total of %.4f\n", e6Create/n, e6Create);

            double e6Select = new ClassTableTest().selectLimit(10*1000, n, new ClassTableRepository(connection));
            System.out.printf("The average query time for 'SELECT' in E6 was: %.4f ms with a total of %.4f\n", e6Select/n, e6Select);

            double e6Update = new ClassTableTest().update(n, new ClassTableRepository(connection));
            System.out.printf("The average query time for 'UPDATE' in E6 was: %.4f ms with a total of %.4f\n", e6Update/n, e6Update);

            Pair<Integer, Double> e6Delete = new ClassTableTest().delete(n, new ClassTableRepository(connection));
            System.out.printf("The average query time for 'DELETE' in E6 was: %.4f ms with a total of: %.4f ms in %d operations\n", e6Delete.getRight()/e6Delete.getLeft(), e6Delete.getRight(), e6Delete.getLeft());

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