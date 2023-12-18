package rodrigues.igor;

import rodrigues.igor.database.ConnectionDAO;
import rodrigues.igor.database.repository.E1;
import rodrigues.igor.generator.PessoaGenerator;
import rodrigues.igor.model.Pessoa;
import rodrigues.igor.test.ResultSet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) throws IOException {
        String name = askName();
        String password = askPassword();

        try(
            Connection e1Connection = new ConnectionDAO().connectE1(name, password);
        ) {

            double avgE1Create = testE1Create(1000, new E1(e1Connection));
            System.out.printf("the average query time for 'CREATE' in E1 was: %.2f ms\n", avgE1Create);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Tests the E1 strategy by creating n entities and returns the average query time;
     */
    private static double testE1Create(int n, E1 repository) throws SQLException {

        ArrayList<Future<ResultSet>> futures = new ArrayList<>();

        int nthreads = 1;
        //we'll divide the task into threads.
        ExecutorService executorService = Executors.newFixedThreadPool(nthreads);
        for(int i = 0; i < nthreads; i++){
            int finalI = i;
            Future<ResultSet> future = executorService.submit(() -> {
                ArrayList<Pessoa> pessoas = new PessoaGenerator().generateList(n/nthreads);
                ResultSet resultSet = new ResultSet();
                for (Pessoa p : pessoas){
                    resultSet.addResult(repository.create(p));
                }

                System.out.printf("Thread %d found average of %f in %d operations with a max of %d and a min of %d\n",
                        finalI, resultSet.averageTime(), resultSet.numberOfResults(), resultSet.maxTime(), resultSet.minTime());

                return resultSet;
                //this will probably never not be 'n/nthreads' operations, and if it isn't it the program should have halted.
                //meaning storing the number of operations is probably not needed, but we'll do it just in case.
            });

            futures.add(future);
        }
        System.out.printf("Created %d threads\n", nthreads);
        executorService.shutdown();

        //wait until all threads have finished.
        try {
            System.out.println("Waiting for all threads to finish");
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long averageTimesWeight = 0;
        int sumOfWeights = 0;
        for(Future<ResultSet> future : futures){
            try{
                ResultSet result = future.get();
                //we'll calculate the weighted average of the given results
                averageTimesWeight += result.sumOfTimes();
                sumOfWeights += result.numberOfResults();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Total ammount of operations: " + sumOfWeights);
        return (double) averageTimesWeight/sumOfWeights;
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