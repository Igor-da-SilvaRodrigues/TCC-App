package rodrigues.igor;

import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.database.ConnectionDAO;
import rodrigues.igor.database.repository.ConcreteTable;
import rodrigues.igor.database.repository.E1;
import rodrigues.igor.generator.PessoaGenerator;
import rodrigues.igor.model.Pessoa;
import rodrigues.igor.test.BatchResultSet;
import rodrigues.igor.test.E1Test;
import rodrigues.igor.test.ResultSet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) throws IOException {
        String name = askName();
        String password = askPassword();
        try(
            Connection e1Connection = new ConnectionDAO().connectE1(name, password);
            Connection e5Connection = new ConnectionDAO().connectE5(name, password);
        ) {

            int n = 10 * 1000;//sample size



            PessoaGenerator pessoaGenerator = new PessoaGenerator();
            double e5createTotal = new ConcreteTable(e5Connection).create(pessoaGenerator.generateList(n));
            System.out.printf("The average query time for 'CREATE' in E5 was: %.4f ms with a total of %.4f\n%n", e5createTotal/n, e5createTotal);

            double e1CreateBatchTotal = new E1Test().createBatch(n, new E1(e1Connection));
            System.out.printf("the average query time for 'CREATE' in E1 was: %.4f ms\n", e1CreateBatchTotal/n);
/*
            double e1SelectBatchTotal = new E1Test().selectLimit(1, n, new E1(e1Connection));
            System.out.printf("the average query time for 'SELECT' in E1 was: %.4f ms, with a total of: %.4f ms\n", e1SelectBatchTotal/n, e1SelectBatchTotal);

            double e1UpdateBatchTotal = new E1Test().update(n, new E1(e1Connection));
            System.out.printf("the average query time for 'UPDATE' in E1 was: %.4f ms, with a total of: %.4f ms\n", e1UpdateBatchTotal/n, e1UpdateBatchTotal);

            Pair<Integer, Double> e1DeleteBatchTotal = new E1Test().delete(n, new E1(e1Connection));
            System.out.printf("The average query time for 'DELETE' in E1 was %.4f ms, with a total of : %.4f ms in %d operations\n", e1DeleteBatchTotal.getRight()/e1DeleteBatchTotal.getLeft(), e1DeleteBatchTotal.getRight(), e1DeleteBatchTotal.getLeft());*/
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Here we are creating 4 threads, each creating a batch insert operation of 250k entities (total of 1kk).
     * Hopefully this will reduce the execution time from ~30m to under 10m ~IT DIDN'T
     */
    private static double testE1CreateBatchMultiThread(int nentities, E1 repository) throws SQLException{
        ArrayList<Future<BatchResultSet>> futures = new ArrayList<>();

        int nthreads = 4;

        //creating threads
        ExecutorService executorService = Executors.newFixedThreadPool(nthreads);
        for (int i = 0; i < nthreads; i++){
            int finalI = i;
            Future<BatchResultSet> future = executorService.submit(() -> {
                ArrayList<Pessoa> pessoas = new PessoaGenerator().generateList(nentities/nthreads);
                BatchResultSet resultSet = new BatchResultSet();
                resultSet.setNresults(pessoas.size());
                resultSet.setResult(repository.create(pessoas));
                System.out.printf("Thread %d found average of %f ms in %d operations taking a total of %d ms\n",
                        finalI, resultSet.averageTime(), resultSet.getNresults(), resultSet.getResult());

                return resultSet;
            });
            futures.add(future);
        }
        System.out.printf("Created %d threads\n", nthreads);

        //scheduling shutdown
        executorService.shutdown();

        //wait until all threads have finished.
        try {
            System.out.println("Waiting for all threads to finish");
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //getting final results
        long sumOfResults = 0;
        long sumOfWeights = 0;
        for (Future<BatchResultSet> future : futures){
            try{
                BatchResultSet resultSet = future.get();
                sumOfResults += resultSet.getResult();
                sumOfWeights += resultSet.getNresults();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        System.out.println("Total ammount of operations: " + sumOfWeights);
        return (double) sumOfResults/sumOfWeights;
    }

    private static double testE1CreateBatch(int n, E1 repository) throws SQLException{
        ArrayList<Pessoa> pessoas = new PessoaGenerator().generateList(n);
        return repository.create(pessoas);
    }

    /**
     * Tests the E1 strategy by creating n entities and returns the average query time;
     */
    private static double testE1Create(int n, E1 repository) throws SQLException {

        ArrayList<Future<ResultSet>> futures = new ArrayList<>();

        int nthreads = 1000;
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

    static class BatchCreateTask implements Callable<ResultSet> {
        List<Pessoa> list;
        public BatchCreateTask(List<Pessoa> list) {
            this.list = list;
        }

        @Override
        public ResultSet call() throws Exception {
            return null;
        }
    }
}