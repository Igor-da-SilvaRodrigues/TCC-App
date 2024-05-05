package rodrigues.igor.test.fixedsize;

import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.database.repository.TestRepository;
import rodrigues.igor.generator.PessoaGenerator;
import rodrigues.igor.model.Pessoa;
import rodrigues.igor.test.GenericTest;

import java.util.List;

/**
 * Class to house tests that maintain a fixed size table before running
 */
public class FixedSizeTest<TEST extends GenericTest<REPOSITORY>, REPOSITORY extends TestRepository>{

    private TEST tester;
    private int size;

    /**
     *
     * @param tester The GenericTest object that will be used to execute the tests.
     * @param size The desired size that this class wil try to maintain.
     */
    public FixedSizeTest(TEST tester, int size) {
        this.tester = tester;
        this.size = size;
    }

    /**
     * Asserts that the table has the right size, and executes a "Create" test;
     * @return
     */
    public double createBatch(int n, REPOSITORY repository) {
        assertAndCorrectSize(repository);
        System.out.println("Inserting " + n + " Entities");
        return tester.createBatch(n, repository);
    }

    /**
     * Asserts that the table has the right size, and executes a "Select" test;
     * @param limit the maximum number of entities to be queried.
     * @param repetitions the number of times the query will be executed.
     * @param repository the table repository to be used by the tester
     * @return
     */
    public double selectLimit(int limit, int repetitions, REPOSITORY repository) {
        assertAndCorrectSize(repository);
        return tester.selectLimit(limit, repetitions, repository);
    }

    /**
     * Asserts that the table has the right size, and executes a "Update" test;
     * @param repetitions the number of times the query will be executed.
     * @param repository the table repository to be used by the tester
     * @return
     */
    public double updateById(int repetitions, REPOSITORY repository) {
        assertAndCorrectSize(repository);
        return tester.update(repetitions, repository);
    }

    /**
     * Asserts that the table has the right size, and executes a "Delete" test
     * @param repetitions the number of times the query will be executed.
     * @param repository the table repository to be used by the tester
     * @return
     */
    public Pair<Integer, Double> delete(int repetitions, REPOSITORY repository) {
        assertAndCorrectSize(repository);
        return tester.delete(repetitions, repository);
    }

    private void assertAndCorrectSize(REPOSITORY repository){
        int currentCount = repository.count();
        int delta = currentCount - this.size;
        if (delta == 0){
            //the size is correct, do nothing
            System.out.println("The size is correct: " + (currentCount + delta));
            return;
        }
        System.out.println("Correcting table size. from: " + currentCount + " to: " + (currentCount - delta));
        if(delta > 0){
            //we have more entities than desired, we should delete DELTA entities.
            System.out.println("Deleting " + delta + " entities");
            List<Pessoa> pessoaList = repository.getAll(delta);
            for (Pessoa p : pessoaList){
                repository.delete(p);
            }
        }else{
            //we have less entities than desired, we should insert DELTA entities.
            System.out.println("Inserting " + (-delta) + " entities");
            List<Pessoa> pessoaList = new PessoaGenerator().generateList(-delta);//delta  is negative, se we need to invert
            repository.create(pessoaList);
        }

    }
}
