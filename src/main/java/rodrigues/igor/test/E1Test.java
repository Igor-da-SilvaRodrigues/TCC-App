package rodrigues.igor.test;

import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.csv.CSVReader;
import rodrigues.igor.database.repository.E1Repository;
import rodrigues.igor.generator.PessoaGenerator;
import rodrigues.igor.model.Pessoa;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class E1Test{
    /**
     * Creates a set of random entities
     * @return the SQL query time.
     * @throws SQLException if an sql error occurred;
     */
    public double createBatch(int n, E1Repository repository) throws SQLException {
        ArrayList<Pessoa> pessoas = new PessoaGenerator().generateList(n);
        return repository.create(pessoas);
    }

    /**
     * Selects all information from the hierarchy
     * @param limit the maximum number of entities to be queried.
     * @param repetitions the number of times the query will be executed.
     * @return The total sql query time.
     */
    public double selectLimit(int limit, int repetitions, E1Repository repository) {
        double sum = 0;
        for (int i = 0; i < repetitions; i++){
            sum += repository.selectLimit(limit);
        }
        return sum;
    }

    /**
     * Updates an entity in the database n times. Only one entity is ever updated for the sake of simplicity, consistency and fairness.
     * @param repetitions the number of times the test will be performed
     * @return the total sql query time
     */
    public double update(int repetitions, E1Repository repository){
        double sum = 0;

        Pessoa pessoa = repository.getOne();
        try {
            ArrayList<String> names = new CSVReader().getNames();
            for (int i = 0; i < repetitions; i++){
                pessoa.randomize(names);
                sum += repository.updateById(pessoa, pessoa.getId().toString());
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return sum;
    }

    /**
     * Deletes n entities from the database.
     * @param repetitions the desired number of repetitions
     * @return A pair. The left value contains the number of delete operations performed, the right value contains the
     * total sql query time.
     */
    public Pair<Integer, Double> delete(int repetitions, E1Repository repository){
        double sum = 0;

        //getting n entities to delete. The actual amount of retrieved entities may be lower than the amount requested.
        //by the caller
        List<Pessoa> pessoaList = repository.getAll(repetitions);

        for (Pessoa p : pessoaList){
            sum += repository.deleteById(p.getId().toString());
        }

        return Pair.of(pessoaList.size(), sum);
    }

}
