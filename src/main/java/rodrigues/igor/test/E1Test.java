package rodrigues.igor.test;

import com.mysql.cj.result.IntegerValueFactory;
import com.sun.jdi.IntegerValue;
import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.database.repository.E1;
import rodrigues.igor.generator.PessoaGenerator;
import rodrigues.igor.model.Pessoa;

import javax.swing.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class E1Test{
    /**
     * Creates a set of random entities
     * @return the SQL query time.
     * @throws SQLException if an sql error occurred;
     */
    public double createBatch(int n, E1 repository) throws SQLException {
        ArrayList<Pessoa> pessoas = new PessoaGenerator().generateList(n);
        return repository.create(pessoas);
    }

    /**
     * Selects all information from the hierarchy, preserving the structure of the data.
     * @param n the maximum number of entities to be queried.
     * @param repetitions the number of times the query will be executed.
     * @return The total sql query time.
     */
    public double selectLimit(int n, int repetitions, E1 repository) {
        double sum = 0;
        for (int i = 0; i < repetitions; i++){
            sum += repository.selectLimit(n);
        }
        return sum;
    }

    /**
     * Updates an entity in the database n times. Only one entity is ever updated for the sake of consistency and fairness.
     * @param repetitions the number of times the test will be performed
     * @return the total sql query time
     */
    public double update(int repetitions, E1 repository){
        double sum = 0;

        String id = repository._getRandomGenericId();
        for(int i = 0; i < repetitions; i++){
            Pessoa p = new Pessoa().getRandom();
            p.setNome("alteration1");
            sum += repository.updateById(p, id);
        }
        return sum;
    }

    /**
     * Deletes n entities from the database.
     * @param repetitions the desired number of repetitions
     * @return A pair. The left value contains the number of delete operations performed, the right value contains the
     * total sql query time.
     */
    public Pair<Integer, Double> delete(int repetitions, E1 repository){
        double sum = 0;

        //getting n entities to delete. The actual amount of retrieved entities may be lower than the amount requested.
        //by the caller
        List<Pessoa> pessoaList = repository._getAll(repetitions);

        for (Pessoa p : pessoaList){
            sum += repository.deleteById(p.getId().toString());
        }

        return Pair.of(pessoaList.size(), sum);
    }

}
