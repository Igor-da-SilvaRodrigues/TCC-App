package rodrigues.igor.test;

import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.csv.CSVReader;
import rodrigues.igor.database.repository.ConcreteTableRepository;
import rodrigues.igor.generator.PessoaGenerator;
import rodrigues.igor.model.Pessoa;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

/**
 * E5 Concrete table com ID universal
 */
public class ConcreteTableTest implements GenericTest<ConcreteTableRepository>{
    public double createBatch(int n, ConcreteTableRepository repository){
        ArrayList<Pessoa> pessoas = new PessoaGenerator().generateList(n);
        return repository.create(pessoas);
    }

    /**
     * A Read test that selects a given number of entities.
     * @param limit the maximum number of entities to be queried
     * @param repetitions the number of times the query will be executed
     * @return the total sql query time
     */
    public double selectLimit(int limit, int repetitions, ConcreteTableRepository repository){
        double sum = 0;
        for (int i = 0; i < repetitions; i++){
            sum += repository.selectPFLimit(limit);
        }
        return sum;
    }

    public double update(int repetitions, ConcreteTableRepository repository){
        double sum = 0;

        Pessoa pessoa = repository.getOne();//fetch a single entity to repeatedly update
        try {
            ArrayList<String> names = new CSVReader().getNames();
            for (int i = 0; i < repetitions; i++) {
                pessoa.randomize(names);//randomize generic and specialized data
                sum += repository.updateById(pessoa, pessoa.getId().toString());
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return sum;
    }

    /**
     * Deletes n entities from the database
     * @param repetitions the desired number of repetitions
     * @return A pair. The left value contains the number of delete operations performed, the right value contains the
     * total sql query time.
     */
    public Pair<Integer, Double> delete(int repetitions, ConcreteTableRepository repository){
        double sum = 0;


        List<Pessoa> pessoaList = repository.getAll(repetitions);
        for (Pessoa pf : pessoaList){
            sum += repository.deleteById(pf, pf.getId().toString());
        }

        return Pair.of(pessoaList.size(), sum);
    }
}
