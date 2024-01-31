package rodrigues.igor.test;

import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.csv.CSVReader;
import rodrigues.igor.database.repository.E3Repository;
import rodrigues.igor.generator.PessoaGenerator;
import rodrigues.igor.model.Pessoa;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class E3Test {
    public double createBatch(int n, E3Repository repository){
        ArrayList<Pessoa> pessoas = new PessoaGenerator().generateList(n);
        return repository.create(pessoas);
    }

    public double selectLimit(int limit,  int repetitions, E3Repository repository) {
        double sum = 0;
        for (int i = 0; i < repetitions; i++){
            sum += repository.selectLimit(limit);
        }
        return sum;
    }

    public double update(int repetitions, E3Repository repository){
        double sum = 0;

        Pessoa pessoa = repository.getOne();
        try {
            ArrayList<String> names = new CSVReader().getNames();
            for (int i = 0; i < repetitions; i++) {
                pessoa.randomize(names);
                sum += repository.update(pessoa, pessoa.getId().toString());
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return sum;
    }

    public Pair<Integer, Double> delete(int repetitions, E3Repository repository){
        double sum = 0;
        List<Pessoa> pessoas = repository.getAll(repetitions);
        for (Pessoa p : pessoas){
            sum += repository.delete(p.getId().toString());
        }
        return Pair.of(pessoas.size(), sum);
    }
}
