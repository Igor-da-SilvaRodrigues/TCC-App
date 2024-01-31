package rodrigues.igor.test;

import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.csv.CSVReader;
import rodrigues.igor.database.repository.ClassTableRepository;
import rodrigues.igor.generator.PessoaGenerator;
import rodrigues.igor.model.Pessoa;
import rodrigues.igor.model.PessoaFisica;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class ClassTableTest {

    public double createBatch(int n, ClassTableRepository repository){
        ArrayList<Pessoa> pessoas = new PessoaGenerator().generateList(n);
        return repository.create(pessoas);
    }

    public double selectLimit(int limit, int repetitions, ClassTableRepository repository){
        double sum = 0;
        for (int i = 0; i < repetitions; i++){
            sum+= repository.selectPfLimit(limit);
        }
        return sum;
    }

    public double update(int repetitions, ClassTableRepository repository){
        double sum = 0;

        Pessoa pessoa = repository.getOne(); //we'll fetch some entity to repeatedly update.
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

    public Pair<Integer, Double> delete(int repetitions, ClassTableRepository repository){
        double sum = 0;

        List<Pessoa> pessoaList = repository.getAll(repetitions);
        for (Pessoa p : pessoaList){
            sum += repository.deleteById(p, p.getId().toString());
        }
        return Pair.of(pessoaList.size(), sum);
    }
}
