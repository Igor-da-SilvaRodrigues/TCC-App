package rodrigues.igor.test;

import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.database.repository.E3Repository;
import rodrigues.igor.generator.PessoaGenerator;
import rodrigues.igor.model.Pessoa;

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
        String id = repository.getRandomGenericId();
        for (int i = 0; i < repetitions; i++) {
            Pessoa p = Pessoa.getRandom();
            p.setNome("alteration%d".formatted(i));
            sum += repository.update(p, id);
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
