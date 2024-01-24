package rodrigues.igor.test;

import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.database.repository.ClassTableRepository;
import rodrigues.igor.generator.PessoaGenerator;
import rodrigues.igor.model.Pessoa;
import rodrigues.igor.model.PessoaFisica;

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

        String id = repository.getRandomPFId();
        for (int i = 0; i < repetitions; i++){
            PessoaFisica pf = PessoaFisica.getRandom();
            pf.setNome("alteration%d".formatted(i));
            sum +=  repository.updateById(pf, id);
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
