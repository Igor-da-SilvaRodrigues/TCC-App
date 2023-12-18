package rodrigues.igor.generator;


import rodrigues.igor.csv.CSVReader;
import rodrigues.igor.model.Pessoa;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Random;

public class PessoaGenerator implements Generator<Pessoa> {

    @Override
    public ArrayList<Pessoa> generateList(int n) {
        Pessoa pessoa = new Pessoa();
        ArrayList<Pessoa> list = new ArrayList<>();
        Random random = new Random();
        ArrayList<String> names;
        try {
            names = new CSVReader().getNames();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < n; i++){
            Pessoa randomPessoa = pessoa.getRandom();
            randomPessoa.setNome(names.get(random.nextInt(names.size())));
            list.add(randomPessoa);
        }

        return list;
    }
}
