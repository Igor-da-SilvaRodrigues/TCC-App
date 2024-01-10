package rodrigues.igor.generator;


import rodrigues.igor.csv.CSVReader;
import rodrigues.igor.model.Pessoa;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Random;

public class PessoaGenerator{

    public ArrayList<Pessoa> generateList(int n) {
        ArrayList<Pessoa> list = new ArrayList<>();
        Random random = new Random();
        ArrayList<String> names;
        try {
            names = new CSVReader().getNames();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < n; i++){
            Pessoa randomPessoa = Pessoa.getRandom();
            randomPessoa.setNome(names.get(random.nextInt(names.size())));
            list.add(randomPessoa);
        }

        return list;
    }
}
