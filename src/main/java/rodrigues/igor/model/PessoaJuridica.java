package rodrigues.igor.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PessoaJuridica extends Pessoa{
    private CNPJ cnpj;

    public PessoaJuridica(Pessoa pessoa){
        this.setId(pessoa.getId());
        this.setNome(pessoa.getNome());
    }
    public PessoaJuridica() {
    }

    /**
     * Generates a random PJ object.
     */
    public static PessoaJuridica getRandom(){
        PessoaJuridica pessoaJuridica = new PessoaJuridica(Pessoa.getPureRandom());
        pessoaJuridica.setCnpj(new CNPJ());
        return pessoaJuridica;
    }


    public CNPJ getCnpj() {
        return cnpj;
    }


    public void setCnpj(CNPJ cnpj) {
        this.cnpj = cnpj;
    }

    @Override
    public void randomize(List<String> names){
        super.randomize(names);
        this.setCnpj(new CNPJ());
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        PessoaJuridica that = (PessoaJuridica) o;
        return Objects.equals(cnpj, that.cnpj);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cnpj);
    }

    @Override
    public String toString() {
        return "PessoaJuridica{" +
                "cnpj=" + cnpj +
                '}';
    }
}
