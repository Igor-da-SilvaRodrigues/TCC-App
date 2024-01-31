package rodrigues.igor.model;

import java.util.List;
import java.util.Objects;

public class PessoaFisica extends Pessoa{

    private CPF cpf;

    public PessoaFisica(Pessoa pessoa){
        this.setId(pessoa.getId());
        this.setNome(pessoa.getNome());
    }

    public PessoaFisica() {
    }

    /**
     * Generates a random PF object.
     */
    public static PessoaFisica getRandom(){
        PessoaFisica pessoaFisica = new PessoaFisica(Pessoa.getPureRandom());
        pessoaFisica.setCpf(new CPF());
        return pessoaFisica;
    }



    public CPF getCpf() {
        return cpf;
    }


    public void setCpf(CPF cpf) {
        this.cpf = cpf;
    }

    @Override
    public void randomize(List<String> names){
        super.randomize(names);
        this.setCpf(new CPF());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        PessoaFisica that = (PessoaFisica) o;
        return Objects.equals(cpf, that.cpf);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cpf);
    }

    @Override
    public String toString() {
        return "PessoaFisica{" +
                "cpf=" + cpf +
                '}';
    }
}
