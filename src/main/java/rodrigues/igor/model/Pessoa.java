package rodrigues.igor.model;

import java.util.Objects;
import java.util.Random;
import java.util.UUID;

public class Pessoa implements Randomizable{
    private UUID id;
    private String nome;
    private CPF cpf;
    private CNPJ cnpj;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public CPF getCpf() {
        return cpf;
    }

    public void setCpf(CPF cpf) {
        this.cpf = cpf;
    }

    public CNPJ getCnpj() {
        return cnpj;
    }

    public void setCnpj(CNPJ cnpj) {
        this.cnpj = cnpj;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    @Override
    public Pessoa getRandom() {
        Random random = new Random();
        Pessoa pessoa = new Pessoa();
        pessoa.setId(UUID.randomUUID());
        pessoa.setNome("t");
        pessoa.setCpf(new CPF());
        pessoa.setCnpj(new CNPJ());
        return pessoa;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pessoa pessoa = (Pessoa) o;
        return Objects.equals(id, pessoa.id) && Objects.equals(nome, pessoa.nome) && Objects.equals(cpf, pessoa.cpf) && Objects.equals(cnpj, pessoa.cnpj);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, nome, cpf, cnpj);
    }

    @Override
    public String toString() {
        return "Pessoa{" +
                "id=" + id +
                ", nome='" + nome + '\'' +
                ", cpf=" + cpf +
                ", cnpj=" + cnpj +
                '}';
    }
}
