package rodrigues.igor.model;

import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

public class Pessoa{
    private UUID id;
    private String nome;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }


    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }


    /**
     * Generates a random Pessoa object. This will randomly be either a PF or PJ object.
     */
    public static Pessoa getRandom() {
        Random random = new Random();
        int type = random.nextInt(2);//0=PF, 1 = PJ
        return type == 0 ? PessoaFisica.getRandom() : PessoaJuridica.getRandom();
    }

    /**
     * Generates a random, pure Pessoa object. For internal use only.
     * Pure means this object is a true generic entity, and doesn't belong to any of the subclasses that extend this one
     * <br>
     * This method doesn't set a random name for the object, as the CSV file is too large and this method is meant to
     * be called multiple times. So the csv file should be read externally only once, and used to insert random names
     * to the output of this method.
     */
    protected static Pessoa getPureRandom(){
        Pessoa pessoa = new Pessoa();
        pessoa.setId(UUID.randomUUID());
        pessoa.setNome("t");
        return pessoa;
    }

    /**
     * Randomizes this entity. This is method will only randomize class specific data, except for the ID. This means it
     * will never make the entity 'switch' subtypes, as may happen with {@link Pessoa#getRandom()}.
     * @param names A list of names to be used as source for a new random name
     *
     */
    public void randomize(List<String> names){
        this.nome = names.get(new Random().nextInt(names.size()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pessoa pessoa = (Pessoa) o;
        return Objects.equals(id, pessoa.id) && Objects.equals(nome, pessoa.nome);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, nome);
    }

    @Override
    public String toString() {
        return "Pessoa{" +
                "id=" + id +
                ", nome='" + nome + '\'' +
                '}';
    }
}
