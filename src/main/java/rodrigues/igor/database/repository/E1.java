package rodrigues.igor.database.repository;

import rodrigues.igor.database.ConnectionDAO;
import rodrigues.igor.model.Pessoa;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

/**
 * Toda a estratégia é mapeada em apenas uma tabela.
 * A chave primária da tabela representará os atributos identificadores das entidades genéricas e especializadas.
 * Não será permitido um valor discriminador para o conjunto genérico.
 */
public class E1 {
    public static final String DB_NAME = "tcc_e1";

    private final Connection connection;

    public E1(Connection connection) {
        this.connection = connection;
    }

    /**
     * Creates the provided entity
     *
     * @return the query time in ms
     */
    public long create (Pessoa pessoa){
        //Randomizing user type
        Type[] types = {Type.PF, Type.PJ};
        Random random = new Random();
        Type type = types[random.nextInt(types.length)];

        long time;
        switch (type) {
            case PF -> time = createPF(pessoa);
            case PJ -> time = createPJ(pessoa);
            default -> throw new RuntimeException("Illegal Type.");
        }
        return time;
    }

    private long createPJ(Pessoa pessoa) {
        String sql = "insert into pessoa(id,nome,cnpj,tipo) values (?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, pessoa.getId().toString());
            statement.setString(2, pessoa.getNome());
            statement.setString(3, pessoa.getCnpj().getAsString());
            statement.setString(4, Type.PJ.getLabel());

            long before = System.currentTimeMillis(); //measure time before execution;
            statement.execute();
            long after = System.currentTimeMillis();//measure time after execution;
            return after-before;
        } catch (SQLException e) {
            throw new RuntimeException(
                String.format("Exception on create PJ.\n exec - %s\n%s", sql, e)
            );
        }
    }

    private long createPF(Pessoa pessoa) {
        String sql = "insert into pessoa(id,nome,cpf,tipo) values (?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, pessoa.getId().toString());
            statement.setString(2, pessoa.getNome());
            statement.setString(3, pessoa.getCpf().getAsString());
            statement.setString(4, Type.PF.getLabel());

            long before = System.currentTimeMillis();
            statement.execute();
            long after = System.currentTimeMillis();
            return after-before;
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format("Exception on create PF.\n exec - %s\n%s", sql, e)
            );
        }
    }

    private enum Type{
        PF("PF"), PJ("PJ");

        private String label;

        Type(String label) {
            this.label = label;
        }

        public String getLabel() {
            return this.label;
        }
    }

}
