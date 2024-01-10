package rodrigues.igor.database.repository;

import org.apache.commons.lang3.NotImplementedException;
import rodrigues.igor.model.Pessoa;

import java.sql.Connection;
import java.util.List;

/**
 * E5 Uma tabela é criada para cada conjunto de entidades especializado.
 * A chave primária das tabelas representará o atributo identificador da entidade genérica e da entidade especializada
 * simultaneamente.
 */
public class ConcreteTable {

    public static final String DB_NAME = "tcc_e5";
    private final Connection connection;

    public ConcreteTable(Connection connection) {
        this.connection = connection;
    }

    /**
     * Creates the provided entity
     * @return the sql query time in ms.
     */
    public long create(Pessoa pessoa){
        throw new NotImplementedException("Not implemented yet");
    }

    /**
     * Creates the provided entities in a batch create operation.
     * @return the sql query time in ms.
     */
    public long create(List<Pessoa> pessoaList){
        throw new NotImplementedException("Not implemented yet");
    }



}
