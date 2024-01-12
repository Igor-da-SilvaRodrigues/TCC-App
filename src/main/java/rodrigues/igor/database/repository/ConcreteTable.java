package rodrigues.igor.database.repository;

import org.apache.commons.lang3.NotImplementedException;
import rodrigues.igor.model.Pessoa;
import rodrigues.igor.model.PessoaFisica;
import rodrigues.igor.model.PessoaJuridica;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
     * Creates the provided entities.
     * Because this strategy consists of several tables, we can't use a single large insert operation.
     * <br>We could create one large operation for each table, but this would require extracting sublists from the given list.
     * This would also require creating a new sublist every time we create new specialized entity sets.
     * <br>Instead, we will merely insert one entity at a time, each in it's own insert operation.
     * This will likely cause lower performance overall.
     * @return the sql query time in ms.
     */
    public double create(List<Pessoa> pessoaList){
        long sum = 0;

        for(Pessoa pessoa : pessoaList){
            if (pessoa instanceof PessoaFisica){
                sum += createPF((PessoaFisica) pessoa);
            } else if (pessoa instanceof PessoaJuridica) {
                sum += createPJ((PessoaJuridica) pessoa);
            }else{
                throw new RuntimeException("Pure Pessoa object");
            }
        }

        return sum;
    }

    public double createPF(PessoaFisica pessoaFisica){
        String sql = "insert into pessoafisica(id, nome, cpf) values (?,?,?)";
        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, pessoaFisica.getId().toString());
            statement.setString(2, pessoaFisica.getNome());
            statement.setString(3, pessoaFisica.getCpf().getAsString());

            long before = System.currentTimeMillis();
            statement.execute();
            long after = System.currentTimeMillis();

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double createPJ(PessoaJuridica pessoaJuridica){
        String sql = "insert into pessoajuridica(id, nome, cnpj) values (?, ?, ?)";
        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, pessoaJuridica.getId().toString());
            statement.setString(2, pessoaJuridica.getNome());
            statement.setString(3, pessoaJuridica.getCnpj().getAsString());

            long before = System.currentTimeMillis();
            statement.execute();
            long after = System.currentTimeMillis();

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


    /**
     * Selects up to n entities from the database. In this strategy we can query several specialized tables, they are all
     * the same in essence, containing the attributes for the generic entity set and the corresponding specialized set.
     * <br> Because of this, for the sake of simplicity, we will only ever query one of the tables and ignore the others.
     * @param n
     * @return the sql query time
     */
    public double selectPFLimit(int n){
        String sql = "select pf.id, pf.nome, pf.cpf from pessoafisica pf";
        if(n > 0){
            sql += " limit ?";
        }

        try (PreparedStatement statement = connection.prepareStatement(sql)){
            if(n>0){statement.setInt(1, n);}

            long before = System.currentTimeMillis();
            statement.execute();
            long after = System.currentTimeMillis();
            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double updateById(Pessoa pessoa, String id){
        if(pessoa instanceof PessoaJuridica){
            return updatePJById((PessoaJuridica) pessoa, id);
        } else if (pessoa instanceof  PessoaFisica) {
            return updatePFById((PessoaFisica) pessoa, id);
        }else {
            throw new RuntimeException("Pure Pessoa object");
        }
    }

    public double updatePFById(PessoaFisica pessoa, String id){
        String sql = "update pessoafisica set nome = ?, cpf = ? where id = ?";
        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, pessoa.getNome());
            statement.setString(2, pessoa.getCpf().getAsString());
            statement.setString(3, id);

            long before = System.currentTimeMillis();
            statement.executeUpdate();
            long after = System.currentTimeMillis();

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double updatePJById(PessoaJuridica pessoa, String id){
        String sql = "update pessoajuridica set nome = ?, cnpj = ? where id = ?";
        try(PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, pessoa.getNome());
            statement.setString(2, pessoa.getCnpj().getAsString());
            statement.setString(3, id);

            long before = System.currentTimeMillis();
            statement.executeUpdate();
            long after = System.currentTimeMillis();

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double deleteById(Pessoa pessoa, String id){
        if (pessoa instanceof PessoaFisica){
            return deletePFById(id);
        } else if (pessoa instanceof PessoaJuridica) {
            return deletePJById(id);
        }else {
            throw new RuntimeException("Pure Pessoa object");
        }
    }

    private double deletePJById(String id) {
        String sql = "delete from pessoajuridica pj where pj.id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, id);

            long before = System.currentTimeMillis();
            int result = statement.executeUpdate();
            long after = System.currentTimeMillis();

            if(result == 0){
                System.out.printf("%s.deletePJById: Unable to find entity of Id %s when trying to execute 'DELETE'%n",getClass().getSimpleName(), id);
            }

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private double deletePFById(String id) {
        String sql = "delete from pessoafisica pf where pf.id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, id);

            long before = System.currentTimeMillis();
            int result = statement.executeUpdate();
            long after = System.currentTimeMillis();

            if(result == 0){
                System.out.printf("%s.deletePFById: Unable to find entity of Id %s when trying to execute 'DELETE'%n",getClass().getSimpleName(), id);
            }

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
