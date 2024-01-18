package rodrigues.igor.database.repository;

import rodrigues.igor.model.CPF;
import rodrigues.igor.model.Pessoa;
import rodrigues.igor.model.PessoaFisica;
import rodrigues.igor.model.PessoaJuridica;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * E5 Uma tabela é criada para cada conjunto de entidades especializado.
 * A chave primária das tabelas representará o atributo identificador da entidade genérica e da entidade especializada
 * simultaneamente.
 */
public class ConcreteTableRepository {

    public static final String DB_NAME = "tcc_e5";
    private final Connection connection;

    public ConcreteTableRepository(Connection connection) {
        this.connection = connection;
    }

    /**
     * Creates the provided entities.
     * Because this strategy consists of several tables, we can't use a single large insert operation.
     * <br>We're creating one large operation for each table and taking the sum of the query time of each operation as the total operation time.
     * <br>One problem with this approach is that it requires creating a new sublist every time we create new specialized
     * entity set.
     * <br>However, since all we care about is the performance of the operation, and not about issues regarding
     * the maintainability of the data access code, we'll go with this method to minimize time wasted due to a large number of operations
     * @return the sql query time in ms, this is the sum of the time taken by the several insert operations triggered by this method.
     */
    public double create(List<Pessoa> pessoaList){
        ArrayList<PessoaFisica> pessoaFisicaList    = new ArrayList<>();
        ArrayList<PessoaJuridica>pessoaJuridicaList = new ArrayList<>();

        for(Pessoa pessoa : pessoaList){
            if (pessoa instanceof PessoaFisica){
                pessoaFisicaList.add((PessoaFisica) pessoa);
            } else if (pessoa instanceof PessoaJuridica) {
                pessoaJuridicaList.add((PessoaJuridica) pessoa);
            }else{
                throw new RuntimeException("Pure Pessoa object");
            }
        }

        double PFBatchResult = createPF(pessoaFisicaList);
        double PJBatchResult = createPJ(pessoaJuridicaList);


        return PFBatchResult + PJBatchResult;
    }

    public double createPF(ArrayList<PessoaFisica> pessoaList){
        String sql = "insert into pessoafisica(id, nome, cpf) values (?,?,?)";
        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            for (PessoaFisica pessoa : pessoaList){
                statement.setString(1, pessoa.getId().toString());
                statement.setString(2, pessoa.getNome());
                statement.setString(3, pessoa.getCpf().getAsString());

                statement.addBatch();
            }


            long before = System.currentTimeMillis();
            statement.executeBatch();
            long after = System.currentTimeMillis();

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double createPJ(ArrayList<PessoaJuridica> pessoaList){
        String sql = "insert into pessoajuridica(id, nome, cnpj) values (?, ?, ?)";
        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            for (PessoaJuridica pessoaJuridica : pessoaList){
                statement.setString(1, pessoaJuridica.getId().toString());
                statement.setString(2, pessoaJuridica.getNome());
                statement.setString(3, pessoaJuridica.getCnpj().getAsString());

                statement.addBatch();
            }


            long before = System.currentTimeMillis();
            statement.executeBatch();
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

    public double  updatePFById(PessoaFisica pessoa, String id){
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

    public String getRandomPFId() {
        int limit = 1000*1000;
        List<PessoaFisica> pessoas = getAllPF(limit);
        Random random = new Random();
        return pessoas.get(random.nextInt(pessoas.size())).getId().toString();
    }

    public List<PessoaFisica> getAllPF(int limit) {
        String sql = "select pf.id, pf.nome, pf.cpf from pessoafisica pf";
        if(limit > 0){
            sql += " limit ?";
        }

        try (PreparedStatement statement = connection.prepareStatement(sql)){
            if(limit > 0){
                statement.setInt(1, limit);
            }

            ResultSet set = statement.executeQuery();
            ArrayList<PessoaFisica> list = new ArrayList<>();
            while (set.next()){
                PessoaFisica pf = new PessoaFisica();
                pf.setNome(set.getString("nome"));
                pf.setId(UUID.fromString(set.getString("id")));
                pf.setCpf(CPF.fromString(set.getString("cpf")));

                list.add(pf);
            }

            return list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
