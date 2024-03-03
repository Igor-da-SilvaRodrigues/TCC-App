package rodrigues.igor.database.repository;

import rodrigues.igor.model.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Toda a hierarquia é mapeada em apenas uma tabela. A
 * chave primária da tabela representará os atributos identificadores
 * das entidades genéricas e especializadas. Será permitido
 * o uso de um valor discriminador para o conjunto genérico
 */
public class E2Repository implements TestRepository{
    public static final String DB_NAME = "tcc_e2";
    private final Connection connection;

    public E2Repository(Connection connection) {
        this.connection = connection;
    }

    /**
     * Creates the entities in a batch operation.
     * @param pessoaList the list o entities to be created
     * @return the sql query time in ms.
     */
    public double create(List<Pessoa> pessoaList){
        String sql = "insert into Pessoa(id, nome, cpf, cnpj, tipo) values (?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            for (Pessoa p : pessoaList){
                //filling generic attributes
                statement.setString(1, p.getId().toString());
                statement.setString(2, p.getNome());
                //filling specialized attributes
                if (p instanceof PessoaFisica){
                    statement.setString(3, ((PessoaFisica) p).getCpf().getAsString());
                    statement.setNull(4, Types.VARCHAR);
                    statement.setString(5, Type.PF.label);
                } else if (p instanceof PessoaJuridica) {
                    statement.setNull(3, Types.VARCHAR);
                    statement.setString(4, ((PessoaJuridica) p).getCnpj().getAsString());
                    statement.setString(5, Type.PJ.label);
                }else {
                    throw new RuntimeException("Pure Pessoa object");
                }

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
     * Selects all entities in the database
     * @param limit the max numbers of entites to be queried. If less than 1, there is no limit.
     * @return the sql query time in ms.
     */
    public double selectLimit(int limit){
        String sql = "select p.id, p.nome, p.cpf, p.cnpj, p.tipo from Pessoa p";
        if (limit > 0){
            sql += " limit ?";
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            if (limit > 0){
                statement.setInt(1, limit);
            }

            long before = System.currentTimeMillis();
            statement.execute();
            long after = System.currentTimeMillis();
            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double updateById(Pessoa pessoa, String id){
        String sql = "update Pessoa set nome = ?, cpf = ?, cnpj = ? where id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, pessoa.getNome());
            if (pessoa instanceof PessoaFisica){
                statement.setString(2, ((PessoaFisica) pessoa).getCpf().getAsString());
                statement.setNull(3, Types.VARCHAR);
            } else if (pessoa instanceof PessoaJuridica) {
                statement.setNull(2,  Types.VARCHAR);
                statement.setString(3, ((PessoaJuridica) pessoa).getCnpj().getAsString());
            }else{
                throw new RuntimeException("Pure Pessoa object");
            }
            statement.setString(4, id);

            long before = System.currentTimeMillis();
            statement.executeUpdate();
            long after = System.currentTimeMillis();
            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double delete(Pessoa pessoa) {
        return deleteById(pessoa.getId().toString());
    }

    @Override
    public int count() {
        String sql = "select count(id) from Pessoa";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public double deleteById(String id){
        String sql = "delete from Pessoa where id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, id);

            long before = System.currentTimeMillis();
            int result = statement.executeUpdate();
            long after = System.currentTimeMillis();

            if(result == 0){
                System.out.printf("%s: Unable to find entity of Id %s when trying to execute 'DELETE'%n",getClass().getSimpleName(), id);
            }

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getRandomGenericId() {
        int limit = 1000*1000;
        List<Pessoa> pessoas = getAll(limit);
        Random random = new Random();
        return pessoas.get(random.nextInt(pessoas.size())).getId().toString();
    }

    public List<Pessoa> getAll(int limit) {
        String sql = "select p.id, p.nome, p.cpf, p.cnpj, p.tipo from Pessoa p";
        if(limit > 0){
            sql += " limit ?";
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            if (limit > 0){
                statement.setInt(1, limit);
            }

            ResultSet set = statement.executeQuery();
            ArrayList<Pessoa> pessoas = new ArrayList<>();
            while (set.next()){
                String type = set.getString("tipo");
                switch (type){
                    case "PF" -> {
                        PessoaFisica pf = new PessoaFisica();
                        pf.setNome(set.getString("nome"));
                        pf.setId(UUID.fromString(set.getString("id")));
                        pf.setCpf(CPF.fromString(set.getString("cpf")));
                        pessoas.add(pf);
                    }
                    case "PJ" -> {
                        PessoaJuridica pj = new PessoaJuridica();
                        pj.setNome(set.getString("nome"));
                        pj.setId(UUID.fromString(set.getString("id")));
                        pj.setCnpj(CNPJ.fromString(set.getString("cnpj")));
                        pessoas.add(pj);
                    }
                    case "P" -> {
                        Pessoa p = new Pessoa();
                        p.setId(UUID.fromString(set.getString("id")));
                        p.setNome(set.getString("nome"));
                        pessoas.add(p);
                    }
                    default -> throw new RuntimeException("Invalid or Null type on entity");
                }
            }

            return pessoas;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Pessoa getOne() {
        return getAll(1).get(0);
    }

    private enum Type{
        P("P"), PF("PF"), PJ("PJ");

        private final String label;

        Type(String label) {
            this.label = label;
        }

    }
}
