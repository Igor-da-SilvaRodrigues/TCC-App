package rodrigues.igor.database.repository;

import rodrigues.igor.model.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Toda a estratégia é mapeada em apenas uma tabela.
 * A chave primária da tabela representará os atributos identificadores das entidades genéricas e especializadas.
 * Não será permitido um valor discriminador para o conjunto genérico.
 */
public class E1Repository {
    public static final String DB_NAME = "tcc_e1";

    private final Connection connection;

    public E1Repository(Connection connection) {
        this.connection = connection;
    }

    /**
     * Creates the provided entity
     *
     * @return the query time in ms
     */
    public long create (Pessoa pessoa){
        long time;
        if(pessoa instanceof PessoaJuridica){
            time = createPJ((PessoaJuridica) pessoa);
        } else if (pessoa instanceof PessoaFisica) {
            time = createPF((PessoaFisica) pessoa);
        }else{
            throw new RuntimeException("Pure pessoa object");
        }
        return time;
    }

    /**
     * Creates the provided entities in a batch CREATE operation.
     *
     * @return the query time in ms
     */
    public long create (List<Pessoa> pessoas){
        String insert = "insert into pessoa(id,nome,cpf,cnpj,tipo) values (?,?,?,?,?)";
        try(PreparedStatement statement = connection.prepareStatement(insert)){
            for (Pessoa p : pessoas){
                statement.setString(1, p.getId().toString());
                statement.setString(2, p.getNome());

                if(p instanceof PessoaFisica){
                    statement.setString(5, Type.PF.getLabel());
                    statement.setString(3, ((PessoaFisica)p).getCpf().getAsString());
                    statement.setNull(4, Types.VARCHAR);
                }else if (p instanceof PessoaJuridica){
                    statement.setString(5, Type.PJ.getLabel());
                    statement.setNull(3, Types.VARCHAR);
                    statement.setString(4, ((PessoaJuridica)p).getCnpj().getAsString());
                }else{
                    throw new RuntimeException("Pure Pessoa object");
                }
                statement.addBatch();
            }

            long before = System.currentTimeMillis();
            statement.executeBatch();
            long after = System.currentTimeMillis();
            return after-before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private long createPJ(PessoaJuridica pessoa) {
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

    private long createPF(PessoaFisica pessoa) {
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

    /**
     * Selects the first n entities in the database for the purpose of testing.
     * The resulting entity list is uninteresting so we will ignore it.
     * @param n The limit for the query. If less than 1, there will be no limit.
     * @return the SQL query time.
     */
    public double selectLimit(int n) {
        String sql = "select p.id, p.nome, p.cpf, p.cnpj, p.tipo from pessoa p";
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

    /**
     * Updates the entity identified by the provided ID with the attributes present in the provided entity object.
     * @return the sql query time.
     */
    public double updateById(Pessoa pessoa, String id){
        String sql = "update pessoa set nome = ?, cpf = ?, cnpj = ? where id = ?";
        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, pessoa.getNome());
            statement.setString(4, id);
            if ( pessoa instanceof PessoaJuridica){
                statement.setNull(2, Types.VARCHAR);
                statement.setString(3, ((PessoaJuridica) pessoa).getCnpj().getAsString());

            } else if (pessoa instanceof PessoaFisica) {
                statement.setString(2, ((PessoaFisica) pessoa).getCpf().getAsString());
                statement.setNull(3, Types.VARCHAR);
            }else{
                throw new RuntimeException("Pure Pessoa object");
            }


            long before = System.currentTimeMillis();
            statement.executeUpdate();
            long after = System.currentTimeMillis();

            return after-before;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes an entity from the db matching the given ID.
     * @return the sql query time.
     */
    public double deleteById(String id){
        String sql = "delete from pessoa p where p.id = ?";
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

    /**
     * Gets all entities in the database table up to a certain limit. Because in this strategy there is only a single
     * table mapping the hierarchy, we'll determine the entity type by the discriminator value.
     * @return A list containing all entities.
     * @param limit the maximum number of entities. If less than 1 there is no limit.
     */
    public List<Pessoa> getAll(int limit){
        String sql = "select p.id, p.nome, p.cpf, p.cnpj, p.tipo from pessoa p";
        if(limit>0){
            sql += " limit ?";
        }
        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            if(limit>0){statement.setInt(1, limit);}

            ResultSet set = statement.executeQuery();
            ArrayList<Pessoa> pessoas = new ArrayList<>();
            while (set.next()){
                String type = set.getString("tipo");
                switch (type) {
                    case "PJ" -> {
                        PessoaJuridica pj = new PessoaJuridica();
                        pj.setNome(set.getString("nome"));
                        pj.setId(UUID.fromString(set.getString("id")));
                        pj.setCnpj(CNPJ.fromString(set.getString("cnpj")));
                        pessoas.add(pj);
                    }
                    case "PF" -> {
                        PessoaFisica pf = new PessoaFisica();
                        pf.setNome(set.getString("nome"));
                        pf.setId(UUID.fromString(set.getString("id")));
                        pf.setCpf(CPF.fromString(set.getString("cpf")));
                        pessoas.add(pf);
                    }
                    default -> throw new RuntimeException("Invalid or Null type on entity");
                }
            }
            return pessoas;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * For internal use only. Returns a random UUID string from the existing generic entities, useful for operations
     * that require an already existing entity.
     * @return a random UUID string from the existing generic entities.
     */
    public String getRandomGenericId(){
        int limit = 1000 * 1000;
        List<Pessoa> pessoas = getAll(limit);
        Random random = new Random();
        return pessoas.get(random.nextInt(pessoas.size())).getId().toString();
    }

    public Pessoa getOne() {
        return getAll(1).get(0);
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
        public static Type random(){
            Type[] types = {Type.PF, Type.PJ};
            Random random = new Random();
            return types[random.nextInt(types.length)];
        }
    }

}
