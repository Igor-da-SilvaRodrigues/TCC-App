package rodrigues.igor.database.repository;

import rodrigues.igor.model.CNPJ;
import rodrigues.igor.model.CPF;
import rodrigues.igor.model.Pessoa;

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
        Type type = Type.random();

        long time;
        switch (type) {
            case PF -> time = createPF(pessoa);
            case PJ -> time = createPJ(pessoa);
            default -> throw new RuntimeException("Illegal Type.");
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
                Type type = Type.random();
                statement.setString(5, type.getLabel());
                switch (type){
                    case PF -> {
                        statement.setString(3, p.getCpf().getAsString());
                        statement.setNull(4, Types.VARCHAR);
                    }
                    case PJ ->{
                        statement.setNull(3, Types.VARCHAR);
                        statement.setString(4, p.getCnpj().getAsString());
                    }
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

    /**
     * Selects the first n entities in the database for the purpose of testing.
     * The resulting entity list is uninteresting so we will ignore them.
     * @param n The limit for the query. If less than 1, there will be no limit.>
     * @return the SQL query time.
     */
    public double selectLimit(int n) {
        String sql = "select p.id, p.nome, p.cpf, p.cnpj, p.tipo from pessoa p join tipo t on p.tipo = t.tipo";
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

            if(pessoa.getCpf() == null){
                statement.setNull(2, Types.VARCHAR);
            }else{
                statement.setString(2, pessoa.getCpf().getAsString());
            }

            if(pessoa.getCnpj() == null){
                statement.setNull(3, Types.VARCHAR);
            }else{
                statement.setString(3, pessoa.getCnpj().getAsString());
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
            statement.execute();
            long after = System.currentTimeMillis();

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @return A list containing all entities.
     * @param limit the maximum number of entities. If less than 1 there is no limit.
     */
    public List<Pessoa> _getAll(int limit){
        String sql = "select p.id, p.nome, p.cpf, p.cnpj, p.tipo from pessoa p join tipo t on p.tipo = t.tipo";
        if(limit>0){
            sql += " limit ?";
        }
        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            if(limit>0){statement.setInt(1, limit);}

            ResultSet set = statement.executeQuery();
            ArrayList<Pessoa> pessoas = new ArrayList<>();
            while (set.next()){
                Pessoa p = new Pessoa();
                p.setNome(set.getString("nome"));
                p.setId(UUID.fromString(set.getString("id")));
                p.setCnpj(CNPJ.fromString(set.getString("cnpj")));
                p.setCpf(CPF.fromString(set.getString("cpf")));

                pessoas.add(p);
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
    public String _getRandomGenericId(){
        int limit = 1000 * 1000;
        List<Pessoa> pessoas = _getAll(limit);
        Random random = new Random();
        return pessoas.get(random.nextInt(pessoas.size())).getId().toString();
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
