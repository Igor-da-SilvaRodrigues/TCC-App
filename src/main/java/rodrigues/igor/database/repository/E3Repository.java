package rodrigues.igor.database.repository;

import com.opencsv.bean.processor.PreAssignmentProcessor;
import rodrigues.igor.model.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Toda a hierarquia é mapeada em apenas uma tabela. Será permitido o uso de atributos discriminadores compostos
 * através de um relacionamento n pra n com uma tabela contendo os valores existentes. Não será permitido um valor
 * discriminador para o conjunto genérico
 */
public class E3Repository {

    public static final String DB_NAME = "tcc_e3";
    private final Connection connection;

    public E3Repository(Connection connection) {
        this.connection = connection;
    }

    /**
     * Creates all entities provided. This inserts entity data in the <em>single table</em> of the hierarchy and then inserts
     * relationship data in the N-N relationship table.
     * @param pessoas
     * @return
     */
    public double create(List<Pessoa> pessoas){
        String sqlP = "insert into Pessoa(id, nome, cpf, cnpj) values (?, ?, ?, ?)";
        ArrayList<Pessoa_Tipo> relationshipList = new ArrayList<>();
        double resultP;
        try (PreparedStatement statement = connection.prepareStatement(sqlP)){
            for (Pessoa p : pessoas){

                //we'll insert the entity and store the relationship for future insert.
                statement.setString(1, p.getId().toString());
                statement.setString(2, p.getNome());

                if(p instanceof PessoaFisica){
                    statement.setString(3, ((PessoaFisica) p).getCpf().getAsString());
                    statement.setNull(4, Types.VARCHAR);
                    relationshipList.add(new Pessoa_Tipo(p.getId().toString(), Type.PF.label));
                } else if (p instanceof PessoaJuridica) {
                    statement.setNull(3, Types.VARCHAR);
                    statement.setString(4, ((PessoaJuridica) p).getCnpj().getAsString());
                    relationshipList.add(new Pessoa_Tipo(p.getId().toString(), Type.PJ.label));
                } else {
                    throw new RuntimeException("Pure Pessoa object");
                }

                statement.addBatch();//adding another batch of insert values
            }

            long before = System.currentTimeMillis();
            statement.executeBatch();
            long after = System.currentTimeMillis();

            resultP = after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //After all entities are inserted, insert their relationships.
        double resultPT;
        String sqlPT = "insert into Pessoa_Tipo(id_Pessoa, id_Tipo) values (?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sqlPT)){
            for (Pessoa_Tipo relationship : relationshipList){
                statement.setString(1, relationship.id_Pessoa);
                statement.setString(2, relationship.id_Tipo);
                statement.addBatch();
            }

            long before = System.currentTimeMillis();
            statement.executeBatch();
            long after = System.currentTimeMillis();

            resultPT = after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //return the sum of the operation times.
        return resultPT + resultP;
    }

    public double selectLimit(int limit){
        String sql = "select p.id, p.nome, p.cpf, p.cnpj, pt.id_Tipo from Pessoa p join Pessoa_Tipo pt on p.id = pt.id_Pessoa";
        if(limit > 0){
            sql += " limit ?";
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            if(limit > 0){
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

    public double update(Pessoa pessoa, String id){
        String sql = "update Pessoa set nome = ?, cpf = ?, cnpj = ? where id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, pessoa.getNome());
            if (pessoa instanceof PessoaFisica){
                statement.setString(2, ((PessoaFisica) pessoa).getCpf().getAsString());
                statement.setNull(3, Types.VARCHAR);

            } else if (pessoa instanceof PessoaJuridica) {
                statement.setNull(2, Types.VARCHAR);
                statement.setString(3, ((PessoaJuridica) pessoa).getCnpj().getAsString());
            }else {
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

    public double delete(String id){
        String sqlPT = "delete from Pessoa_Tipo pt where pt.id_Pessoa = ?";
        String sqlP = "delete from Pessoa p where p.id = ?";
        try (
                PreparedStatement statement = connection.prepareStatement(sqlP);
                PreparedStatement relationshipStatement = connection.prepareStatement(sqlPT);
        ){
            statement.setString(1, id);
            relationshipStatement.setString(1, id);


            long beforePT = System.currentTimeMillis();
            relationshipStatement.executeUpdate();
            long afterPT = System.currentTimeMillis();
            double resultPT = afterPT - beforePT;

            long before = System.currentTimeMillis();
            int linesAltered = statement.executeUpdate();
            long after = System.currentTimeMillis();
            double resultP = after - before;

            if(linesAltered == 0){
                System.out.printf("%s: Unable to find entity of Id %s when trying to execute 'DELETE'%n",getClass().getSimpleName(), id);
            }

            return resultPT + resultP;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getRandomGenericId() {
        int limit = 1000 * 1000;
        List<Pessoa> pessoas = getAll(limit);
        Random random = new Random();
        return pessoas.get(random.nextInt(pessoas.size())).getId().toString();
    }

    public List<Pessoa> getAll(int limit){
        String sql = "select p.id, p.nome, p.cpf, p.cnpj, pt.id_Tipo from Pessoa p join Pessoa_Tipo pt on p.id = pt.id_Pessoa";
        if(limit > 0){
            sql += " limit ?";
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            if(limit > 0){
                statement.setInt(1, limit);
            }

            ResultSet set = statement.executeQuery();
            ArrayList<Pessoa> pessoaList = new ArrayList<>();
            while (set.next()){
                String type = set.getString("id_Tipo");
                switch (type) {
                    case "PF" -> {
                        PessoaFisica pf = new PessoaFisica();
                        pf.setId(UUID.fromString(set.getString("id")));
                        pf.setNome(set.getString("nome"));
                        pf.setCpf(CPF.fromString(set.getString("cpf")));
                        pessoaList.add(pf);
                    }
                    case "PJ" -> {
                        PessoaJuridica pj = new PessoaJuridica();
                        pj.setId(UUID.fromString(set.getString("id")));
                        pj.setNome(set.getString("nome"));
                        pj.setCnpj(CNPJ.fromString(set.getString("cnpj")));
                        pessoaList.add(pj);
                    }
                    default -> throw new RuntimeException("Invalid or Null type on entity");
                }
            }

            return pessoaList;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Pessoa getOne() {
        return getAll(1).get(0);
    }

    private class Pessoa_Tipo{
        private final String id_Pessoa;
        private final String id_Tipo;

        public Pessoa_Tipo(String id_Pessoa, String id_Tipo) {
            this.id_Pessoa = id_Pessoa;
            this.id_Tipo = id_Tipo;
        }
    }

    private enum Type{
        PF("PF"), PJ("PJ");

        private final String label;

        Type(String label) {
            this.label = label;
        }
    }
}
