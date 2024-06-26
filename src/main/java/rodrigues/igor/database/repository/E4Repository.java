package rodrigues.igor.database.repository;

import rodrigues.igor.model.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class E4Repository implements TestRepository{

    public static final String DB_NAME = "tcc_e4";
    private final Connection connection;

    public E4Repository(Connection connection) {
        this.connection = connection;
    }

    public double create(List<Pessoa> list){
        String sql = "insert into Pessoa(id, nome, cpf, cnpj) values (?, ?, ?, ?)";
        String sqlRelationship = "insert into Pessoa_Tipo(id_Pessoa, id_Tipo) values (?, ?)";
        try (
                PreparedStatement pessoaStatement = connection.prepareStatement(sql);
                PreparedStatement tipoStatement = connection.prepareStatement(sqlRelationship);
                ){
            //filling batch insert statements
            for (Pessoa p : list){
                pessoaStatement.setString(1, p.getId().toString());
                pessoaStatement.setString(2, p.getNome());

                tipoStatement.setString(1, p.getId().toString());
                if (p instanceof PessoaFisica){
                    pessoaStatement.setString(3, ((PessoaFisica) p).getCpf().getAsString());
                    pessoaStatement.setNull(4, Types.VARCHAR);

                    tipoStatement.setString(2, Type.PF.label);
                } else if (p instanceof PessoaJuridica) {
                    pessoaStatement.setNull(3, Types.VARCHAR);
                    pessoaStatement.setString(4, ((PessoaJuridica) p).getCnpj().getAsString());

                    tipoStatement.setString(2, Type.PJ.label);
                }else {
                    throw new RuntimeException("Pure Pessoa object");
                }

                pessoaStatement.addBatch();
                tipoStatement.addBatch();
            }

            //inserting entities first to avoid key constraint issues
            long beforePessoa = System.currentTimeMillis();
            pessoaStatement.executeBatch();
            long afterPessoa = System.currentTimeMillis();

            //inserting their relationships
            long beforeTipo = System.currentTimeMillis();
            tipoStatement.executeBatch();
            long afterTipo = System.currentTimeMillis();


            return (afterTipo - beforeTipo) + (afterPessoa - beforePessoa);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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

    @Override
    public double updateById(Pessoa pessoa, String id) {
        return update(pessoa, id);
    }

    public double update(Pessoa p, String id){
        String sql = "update Pessoa set nome = ?, cpf = ?, cnpj = ? where id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, p.getNome());
            if (p instanceof PessoaFisica){
                statement.setString(2, ((PessoaFisica) p).getCpf().getAsString());
                statement.setNull(3, Types.VARCHAR);
            } else if (p instanceof PessoaJuridica) {
                statement.setNull(2, Types.VARCHAR);
                statement.setString(3, ((PessoaJuridica) p).getCnpj().getAsString());
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

    public Pessoa getOne() {
        return getAll(1).get(0);
    }

    public ArrayList<Pessoa> getAll(int limit) {
        String sql = "select p.id, p.nome, p.cpf, p.cnpj, pt.id_Tipo from Pessoa p join Pessoa_Tipo pt on p.id = pt.id_Pessoa";
        if (limit > 0){
            sql += " limit ?";
        }

        try (PreparedStatement statement = connection.prepareStatement(sql)){
            if (limit > 0) {
                statement.setInt(1, limit);
            }
            ResultSet set = statement.executeQuery();
            ArrayList<Pessoa> pessoas = new ArrayList<>();
            while (set.next()){
                String type = set.getString("id_Tipo");
                switch (type){
                    case "PF" ->{
                        PessoaFisica pf = new PessoaFisica();
                        pf.setId(UUID.fromString(set.getString("id")));
                        pf.setNome(set.getString("nome"));
                        pf.setCpf(CPF.fromString(set.getString("cpf")));
                        pessoas.add(pf);
                    }
                    case "PJ"->{
                        PessoaJuridica pj = new PessoaJuridica();
                        pj.setId(UUID.fromString(set.getString("id")));
                        pj.setNome(set.getString("nome"));
                        pj.setCnpj(CNPJ.fromString(set.getString("cnpj")));
                        pessoas.add(pj);
                    }
                    default -> throw new RuntimeException("Invalid o Null type on entity");
                }
            }

            return pessoas;
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

    public double deleteById(String id) {
        String sql = "delete from Pessoa where id = ?";
        String sqlPT = "delete from Pessoa_Tipo where id_Pessoa = ?";
        try (
                PreparedStatement statement = connection.prepareStatement(sql);
                PreparedStatement statementPT = connection.prepareStatement(sqlPT)
        ){
            statement.setString(1, id);
            statementPT.setString(1, id);

            //removing relationship first to avoid constraint issues
            long beforePT = System.currentTimeMillis();
            statementPT.executeUpdate();
            long afterPT = System.currentTimeMillis();
            double resultPT = afterPT - beforePT;


            long before = System.currentTimeMillis();
            int linesAltered = statement.executeUpdate();
            long after = System.currentTimeMillis();
            double result = after - before;

            if (linesAltered == 0){
                System.out.printf("%s: Unable to find entity of Id %s when trying to execute 'DELETE'%n",getClass().getSimpleName(), id);
            }

            return result + resultPT;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private enum Type{
        P("P"),PF("PF"),PJ("PJ");

        private final String label;

        Type(String label) {
            this.label = label;
        }
    }
}
