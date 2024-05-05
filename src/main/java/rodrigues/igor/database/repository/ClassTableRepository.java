package rodrigues.igor.database.repository;

import rodrigues.igor.model.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ClassTableRepository implements TestRepository{

    public static final String DB_NAME = "tcc_e6";
    private final Connection connection;

    public ClassTableRepository(Connection connection) {
        this.connection = connection;
    }


    public double create(List<Pessoa> pessoas){
        ArrayList<Pessoa> genericPessoaList = new ArrayList<>();
        ArrayList<PessoaFisica> pessoaFisicaList = new ArrayList<>();
        ArrayList<PessoaJuridica> pessoaJuridicaList = new ArrayList<>();

        //preparing lists like the E5 approach.
        for (Pessoa p : pessoas){
            genericPessoaList.add(p);
            if (p instanceof PessoaFisica){
                pessoaFisicaList.add((PessoaFisica) p);
            } else if (p instanceof PessoaJuridica) {
                pessoaJuridicaList.add((PessoaJuridica) p);
            }else {
                throw new RuntimeException("Raw Pessoa object");
            }
        }

        double pResult = 0;
        double pfResult = 0;
        double pjResult = 0;

        //creating generic objects first to avoid constraint issues
        pResult = createGeneric(genericPessoaList);

        //creating the specialized objects asynchronously
        CompletableFuture<Double> pfFuture = CompletableFuture.supplyAsync(() -> createPF(pessoaFisicaList));
        CompletableFuture<Double> pjFuture = CompletableFuture.supplyAsync(() -> createPJ(pessoaJuridicaList));

        //waiting for futures to complete
        try {
            pfResult = pfFuture.get();
            pjResult = pjFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        return pResult + pfResult + pjResult;
    }

    private double createGeneric(List<Pessoa> genericPessoaList) {
        String sql = "insert into Pessoa(id, nome) values (?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            for (Pessoa p : genericPessoaList){
                statement.setString(1, p.getId().toString());
                statement.setString(2, p.getNome());
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
    private double createPF(List<PessoaFisica> pessoaFisicaList){
        String sql = "insert into PessoaFisica(id_Pessoa, cpf) values (?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            for (PessoaFisica p : pessoaFisicaList){
                statement.setString(1, p.getId().toString());
                statement.setString(2, p.getCpf().getAsString());
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
    private double createPJ(List<PessoaJuridica> pessoaJuridicaList){
        String sql = "insert into PessoaJuridica(id_Pessoa, cnpj) values (?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            for (PessoaJuridica p : pessoaJuridicaList){
                statement.setString(1, p.getId().toString());
                statement.setString(2, p.getCnpj().getAsString());
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


    @Override
    public double selectLimit(int limit) {
        return selectPfLimit(limit);
    }


    /**
     * The hierarchy contains two specialized sets, so to retrieve all information in the hierarchy we will have to
     * choose one of the two to query.
     * We <b>could</b> join the entire hierarchy in a single select query, but that doesn't seem very useful, as we would
     * have to resolve the entity type in runtime.
     * Because of that, we will only query the <em>pessoafisica</em> table.
     * @param limit
     * @return the sql query time
     */
    public double selectPfLimit(int limit){
        String sql = "select p.id, p.nome, pf.cpf from Pessoa p join PessoaFisica pf on p.id = pf.id_Pessoa";
        if(limit>0){
            sql += " limit ?";
        }

        try (PreparedStatement statement = connection.prepareStatement(sql)){
            if(limit>0){statement.setInt(1, limit);}

            long before = System.currentTimeMillis();
            statement.execute();
            long after = System.currentTimeMillis();

            if (!statement.getResultSet().next()){
                throw new RuntimeException("Empty result set");
            }

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double updateById(Pessoa pessoa, String id){
        //we first update generic information
        double resultP = updatePessoaById(pessoa, id);
        //then update specialized information from whichever subtype the entity is.
        double nextResult;
        if (pessoa instanceof PessoaFisica){
            nextResult = updatePFById((PessoaFisica) pessoa, id);
        } else if (pessoa instanceof PessoaJuridica) {
            nextResult = updatePJById((PessoaJuridica) pessoa, id);
        }else{
            throw new RuntimeException("Pure Pessoa object");
        }
        //the result is the sum of the sql times
        return resultP + nextResult;
    }


    /**
     * Updating entity asynchronously to <b>maybe</b> save on sql time. The method itself isn't async, but it
     * completes the queries asynchronously.
     * <p>
     *      <b>Afterword:</b> This is slower. Odd considering the 'create' method is faster on async.
     * </p>
     * @param pessoa
     * @param id
     * @return
     */
    public double asyncUpdateById(Pessoa pessoa, String id){
        CompletableFuture<Double> pFuture = CompletableFuture.supplyAsync(() -> updatePessoaById(pessoa, id));
        CompletableFuture<Double> nextFuture;
        if (pessoa instanceof PessoaFisica){
            nextFuture = CompletableFuture.supplyAsync(() -> updatePFById((PessoaFisica) pessoa, id));
        } else if (pessoa instanceof PessoaJuridica) {
            nextFuture = CompletableFuture.supplyAsync(() -> updatePJById((PessoaJuridica) pessoa,id));
        }else{
            throw new RuntimeException("Pure Pessoa object");
        }

        //waiting for futures to complete;
        double pResult;
        double nextResult;
        try {
            pResult = pFuture.get();
            nextResult = nextFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return pResult + nextResult;
    }

    public double updatePessoaById(Pessoa pessoa, String id){
        String sql = "update Pessoa set nome = ? where id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, pessoa.getNome());
            statement.setString(2, id);

            long before = System.currentTimeMillis();
            statement.executeUpdate();
            long after = System.currentTimeMillis();

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double updatePFById(PessoaFisica pessoaFisica, String id){
        String sql = "update PessoaFisica set cpf = ? where id_Pessoa = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, pessoaFisica.getCpf().getAsString());
            statement.setString(2, id);

            long before = System.currentTimeMillis();
            statement.executeUpdate();
            long after = System.currentTimeMillis();
            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double updatePJById(PessoaJuridica pessoaJuridica, String id){
        String sql = "update PessoaJuridica set cnpj = ? where id_Pessoa = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, pessoaJuridica.getCnpj().getAsString());
            statement.setString(2, id);

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
        return deleteById(pessoa, pessoa.getId().toString());
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


    /**
     * Normally, this kind of method would have to query the database multiple times to delete all possible entities with
     * this ID, including all of the specialized tables. However that is wasteful, especially in a exclusive relationship
     * like the one we are dealing with.
     *
     * <p>
     *     So in order to get the best possible response time, and remain fair in our test, we will determine the subtype
     *     of the entity in code, and only query the necessary tables. That is why we are receiving a generic object here,
     *     we will use it to determine the subtype of the entity.
     * </p>
     *
     * <p>
     *     Additionally, we are not using cascade deletion.
     * </p>
     * @param id
     * @param pessoa
     * @return
     */
    public double deleteById(Pessoa pessoa, String id){
        //first delete specialized entities to avoid issues with constraints.
        double result1;
        if (pessoa instanceof PessoaFisica){
            result1 =  deletePFById(id);
        } else if (pessoa instanceof PessoaJuridica) {
            result1 = deletePJById(id);
        }else {
            throw new RuntimeException("Pure Pessoa object");
        }
        //finally, delete generic entity
        double result2 = deletePById(id);
        return result2 + result1;
    }

    private double deletePById(String id) {
        String sql = "delete from Pessoa where id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, id);

            long before = System.currentTimeMillis();
            int result = statement.executeUpdate();
            long after = System.currentTimeMillis();

            if(result == 0){
                System.out.printf("%s.deletePById: Unable to find entity of Id %s when trying to execute 'DELETE'%n",getClass().getSimpleName(), id);
            }

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private double deletePJById(String id) {
        String sql = "delete from PessoaJuridica where id_Pessoa = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, id);

            long before = System.currentTimeMillis();
            int result = statement.executeUpdate();
            long after  = System.currentTimeMillis();

            if(result == 0){
                System.out.printf("%s.deletePFById: Unable to find entity of Id %s when trying to execute 'DELETE'%n",getClass().getSimpleName(), id);
            }

            return after - before;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double deletePFById(String id){
        String sql = "delete from PessoaFisica where id_Pessoa = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setString(1, id);

            long before = System.currentTimeMillis();
            int result = statement.executeUpdate();
            long after  = System.currentTimeMillis();

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

    private List<PessoaFisica> getAllPF(int limit) {
        String sql = "select p.id, p.nome, pf.cpf from Pessoa p join PessoaFisica pf on p.id = pf.id_Pessoa";
        if(limit>0){
            sql += " limit ?";
        }

        try (PreparedStatement statement = connection.prepareStatement(sql)){
            if (limit > 0){
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
    private List<PessoaJuridica> getAllPJ(int limit){
        String sql = "select p.id, p.nome, pj.cnpj from Pessoa p join PessoaJuridica pj on p.id = pj.id_Pessoa";
        if(limit > 0){
            sql += " limit ?";
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)){
            if(limit > 0){
                statement.setInt(1, limit);
            }
            ResultSet set = statement.executeQuery();
            ArrayList<PessoaJuridica> list  = new ArrayList<>();
            while (set.next()){
                PessoaJuridica pj = new PessoaJuridica();
                pj.setNome(set.getString("nome"));
                pj.setId(UUID.fromString(set.getString("id")));
                pj.setCnpj(CNPJ.fromString(set.getString("cnpj")));
                list.add(pj);
            }
            return list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method gets up to a number of entities from the database.
     * Because in this strategy there are multiple specialized tables to fetch, we have to divide the limit to allow for
     * an even distribution of entities among the available subtypes.
     * In our case, with two subtypes, means the limit HAS to be divisible by 2. Otherwise the size of the resulting list
     * may be lower than expected.
     * @param limit
     * @return
     */
    public List<Pessoa> getAll(int limit) {
        if(limit == 1){
            return List.of(getOne());
        }
        ArrayList<Pessoa> list = new ArrayList<>();
        list.addAll(getAllPJ(limit/2));
        list.addAll(getAllPF(limit/2));
        return list;
    }

    /**
     * Gets one entity. It will randomly be either PF or PJ.
     * @return the fetched entity.
     * @throws IndexOutOfBoundsException if by chance, the database table was empty.
     */
    public Pessoa getOne(){
        Type choice = Type.getRandom();
        switch (choice){
            case PF -> {
                return getAllPF(1).get(0);
            }
            case PJ -> {
                return getAllPJ(1).get(0);
            }
            default -> throw new RuntimeException("Unexpected Type, something went wrong...");
        }
    }



    private enum Type{
        PF("PF"), PJ("PJ");

        private final String label;

        Type(String label) {
            this.label = label;
        }

        public static Type getRandom(){
            Type[] t = new Type[]{Type.PF, Type.PJ};
            return t[new Random().nextInt(t.length)];
        }

    }

}
