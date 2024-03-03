package rodrigues.igor.database.repository;

import rodrigues.igor.model.Pessoa;

import java.util.List;

public interface TestRepository {
    public double create(List<Pessoa> list);
    public double selectLimit(int limit);
    public double updateById(Pessoa pessoa, String id);
    public double delete(Pessoa pessoa);

    public int count();

    List<Pessoa> getAll(int limit);
}
