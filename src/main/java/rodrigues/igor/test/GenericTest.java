package rodrigues.igor.test;

import org.apache.commons.lang3.tuple.Pair;
import rodrigues.igor.database.repository.TestRepository;

public interface GenericTest<T extends TestRepository> {
    public double createBatch(int n, T repository);
    public double selectLimit(int limit, int repetitions, T repository);
    public double update(int repetitions, T repository);
    public Pair<Integer, Double> delete(int repetitions, T repository);
}
