package rodrigues.igor.test.result;

/**
 * Represents a SINGLE
 */
public class StrategyResult {
    private final double createResult;
    private final double selectResult;
    private final double updateResult;
    private final double deleteResult;//Only valid if calculated over 1000000 operations. Should be checked before inserting.

    public StrategyResult(double createResult, double selectResult, double updateResult, double deleteResult) {
        this.createResult = createResult;
        this.selectResult = selectResult;
        this.updateResult = updateResult;
        this.deleteResult = deleteResult;
    }

    public double getCreateResult() {
        return createResult;
    }

    public double getSelectResult() {
        return selectResult;
    }

    public double getUpdateResult() {
        return updateResult;
    }

    public double getDeleteResult() {
        return deleteResult;
    }
}
