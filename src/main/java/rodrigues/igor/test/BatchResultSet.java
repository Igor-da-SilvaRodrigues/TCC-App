package rodrigues.igor.test;

public class BatchResultSet {

    /**
     * the time result of the operation involving 'nresults' entities
     */
    private long result;
    /**
     * The amount of entities involved in the operation. The value represented by 'result' was the total amount of time
     * used to process a batch of this size.
     */
    private long nresults;

    public long getResult() {
        return result;
    }

    public void setResult(long result) {
        this.result = result;
    }

    public long getNresults() {
        return nresults;
    }

    public void setNresults(long nresults) {
        this.nresults = nresults;
    }

    public double averageTime() {
        return (double) result/nresults;
    }
}

