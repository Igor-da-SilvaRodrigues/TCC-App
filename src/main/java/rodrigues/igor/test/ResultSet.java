package rodrigues.igor.test;

import java.util.ArrayList;

/**
 * Contains time information about a test
 */
public class ResultSet {

    private final ArrayList<Long> results;

    public ResultSet() {
        this.results = new ArrayList<>();
    }

    public void addResult(long result){
        this.results.add(result);
    }
    public long sumOfTimes(){
        return this.results.stream().mapToLong(Long::longValue).sum();
    }
    public int numberOfResults(){
        return this.results.size();
    }
    /**
     *
     * @return the average time (<code>totalTime()/numberOfResults()</code>)
     */
    public double averageTime(){
        return (double) sumOfTimes()/ numberOfResults();
    }

    /**
     * Less elaborate, more readable version, because there is no way I know wtf the other one returns
     * returns the maximum number of the list, or null is the list is empty.
     */
    public Long maxTime(){
        Long[] result = {null};
        this.results.forEach(aLong -> {
            if(result[0] == null || result[0] < aLong){
                result[0] = aLong;
            }
        });
        return result[0];
    }
    /**
     * Less elaborate, more readable version, because there is no way I know wtf the other one returns
     * returns the minimum number of the list, or null is the list is empty.
     */
    public Long minTime(){
        Long[] result = {null};
        this.results.forEach(aLong -> {
            if(result[0] == null || result[0] > aLong){
                result[0] = aLong;
            }
        });
        return result[0];
    }

    public boolean isEmpty(){
        return this.results.isEmpty();
    }
}
