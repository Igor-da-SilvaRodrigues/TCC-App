package rodrigues.igor.generator;

import rodrigues.igor.model.Randomizable;

import java.util.ArrayList;

public interface Generator<T extends Randomizable> {
    /**
     * Generates a list of n random objects
     * @param n the number of objects to be generated
     * @return a list of n random objects
     */
    public ArrayList<T> generateList(int n);
}
