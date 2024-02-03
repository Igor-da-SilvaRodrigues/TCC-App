package rodrigues.igor.csv;

import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.util.ArrayList;
import java.util.Objects;

public class CSVReader {
    private static final String csvFile = "/data/nomes.csv";

    /**
     * Returns the list of names contained in the CSV file.
     * @throws FileNotFoundException
     */
    public ArrayList<String> getNames() throws FileNotFoundException {
        return get(csvFile);
    }

    public ArrayList<String> get(String path) {
        try(com.opencsv.CSVReader reader = new com.opencsv.CSVReader(new InputStreamReader(Objects.requireNonNull(CSVReader.class.getResourceAsStream(path))))) {
            String[] nextLine;
            ArrayList<String> result = new ArrayList<>();
            while ((nextLine = reader.readNext()) != null){
                for (String cell : nextLine){
                    result.add(nextLine[0]);
                }
            }

            return result;
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }
}
