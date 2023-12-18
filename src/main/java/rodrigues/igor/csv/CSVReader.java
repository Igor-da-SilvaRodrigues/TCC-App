package rodrigues.igor.csv;

import com.opencsv.exceptions.CsvValidationException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class CSVReader {
    private static final String csvFile = "src/main/java/rodrigues/igor/database/data/nomes.csv";

    public ArrayList<String> getNames() throws FileNotFoundException {
        return get("nome", csvFile);
    }

    public ArrayList<String> get(String name, String path) throws FileNotFoundException {
        try(com.opencsv.CSVReader reader = new com.opencsv.CSVReader(new FileReader(path))) {
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
