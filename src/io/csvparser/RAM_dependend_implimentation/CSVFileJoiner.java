package io.csvparser.RAM_dependend_implimentation;

import io.csvparser.interfaces.IFileProcesser;
import io.csvparser.utilities.Constants;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.csvparser.utilities.ParsingUtils.*;

public class CSVFileJoiner implements IFileProcesser {

    public void perform(List<Path> paths) throws IOException {
        try {
            this.printMergedDataOntoFile(paths);
        } catch (IOException error) {
            System.out.println("An error has occured while processing the files: " + error.getMessage());
            System.exit(Constants.RUNTIME_ERROR_CODE);
        }
    }
    private void printMergedDataOntoFile(List<Path> paths) throws IOException {
        Path pathForFileA = paths.get(Constants.FILE_A_INDEX);
        if (pathForFileA == null) {
            throw new IOException("File A hasn't been found");
        }
        Map<String, String> result = new LinkedHashMap(getConvertedDataFromCSVintoMap(pathForFileA));
        List dataFromFile = new ArrayList();
        Path pathForFileB = paths.get(Constants.FILE_B_INDEX);
        if (pathForFileB == null) {
            throw new IOException("File B hasn't been found");
        }
        dataFromFile.addAll(this.getDataFromFile(pathForFileB));
        Path resultFilePath = Paths.get("result.csv"); // NOTE: Creating a result file
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(resultFilePath)) {
            boolean isEndOfLineSymbolNeeded = false; // NOTE: BufferedWrited doesn't provide writing as a line, so we'd add the EOL symbol if needed
            for (int i = 0; i < dataFromFile.size(); ++i) {
                String currentString = dataFromFile.get(i).toString();
                String numberAsKey = getKeyFromLine(currentString);
                if (!result.keySet().contains(numberAsKey)) {
                    continue;
                }
                final int amountOfSeparators = 1;
                String csvStrValueWithSeparator = currentString.substring(Constants.AMOUNT_OF_DIGITS_IN_KEY, Constants.AMOUNT_OF_DIGITS_IN_KEY + Constants.AMOUNT_OF_CHARACTERS_IN_VALUE + 1);
                String valueForCurrentKey = result.get(numberAsKey);
                if (!hasAttachedData(valueForCurrentKey)) {
                    String endOfLineSymbol = isEndOfLineSymbolNeeded ? "\n" : "";
                    bufferedWriter.write(endOfLineSymbol + numberAsKey + "," + valueForCurrentKey + csvStrValueWithSeparator);
                    isEndOfLineSymbolNeeded = true;
                } else {
                    String originalValue = valueForCurrentKey.substring(Constants.AMOUNT_OF_DIGITS_IN_KEY + amountOfSeparators, Constants.AMOUNT_OF_DIGITS_IN_KEY + Constants.AMOUNT_OF_CHARACTERS_IN_VALUE + amountOfSeparators + 1);
                    bufferedWriter.write(numberAsKey + "," + originalValue + csvStrValueWithSeparator);
                }
            }
        } catch (IOException error) {
            System.out.println("An error has occured while reading into the .csv file.");
            System.exit(Constants.RUNTIME_ERROR_CODE);
        }
    }


    private List<String> getDataFromFile(Path path) throws IOException {
        List<String> dataFromFile = new ArrayList<> ();
        List<String> csvFileRawData = Files.readAllLines(path, Charset.forName("UTF-8"));
        if (csvFileRawData.isEmpty()) {
            throw new IOException("Couldn't read data from file: " + path);
        }
        return csvFileRawData;
    }

    /**
     *  @param csvFilePath a path to any .csv file
     * @throws IOException when file/data hasn't been found
     * @return mapped KEY(string represents a number, contains 9 characters) - Value (random string, contains 14 characters)
     *  */
    private Map<String, String> getConvertedDataFromCSVintoMap(Path csvFilePath) throws IOException {
        List dataFromFile = new ArrayList();
        dataFromFile.addAll(this.getDataFromFile(csvFilePath)); // get data from the first file
        Map<String, String> csvFileData = new LinkedHashMap();

        // NOTE: Reading the data from the first .csv file
        for (int i = 0; i < dataFromFile.size(); ++i) {
            String currentString = dataFromFile.get(i).toString();
            String numberAsKey = getKeyFromLine(currentString);
            final int commaIndex = 1; // NOTE: adding the comma index to get the string without it
            String value = getValueFromLine(currentString);
            csvFileData.put(numberAsKey, value);
        }

        // NOTE: Merging data from the second file and writing it into the result file (results.csv)
        if (csvFileData.isEmpty()) {
            throw new IOException("Data hasn't been found in the file " + csvFilePath);
        }
        return csvFileData;
    }

}
