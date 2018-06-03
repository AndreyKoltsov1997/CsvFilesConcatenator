package com.company;
import com.company.utilities.Constants;

import java.awt.*;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;

public class CsvFilesConcatenator {

    public static void main(String[] args) throws IOException {
        //List<Path> paths = Arrays.asList(Paths.get("/Users/Andrey/Desktop/INPUT_A.csv"), Paths.get("/Users/Andrey/Desktop/INPUT_B.csv"));
        List<Path> paths = Arrays.asList(Paths.get("/Users/Andrey/Desktop/input_large_file_A.csv"), Paths.get("/Users/Andrey/Desktop/input_large_file_B.csv"));

        try {
            //printMergedDataOntoFile(paths);
            advancedMethod(paths);
        } catch (IOException error) {
            System.out.println("An error has occured while reading the data. " + error.getMessage());
            System.exit(Constants.RUNTIME_ERROR_CODE);
        }
    }


    // MARK : ADVANCED METHOD
    private static void advancedMethod(List<Path> paths) throws IOException {
        Path resultFilePath = Paths.get("result.csv"); // NOTE: Creating a result file
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(resultFilePath)) {
            File fileA = new File(paths.get(0).toUri());
            File fileB = new File(paths.get(1).toUri());
            BufferedReader bufferedReaderForFileA = new BufferedReader(new FileReader(fileA));
            BufferedReader bufferedReaderForFileB = new BufferedReader(new FileReader(fileB));
            final int BUCKET_SIZE = 15; // NOTE: ammount of rows that could be read in one iteration
            int amountOfLinesRead = 0;
            String currentLineInFileA = null;
            String currentLineInFileB = null;
            List<String> dataFromFileA = new ArrayList<>();
            List<String> dataFromFileB = new ArrayList<>();
            boolean fileAhasMoreData = true;
            boolean fileBhasMoreData = true;
            while ((fileAhasMoreData || fileBhasMoreData)) {
                while (amountOfLinesRead <= BUCKET_SIZE) {
                    if ((currentLineInFileA = bufferedReaderForFileA.readLine()) != null) {
                        dataFromFileA.add(currentLineInFileA);
                    } else {
                        fileAhasMoreData = false;
                    }
                    if ((currentLineInFileB = bufferedReaderForFileB.readLine()) != null) {
                        dataFromFileB.add(currentLineInFileB);
                    } else {
                        fileBhasMoreData = false;
                    }
                    amountOfLinesRead++;
                }
                Map<String, String> result = putDataIntoMap(dataFromFileA);
                writeMergedDataOntoFile(result, dataFromFileB, bufferedWriter);
            }

        } catch (IOException error) {
            // todo: change error message
            System.out.println("An error has occured while streaming into the file: " + resultFilePath + error.getMessage());
            System.exit(Constants.RUNTIME_ERROR_CODE);
        }
    }

    private static String getKeyFromLine(String line) {
        return line.substring(0, Constants.amountOfDigitsInKey);
    }

    private static String getValueFromLine (String line) {
        final int commaIndex = 1;
        return line.substring(Constants.amountOfDigitsInKey + commaIndex, Constants.amountOfDigitsInKey + Constants.amountOfCharactersInValue + commaIndex);
    }


    // MARK: advanced method
    // todo: change the ugly name
    private static void writeMergedDataOntoFile(Map<String, String> map, List<String> list, BufferedWriter bufferedWriter) throws IOException {
        System.out.println("GIVEN MAP");
        System.out.println(map);
        boolean isEndOfLineSymbolNeeded = false; // NOTE: BufferedWrited doesn't provide writing as a line, so we'd add the EOL symbol if needed
        for (int i = 0; i < list.size(); ++i) {
            String currentString = list.get(i).toString();
            // String numberAsKey = currentString.substring(0, Constants.amountOfDigitsInKey);
            String numberAsKey = getKeyFromLine(currentString);
            if (!map.keySet().contains(numberAsKey)) {
                continue;
            }
            final int amountOfSeparators = 1;
            String csvStrValueWithSeparator = currentString.substring(Constants.amountOfDigitsInKey, Constants.amountOfDigitsInKey + Constants.amountOfCharactersInValue + 1);
            String valueForCurrentKey = map.get(numberAsKey);
            if (!hasAttachedData(valueForCurrentKey)) {
                String endOfLineSymbol = isEndOfLineSymbolNeeded ? "\n" : "";
                bufferedWriter.write(endOfLineSymbol + numberAsKey + "," + valueForCurrentKey + csvStrValueWithSeparator);
                isEndOfLineSymbolNeeded = true;
            } else {
                String originalValue = valueForCurrentKey.substring(Constants.amountOfDigitsInKey + amountOfSeparators, Constants.amountOfDigitsInKey + Constants.amountOfCharactersInValue + amountOfSeparators);
                bufferedWriter.write( numberAsKey + "," + originalValue + csvStrValueWithSeparator);
            }
        }
    }

    private static Map<String,String> putDataIntoMap(List<String> dataFromFileA) throws IOException { // MARK: AdvancedMethod
        Map<String, String> result = new LinkedHashMap();

        for (int i = 0; i < dataFromFileA.size(); ++i) {
            String currentLine = dataFromFileA.get(i);
            String currentKey = getKeyFromLine(currentLine);
            String currentValue = getValueFromLine(currentLine);
            result.put(currentKey, currentValue);
        }
        if (result.isEmpty()) {
            throw new IOException("Data hasn't been found in the first file.");
        }
        return result;
    }


    // MARK: simple method
    private static void printMergedDataOntoFile(List<Path> paths) throws IOException {
        Map<String, String> result = new LinkedHashMap(getConvertedDataFromCSVintoMap(paths.get(0)));
        List dataFromFile = new ArrayList();
        Path secondCSVfilePath = paths.get(1);
        dataFromFile.addAll(getDataFromFile(secondCSVfilePath));

        Path resultFilePath = Paths.get("result.csv"); // NOTE: Creating a result file
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(resultFilePath)) {
            boolean isEndOfLineSymbolNeeded = false; // NOTE: BufferedWrited doesn't provide writing as a line, so we'd add the EOL symbol if needed
            for (int i = 0; i < dataFromFile.size(); ++i) {
                String currentString = dataFromFile.get(i).toString();
               // String numberAsKey = currentString.substring(0, Constants.amountOfDigitsInKey);
                String numberAsKey = getKeyFromLine(currentString);
                if (!result.keySet().contains(numberAsKey)) {
                    continue;
                }
                final int amountOfSeparators = 1;
                String csvStrValueWithSeparator = currentString.substring(Constants.amountOfDigitsInKey, Constants.amountOfDigitsInKey + Constants.amountOfCharactersInValue + 1);
                String valueForCurrentKey = result.get(numberAsKey);
                if (!hasAttachedData(valueForCurrentKey)) {
                    String endOfLineSymbol = isEndOfLineSymbolNeeded ? "\n" : "";
                    bufferedWriter.write(endOfLineSymbol + numberAsKey + "," + valueForCurrentKey + csvStrValueWithSeparator);
                    isEndOfLineSymbolNeeded = true;
                } else {
                    String originalValue = valueForCurrentKey.substring(Constants.amountOfDigitsInKey + amountOfSeparators, Constants.amountOfDigitsInKey + Constants.amountOfCharactersInValue + amountOfSeparators + 1);
                    bufferedWriter.write(numberAsKey + "," + originalValue + csvStrValueWithSeparator);
                }
            }
        } catch (IOException error) {
            System.out.println("An error has occured while reading into the .csv file.");
            System.exit(Constants.RUNTIME_ERROR_CODE);
        }
    }

    /**
     *
     * @param value a string which represents value from given .csv file
     * @return false if no additional data has been attached
     */
    private static boolean hasAttachedData(String value) {
        System.out.println("GIVEN VALUE: " + value);
        return (value.length() > Constants.amountOfCharactersInValue);
    }

    private static List<String> getDataFromFile(Path path) throws IOException {
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
    private static Map<String, String> getConvertedDataFromCSVintoMap(Path csvFilePath) throws IOException {
        List dataFromFile = new ArrayList();
        dataFromFile.addAll(getDataFromFile(csvFilePath)); // get data from the first file
        Map<String, String> csvFileData = new LinkedHashMap();

        // NOTE: Reading the data from the first .csv file
        for (int i = 0; i < dataFromFile.size(); ++i) {
            String currentString = dataFromFile.get(i).toString();
            //String numberAsKey = currentString.substring(0, Constants.amountOfDigitsInKey);
            String numberAsKey = getKeyFromLine(currentString);
            final int commaIndex = 1; // NOTE: adding the comma index to get the string without it
          //  String value = currentString.substring(Constants.amountOfDigitsInKey + commaIndex, Constants.amountOfDigitsInKey + Constants.amountOfCharactersInValue + commaIndex);
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