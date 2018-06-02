package com.company;
import com.company.utilities.Constants;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class DataConcadenator {

    public static void main(String[] args) throws IOException {
        List<Path> paths = Arrays.asList(Paths.get("/Users/Andrey/Desktop/INPUT_A.csv"), Paths.get("/Users/Andrey/Desktop/INPUT_B.csv"));
        printMergedDataOntoFile(paths);
        Path target = Paths.get("/Users/Andrey/Desktop/RESULT.csv");
       // Files.write(target, mergedLines, Charset.forName("UTF-8"));
    }


    private static void printMergedDataOntoFile(List<Path> paths) throws IOException {
        List dataFromFile = new ArrayList();
        dataFromFile.addAll(getDataFromFile(paths.get(0))); // get data from the first file
        Map<String, String> result = new LinkedHashMap();
        if (!dataFromFile.isEmpty()) {
            for (int i = 0; i < dataFromFile.size(); ++i) {
                String currentString = dataFromFile.get(i).toString();
                String numberAsKey = currentString.substring(0, Constants.amountOfDigitsInKey);
                // NOTE: "+1" is due to the comma separator
                final int amountOfSeparators = 1;
                // TODO: remove separators in a different way maybe?
                String value = currentString.substring(Constants.amountOfDigitsInKey + amountOfSeparators, Constants.amountOfDigitsInKey + Constants.amountOfCharactersInValue + amountOfSeparators);
                result.put(numberAsKey, value);
            }
        }

        if (!result.isEmpty()) {
            dataFromFile.clear();
            dataFromFile.addAll(getDataFromFile(paths.get(1))); // add data from the second file
            if (!dataFromFile.isEmpty()) {
                for (int i = 0; i < dataFromFile.size(); ++i) {
                    String currentString = dataFromFile.get(i).toString();
                    String numberAsKey = currentString.substring(0, Constants.amountOfDigitsInKey);
                    if (!result.keySet().contains(numberAsKey)) {
                        continue;
                    }
                    final int amountOfSeparators = 1;
                    String valueWithSeparator = currentString.substring(Constants.amountOfDigitsInKey, Constants.amountOfDigitsInKey + Constants.amountOfCharactersInValue);
                    String previousValue = result.get(numberAsKey);
                    if (previousValue.length() == Constants.amountOfCharactersInValue) {
                        System.out.println(numberAsKey + "," + previousValue + valueWithSeparator);
                    } else {
                        String originalValue = previousValue.substring(Constants.amountOfDigitsInKey + amountOfSeparators, Constants.amountOfDigitsInKey + Constants.amountOfCharactersInValue + amountOfSeparators);
                        System.out.println(numberAsKey + "," + originalValue + valueWithSeparator);
                    }
                }
            }
        }
    }


    private static List<String> getDataFromFile(Path path) throws IOException {
        List<String> mergedLines = new ArrayList<> ();
        List<String> lines = Files.readAllLines(path, Charset.forName("UTF-8"));
        if (!lines.isEmpty()) {
            if (mergedLines.isEmpty()) {
                mergedLines.add(lines.get(0)); //add header only once
            }
            mergedLines.addAll(lines.subList(1, lines.size()));
        }
        return mergedLines;
    }

}