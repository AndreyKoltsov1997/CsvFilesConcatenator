package com.company.RAM_independent_implimentation;

import com.company.utilities.CSVFileElement;
import com.company.utilities.Constants;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.company.utilities.ParsingUtils.*;


public class CSVexternalFileJoiner {

    public static void perform(List<Path> targetFiles) throws IOException {
        Path fileApath = targetFiles.get(Constants.FILE_A_INDEX);
        Path fileBpath = targetFiles.get(Constants.FILE_B_INDEX);

        boolean fileAfullyProcessed = false;
        int currentLineInB = 0;
        int currentLineInA = 0;
        Map<String, String> dataFromFileA = convertFileDataOntoMap(fileApath, currentLineInA);
        List<CSVFileElement> dataFromFileB = getElementFromBucket(fileBpath, currentLineInB);
        Path resultFilePath = Paths.get("result.csv"); // NOTE: Creating a result file
        try (BufferedWriter resultBufferedWriter = Files.newBufferedWriter(resultFilePath)) {
            while (!isFileFullyProcessed(dataFromFileA.size())) {
              //  int currentLineInB = ;
                boolean fileBfullyProcessed = false;
                while (!isFileFullyProcessed(dataFromFileB.size())) {
                    storeMatchingValuesInFile(dataFromFileA, dataFromFileB, resultFilePath, resultBufferedWriter);
                    dataFromFileB = getElementFromBucket(fileBpath, currentLineInB);
                    currentLineInB += Constants.BUCKET_SIZE;
                }
                currentLineInA += Constants.BUCKET_SIZE;
                dataFromFileA = convertFileDataOntoMap(fileApath, currentLineInA);
            }
        } catch (IOException error) {
            System.out.println("An error has occurd while reading into the file: " + error.getMessage());
            System.exit(Constants.RUNTIME_ERROR_CODE);
        }
    }
    private static boolean isFileFullyProcessed(int containerSize) {
        return (containerSize < Constants.BUCKET_SIZE);
    }
    private static void storeMatchingValuesInFile(Map<String,String> dataFromFileA, List<CSVFileElement> dataFromFileB, Path resultFilePath, BufferedWriter bufferedWriter) throws IOException {
            dataFromFileB.forEach((currentElement) -> {
                if (dataFromFileA.containsKey(currentElement.getKey())) {
                    CSVFileElement matchingElement = new CSVFileElement(currentElement.toString());
                    try {
                        bufferedWriter.write(matchingElement.toString());
                    } catch (IOException error) {
                        System.out.println("Value *" + matchingElement.toString() + " couldn't be stored. Reason: " + error.getMessage() + ". Skipping.");
                    }
                }
            });

    }

    private static List<CSVFileElement> getElementFromBucket(Path targetFilePath, int startingLine) throws IOException {
        // todo: add illegal argument exception for parameters
        List<CSVFileElement> elementsInBucket = new ArrayList<>();
        try (BufferedReader bufferedReader = Files.newBufferedReader(targetFilePath)) {
            String currentLine = null;
            int currentLineIndex = 0;
            while (((currentLine = bufferedReader.readLine()) != null) && isCursorInBounds(currentLineIndex)) {
                CSVFileElement currentElement = new CSVFileElement(currentLine);
                currentLineIndex++;
            }
        }
        return elementsInBucket;
    }


    private static boolean isCursorInBounds(int currentLineIndex) {
        int nextLineIndex = currentLineIndex + 1; // we're checking further so the next line won't overflow the bucket
        return (Constants.BUCKET_SIZE > nextLineIndex);
    }

    private static Map<String, String> convertFileDataOntoMap(Path targetFilePath, int startingLine) throws IOException {
        Map<String, String> result = new LinkedHashMap<>();
        try (BufferedReader bufferedReader = Files.newBufferedReader(targetFilePath)) {
            String currentLine = null;
            int currentLineIndex = 0;
            while (((currentLine = bufferedReader.readLine()) != null) && isCursorInBounds(currentLineIndex)) {
                CSVFileElement currentElement = new CSVFileElement(currentLine);
                result.put(currentElement.getKey(), currentElement.getValue());
                currentLineIndex++;
            }
        }
        return result;
    }


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
            // TODO: add separate functions for both of the files
            // ... FILE A should be in a linked-hash-map
            // ... FILE B - we'd store only values to we ould ocmpare it easily
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



    // todo: change the ugly name
    private static void writeMergedDataOntoFile(Map<String, String> map, List<String> list, BufferedWriter bufferedWriter) throws IOException {
        System.out.println("GIVEN MAP");
        System.out.println(map);
        boolean isEndOfLineSymbolNeeded = false; // NOTE: BufferedWrited doesn't provide writing as a line, so we'd add the EOL symbol if needed
        for (int i = 0; i < list.size(); ++i) {
            String currentString = list.get(i).toString();
            // String numberAsKey = currentString.substring(0, Constants.AMOUNT_OF_DIGITS_IN_KEY);
            String numberAsKey = getKeyFromLine(currentString);
            if (!map.keySet().contains(numberAsKey)) {
                continue;
            }
            final int amountOfSeparators = 1;
            String csvStrValueWithSeparator = currentString.substring(Constants.AMOUNT_OF_DIGITS_IN_KEY, Constants.AMOUNT_OF_DIGITS_IN_KEY + Constants.AMOUNT_OF_CHARACTERS_IN_VALUE + 1);
            String valueForCurrentKey = map.get(numberAsKey);
            if (!hasAttachedData(valueForCurrentKey)) {
                String endOfLineSymbol = isEndOfLineSymbolNeeded ? "\n" : "";
                bufferedWriter.write(endOfLineSymbol + numberAsKey + "," + valueForCurrentKey + csvStrValueWithSeparator);
                isEndOfLineSymbolNeeded = true;
            } else {
                String originalValue = valueForCurrentKey.substring(Constants.AMOUNT_OF_DIGITS_IN_KEY + amountOfSeparators, Constants.AMOUNT_OF_DIGITS_IN_KEY + Constants.AMOUNT_OF_CHARACTERS_IN_VALUE + amountOfSeparators);
                bufferedWriter.write( numberAsKey + "," + originalValue + csvStrValueWithSeparator);
            }
        }
    }
}
