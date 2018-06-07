package io.csvparser.RAM_independent_implimentation;

import io.csvparser.utilities.CSVFileElement;
import io.csvparser.utilities.Constants;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/** The "advanced" implimentation, it assumes that none of the tables could fit RAM so ...
 * ... it sorts them by buckets.  */

public class CSVexternalFileJoiner {

    public static void perform(List<Path> targetFiles) throws IOException {
        Path pathForFileA = targetFiles.get(Constants.FILE_A_INDEX);
        Path pathForFileB = targetFiles.get(Constants.FILE_B_INDEX);
        int currentLineInB = 0;
        int currentLineInA = 0;
        int amountOfRowsInA = getAmountOfRowsInFile(pathForFileA);
        int amountOfRowsInB = getAmountOfRowsInFile(pathForFileB);
        Constants.BUCKET_SIZE = calculateBucketCapacity(amountOfRowsInA, amountOfRowsInB);
        Path resultFilePath = Paths.get("result.csv"); // NOTE: Creating a result file
        boolean isFileAprocessed = false; // NOTE: those variale are redutant ...
        boolean isFileBprocessed = false; //..., but the code looks more readable

        try (BufferedWriter resultBufferedWriter = Files.newBufferedWriter(resultFilePath)) {
            while (!isFileAprocessed) {
                if (currentLineInA >= amountOfRowsInA) {
                    isFileAprocessed = true;
                    break;
                }
                Map<String, String> dataFromFileA = convertFileDataOntoMap(pathForFileA, currentLineInA, amountOfRowsInA);
                isFileBprocessed = false;
                currentLineInB = 0;
                while (!isFileBprocessed) {
                    if (currentLineInB >= amountOfRowsInB) {
                        isFileBprocessed = true;
                        break;
                    }
                    List<CSVFileElement> dataFromFileB = getElementFromBucket(pathForFileB, currentLineInB, amountOfRowsInB);
                    storeMatchingValuesInFile(dataFromFileA, dataFromFileB, resultFilePath, resultBufferedWriter);
                    currentLineInB += Constants.BUCKET_SIZE;
                }
                currentLineInA += Constants.BUCKET_SIZE;

            }
        } catch (IOException error) {
            System.out.println("An error has occured while reading into the file: " + error.getMessage());
            System.exit(Constants.RUNTIME_ERROR_CODE);
        }
    }

    private static void storeMatchingValuesInFile(Map<String,String> dataFromFileA, List<CSVFileElement> dataFromFileB, Path resultFilePath, BufferedWriter bufferedWriter) throws IOException {
        dataFromFileB.forEach((currentElement) -> {
            if (dataFromFileA.containsKey(currentElement.getKey())) {
                String originalValue = dataFromFileA.get(currentElement.getKey());
                try {
                    bufferedWriter.write(currentElement.getKey() + ',' + originalValue + ',' + currentElement.getValue() + "\n");
                } catch (IOException error) {
                    System.out.println("Value *" + currentElement.toString() + " couldn't be stored. Reason: " + error.getMessage() + ". Skipping.");
                }
            }
        });
    }

    private static List<CSVFileElement> getElementFromBucket(Path targetFilePath, int startingLine, int amountOfRows) throws IOException {
        List<CSVFileElement> elementsInBucket = new ArrayList<>();
        try (BufferedReader bufferedReader = Files.newBufferedReader(targetFilePath)) {
            String currentLine = null;
            int currentLineIndex = 0;
            while ((currentLine = bufferedReader.readLine()) != null) {
                if (startingLine >= amountOfRows) {
                    currentLineIndex = startingLine - (1 + (startingLine - amountOfRows));
                    startingLine = startingLine - amountOfRows;
                    // NOTE: going until the file finished
                    CSVFileElement currentElement = new CSVFileElement(currentLine);
                    elementsInBucket.add(currentElement);
                    currentLineIndex++;
                } else if (isCursorInBounds(currentLineIndex, startingLine)) {
                    if (currentLineIndex < startingLine) {
                        currentLineIndex++;
                        continue;
                    }
                    CSVFileElement currentElement = new CSVFileElement(currentLine);
                    elementsInBucket.add(currentElement);
                    currentLineIndex++;
                }
            }
        }
        return elementsInBucket;
    }


    private static boolean isCursorInBounds(int currentLineIndex, int startingLine) {
        int difference = currentLineIndex - startingLine;
        return (Constants.BUCKET_SIZE > difference);
    }


    private static Map<String, String> convertFileDataOntoMap(Path targetFilePath, int startingLine, int amountOfRows) throws IOException {
        Map<String, String> result = new LinkedHashMap<>();
        try (BufferedReader bufferedReader = Files.newBufferedReader(targetFilePath)) {
            String currentLine = null;
            int currentLineIndex = 0;
            while ((currentLine = bufferedReader.readLine()) != null) {
                if (startingLine >= amountOfRows) {
                    currentLineIndex = startingLine - (1 + (startingLine - amountOfRows));
                    startingLine = startingLine - amountOfRows;
                    // NOTE: going until the file finished
                    CSVFileElement currentElement = new CSVFileElement(currentLine);
                    result.put(currentElement.getKey(), currentElement.getValue());
                    currentLineIndex++;
                } else if (isCursorInBounds(currentLineIndex, startingLine)) {
                    if (currentLineIndex < startingLine) {
                        currentLineIndex++;
                        continue;
                    }
                    CSVFileElement currentElement = new CSVFileElement(currentLine);
                    result.put(currentElement.getKey(), currentElement.getValue());
                    currentLineIndex++;
                }
            }
        }
        return result;
    }

    private static int getAmountOfRowsInFile(Path targetFile) {
        int result = 0;
        String curLine = null;
        try(BufferedReader bufferedReader = Files.newBufferedReader(targetFile)) {
            while ((curLine = bufferedReader.readLine()) != null) {
                result++;
            }
        } catch (IOException error) {
            System.out.println("Unable to work with the file: " + targetFile);
            System.exit(Constants.RUNTIME_ERROR_CODE);
        }
        return result;
    }


    /** Calculates the optimal bucket size for sorting */
    private static int calculateBucketCapacity(final int amountOfRowsInFileA, final int amountOfRowsInFileB) {
        final int BUCKET_CAPACITY_UPPER_BOUND = 2;
        int capacity = (int) (((amountOfRowsInFileA + amountOfRowsInFileB)/2) * 0.03);
        if (capacity < BUCKET_CAPACITY_UPPER_BOUND) {
            capacity = BUCKET_CAPACITY_UPPER_BOUND;
        }
        return capacity;
    }
}
