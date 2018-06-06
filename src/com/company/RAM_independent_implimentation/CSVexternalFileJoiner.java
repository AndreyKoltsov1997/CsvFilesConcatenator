package com.company.RAM_independent_implimentation;

import com.company.utilities.Constants;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.company.utilities.ParsingUtils.*;


public class CSVexternalFileJoiner {

    public static void perform() throws IOException {

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
}
