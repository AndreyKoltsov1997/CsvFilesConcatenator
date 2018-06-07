package io.csvparser.utilities;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ParsingUtils {

    /** returns first 9 digits (key) from CSV file (KEY-VALUE) element */
    public static String getKeyFromLine(String line) {
        return line.substring(0, Constants.AMOUNT_OF_DIGITS_IN_KEY);
    }

    /** returns a string of 14 symbols (value) from CSV file (KEY-VALUE) element */
    public static String getValueFromLine (String line) {
        final int commaIndex = 1;
        return line.substring(Constants.AMOUNT_OF_DIGITS_IN_KEY + commaIndex, Constants.AMOUNT_OF_DIGITS_IN_KEY + Constants.AMOUNT_OF_CHARACTERS_IN_VALUE + commaIndex);
    }

    /** checks if we already merged some data to the key in result sorting */
    public static boolean hasAttachedData(String value) {
        return (value.length() > Constants.AMOUNT_OF_CHARACTERS_IN_VALUE);
    }


    /** converts the data from the given CSV file into a LinkedHashMap */
    public static Map<String,String> putDataIntoMap(List<String> dataFromFileA) throws IOException {
        Map<String, String> result = new LinkedHashMap();

        for (int i = 0; i < dataFromFileA.size(); ++i) {
            String currentLine = dataFromFileA.get(i);
            String currentKey = getKeyFromLine(currentLine);
            String currentValue = getValueFromLine(currentLine);
            result.put(currentKey, currentValue);
        }
        if (result.isEmpty()) {
            throw new IOException("Data hasn't been found in the first file. (fileA)");
        }
        return result;
    }

    // NOTE: Command line arguments parsing utilities

    // I wasn't sure whether it should be CML app, so I've added those just in case

    /** check the amount of arguments */
    public static void checkArguments(String[] args) {
        String misleadingMsg = " USE: ./CsvFilesConcadenator filA.csv fileB.csv";
        if (args.length != Constants.AMOUNT_OF_FILES_REQUIRED) {
            System.out.println("ERROR: the program revices " + Constants.AMOUNT_OF_FILES_REQUIRED + " parameters.");
            System.exit(Constants.RUNTIME_ERROR_CODE);
        }

    }

    /** returns file extension */
    private static final String getFileExtension(Path filePath) {
        String fileName = filePath.getFileName().toString();
        int amountOfCharactersInExtension = 3; // csv
        return fileName.substring(fileName.length() - amountOfCharactersInExtension, fileName.length());
    }

    /** check if the given file extension is correct */
    private final boolean isFileExtensionCorrect(Path filePath) {
        String fileExtension = getFileExtension(filePath);
        int extensionDifference = fileExtension.compareTo(Constants.REQUIRED_EXTERNSION);
        return extensionDifference == 0;
    }
}
