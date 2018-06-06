package com.company.utilities;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ParsingUtils {
    public static String getKeyFromLine(String line) {
        return line.substring(0, Constants.amountOfDigitsInKey);
    }

    public static String getValueFromLine (String line) {
        final int commaIndex = 1;
        return line.substring(Constants.amountOfDigitsInKey + commaIndex, Constants.amountOfDigitsInKey + Constants.amountOfCharactersInValue + commaIndex);
    }

    /**
     *
     * @param value a string which represents value from given .csv file
     * @return false if no additional data has been attached
     */
    public static boolean hasAttachedData(String value) {
        System.out.println("GIVEN VALUE: " + value);
        return (value.length() > Constants.amountOfCharactersInValue);
    }

    public static Map<String,String> putDataIntoMap(List<String> dataFromFileA) throws IOException {
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
}
