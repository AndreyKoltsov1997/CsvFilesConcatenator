package com.company.utilities;

public class CSVFileElement {

    private String key;
    private String value;

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public CSVFileElement(String line) {
        final int KEY_LENGTH = Constants.AMOUNT_OF_DIGITS_IN_KEY;
        this.key = line.substring(0, KEY_LENGTH);
        final int VALUE_LENGTH = Constants.AMOUNT_OF_CHARACTERS_IN_VALUE;
        final int commaIndex = 1;
        this.value = line.substring(KEY_LENGTH + commaIndex, line.length());
    }

    @Override
    public String toString() {
        return (this.key + "," + this.value);
    }
}

