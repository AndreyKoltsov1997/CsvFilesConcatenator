package io.csvparser;
import io.csvparser.RAM_dependend_implimentation.CSVFileJoiner;
import io.csvparser.RAM_independent_implimentation.CSVexternalFileJoiner;
import io.csvparser.utilities.Constants;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;

import io.csvparser.RAM_independent_implimentation.*;


public class CsvFilesConcatenator {

    public static void main(String[] args) throws IOException {
        List<Path> paths = Arrays.asList(Paths.get("INPUT_A.csv"), Paths.get("INPUT_B.csv"));
        try {
            /** "SIMPLE" implimentation: */

            CSVFileJoiner.perform(paths);

            /** "ADVANCED" implimentation: */
            /** I've been testing  "Advanced" implimentation on a files 3 000 000 rows in each. (~80 mb each approx.),
             * .. it took me around a minute to sort. I won't be pushing the files into Git due to the large size. */

//            CSVexternalFileJoiner.perform(paths);
            CSVexternalFileJoiner csvExternalFileJoiner = new CSVexternalFileJoiner();
            csvExternalFileJoiner.perform(paths);

        } catch (IOException error) {
            System.out.println("An error has occured while reading the data. " + error.getMessage());
            System.exit(Constants.RUNTIME_ERROR_CODE);
        }
    }
}