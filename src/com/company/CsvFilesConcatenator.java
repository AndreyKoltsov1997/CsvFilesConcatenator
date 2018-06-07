package com.company;
import com.company.utilities.Constants;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;

import static com.company.utilities.ParsingUtils.*;
import com.company.RAM_dependend_implimentation.*;
import com.company.RAM_independent_implimentation.*;


public class CsvFilesConcatenator {

    public static void main(String[] args) throws IOException {
        List<Path> paths = Arrays.asList(Paths.get("/Users/Andrey/Desktop/INPUT_A.csv"), Paths.get("/Users/Andrey/Desktop/INPUT_B.csv"));
       // List<Path> paths = Arrays.asList(Paths.get("/Users/Andrey/Desktop/input_large_file_A.csv"), Paths.get("/Users/Andrey/Desktop/input_large_file_B.csv"));

        try {
            /** "SIMPLE" implimentation: */
            //CSVFileJoiner.perform(paths);
            /** "ADVANCED" implimentation: */
            CSVexternalFileJoiner.perform(paths);
        } catch (IOException error) {
            System.out.println("An error has occured while reading the data. " + error.getMessage());
            System.exit(Constants.RUNTIME_ERROR_CODE);
        }
    }
}