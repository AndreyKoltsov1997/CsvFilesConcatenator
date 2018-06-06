package com.company.external_parser;


import com.company.utilities.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class ExternalMergeSort {

    public static final int MAX_LINE_READ = 40000;
    private String fileName;
    private int compareIndex;

    public ExternalMergeSort(String fileName, int compareIndex) {
        this.fileName = fileName;
        this.compareIndex = compareIndex;
    }

    public static void start() {
        ExternalMergeSort sortLargeFiles = new ExternalMergeSort(
                "/Users/Andrey/Desktop/csvChunk/input_large_a.csv", 1);
        sortLargeFiles.performExternalMergeSort();
    }

    private String generateFileName(int index) {
        return File.separator + this.fileName.substring(0, this.fileName.length() - 4) + "_" + "chunk" + "_"
                + index;
    }

    private File generateFile(int index) {
        File file = new File(generateFileName(index));
        file.deleteOnExit();
        return file;
    }


    public void performExternalMergeSort() {

        try {
            Path targetFile = Paths.get(this.fileName);
            File file = new File(targetFile.toUri());
            //   FileReader fileReader = new FileReader(file);
            BufferedReader originalFileBufferedReader = Files.newBufferedReader(targetFile);
            int counterForLine = 0;
            int fileIndex = 0;

            List<CSVFileElement> listOfLines = new ArrayList<CSVFileElement>();

            boolean hasMoreElementsToAdd = true;
            while (hasMoreElementsToAdd) {
                String line = originalFileBufferedReader.readLine();
                if (line == null) {
                    if (!listOfLines.isEmpty()) {
                        addElementsIntoNewFile(listOfLines, fileIndex);
                        fileIndex++;
                        hasMoreElementsToAdd = false;
                    }
                    hasMoreElementsToAdd = false;
                    break;
                }


                System.out.println("PROCESSING LINE: " + line);
                if (line.equals("")) {
                    continue;
                }
                if (counterForLine >= MAX_LINE_READ) {

                    addElementsIntoNewFile(listOfLines, fileIndex);
                    fileIndex++;
                    // NOTE: Resetting everything
                    counterForLine = 0;
                    listOfLines.clear();
                }
                listOfLines.add(new CSVFileElement(line));
                counterForLine++;
            }

            originalFileBufferedReader.close();

            mergeFiles(fileIndex);

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Error: " + ex.getMessage());
        }
    }

    private void addElementsIntoNewFile(List elements, int fileIndex) throws IOException {
        Comparator<CSVFileElement> comparator = new Comparator<CSVFileElement>() {
            public int compare(CSVFileElement lhs, CSVFileElement rhs) {
                int exitFlag = 0;
                if ((lhs == null) || (rhs == null)) {
                    return -1;
                }
                if ((lhs.getKey() != null) && (rhs.getKey() != null)) {
                    final int firstKey = Integer.parseInt(lhs.getKey());
                    final int secondKey = Integer.parseInt(rhs.getKey());
                    if (firstKey > secondKey) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else {
                    return -1;
                }
            }
        };
        System.out.println("BEFORE SORTING: ");
        System.out.println(elements);
        Collections.sort(elements, comparator); // sorting the added data
        //create new file and add data into it
        //FileWriter fileWriter = new FileWriter(generateFile(fileIndex));
        System.out.println("AFTER SORTING ");
        System.out.println(elements);

        File currentFile = generateFile(fileIndex);
        BufferedWriter bufferedWriter = Files.newBufferedWriter(currentFile.toPath());
        // adding into the new file
        for (int i = 0; i < elements.size(); i++) {
            bufferedWriter.append(elements.get(i) + "\n");
        }
        bufferedWriter.close();
    }

    private void mergeFiles(int amoutOfFiles) {
        try {
            ArrayList<BufferedReader> listOfBufferedReader = new ArrayList<BufferedReader>();
            try {
                for (int index = 0; index < amoutOfFiles; index++) {
                    String fileName = generateFileName(index);
                    Path chunkFile = Paths.get(fileName);

                    listOfBufferedReader.add(Files.newBufferedReader(chunkFile));
                }
            } catch (Exception error) {
                System.out.println("Error: " + error.getMessage());
                error.printStackTrace();
            }

            sortFilesAndWriteOutput(listOfBufferedReader);

            for (int index = 0; index < listOfBufferedReader.size(); index++) {
                listOfBufferedReader.get(index).close();
                //listOfFileReader.get(index).close();
            }
        } catch (Exception ex) {

        }
    }

    private void sortFilesAndWriteOutput(List<BufferedReader> listOfBufferedReader) {
        Comparator<CSVFileElement> comparator = new Comparator<CSVFileElement>() {
            // TODO: DELETE COPY-PASTE
            public int compare(CSVFileElement lhs, CSVFileElement rhs) {
                int exitFlag = 0;
                if ((lhs == null) || (rhs == null)) {
                    return -1;
                }
                if ((lhs.getKey() != null) && (rhs.getKey() != null)) {
                    final int firstKey = Integer.parseInt(lhs.getKey());
                    final int secondKey = Integer.parseInt(rhs.getKey());
                    if (firstKey > secondKey) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else {
                    return -1;
                }
            }
        };
        try {
            List<CSVFileElement> listOfLinesfromAllFiles = new ArrayList<CSVFileElement>();
            // Read first line from each file


            File sortedFile = new File(this.fileName.substring(0, this.fileName.length() - 4) + "_sorted.csv");
            BufferedWriter sortedFileBufferWrirer = Files.newBufferedWriter(sortedFile.toPath());
            boolean hasReadEveryLine = false;
            while (!hasReadEveryLine) {
                int amountOfCompletelyReadFiles = 0;
                // iterationg through the files
                // reading first row from every file and put it in result
                for (int currentFile = 0; currentFile < listOfBufferedReader.size(); ++currentFile) {
                    String currentLineFromFile = listOfBufferedReader.get(currentFile).readLine();


                    if (currentLineFromFile == null) {
                        amountOfCompletelyReadFiles++;
                    } else {
                        CSVFileElement parsedLine = new CSVFileElement(currentLineFromFile);
                        //parsedLine.fileIndex = currentFile;
                        listOfLinesfromAllFiles.add(parsedLine);
                    }
                    if (amountOfCompletelyReadFiles == listOfBufferedReader.size()) {
                        hasReadEveryLine = true;
                    }
                }

                System.out.println("Currently having collection: " + listOfLinesfromAllFiles);
                Collections.sort(listOfLinesfromAllFiles, comparator);

                Path tmpFilePath = Paths.get(this.fileName.substring(0, this.fileName.length() - 4) + "tmp.csv");
                BufferedWriter tmpFileBufferWriter = Files.newBufferedWriter(tmpFilePath);
                //  tmpFileBufferWriter.append());
                for (int i = 0; i < listOfLinesfromAllFiles.size(); ++i) {
                    if (i < (listOfLinesfromAllFiles.size() - 1)) {
                        tmpFileBufferWriter.append(listOfLinesfromAllFiles.get(i).toString() + "\n");
                    } else {
                        tmpFileBufferWriter.append(listOfLinesfromAllFiles.get(i).toString());

                    }
                }

                tmpFileBufferWriter.close();
                if (!tmpFilePath.toFile().renameTo(sortedFile)) {
                    System.out.println("ERROR: file can't be rewritten");
                }
            }

            sortedFileBufferWrirer.flush();
            sortedFileBufferWrirer.close();

        } catch (Exception ex) {
            System.out.println("Error:" + ex.getMessage());

        }
    }
}
