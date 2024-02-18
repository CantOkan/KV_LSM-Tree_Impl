package org.canok.lsmtree.sstable;


import org.canok.lsmtree.sstable.immutable.SparseIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public class SSTable {

    public static final Logger log = LoggerFactory.getLogger(SSTable.class);

    private final String fileName;

    private final SparseIndex sparseIndex;

    private static final String FILE_PATH = "sstable/";

    public SSTable(Long fileName) {
        this.fileName = FILE_PATH + fileName.toString() + ".csv";
        this.sparseIndex = new SparseIndex(fileName);
    }

    public void write(String[][] datas) {
        Path filePath = Paths.get(fileName);
        if (!Files.exists(filePath)) {
            try {
                Files.createFile(filePath);
                log.info("creates file with:{}", fileName);
            } catch (IOException e) {
                log.error("Error creating file: {}", e.getMessage());
                return;
            }
        }
        int offset = 0;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
            for (String[] data : datas) {
                writer.write("Key: " + data[0] + ", Value: " + data[1]);
                writer.newLine();
                appendSparseIndex(data[0], offset);
                offset++;
            }
            log.info("Data written to file successfully.");
        } catch (IOException e) {
            log.error("Error writing to file: {}", e.getMessage());
        }
    }

    public void appendSparseIndex(String key, int offset) {
        sparseIndex.write(key, offset);
    }

    public Optional<String> getValue(Integer key) {
        Optional<Integer> offset = sparseIndex.getOffset(String.valueOf(key));
        if (offset.isPresent()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line = readLineFromFile(fileName, offset.get());
                return Optional.of(getValueFromLine(line));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
    }

    private String getValueFromLine(String line) {
        return line.split(",")[1].trim().split(":")[1].trim(); // Assuming the format is "Key: <key>, Value: <value>"
    }

    private String readLineFromFile(String fileName, int lineNumber) throws IOException {
        Path path = Paths.get(fileName);
        try (BufferedReader reader = new BufferedReader(new FileReader(path.toFile()))) {
            for (int i = 0; i < lineNumber; i++) {
                reader.readLine();
            }
            return reader.readLine();
        }
    }

}
