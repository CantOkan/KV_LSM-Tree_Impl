package org.canok.lsmtree.sstable.immutable;

import org.canok.lsmtree.sstable.SSTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;

public class SparseIndex {

    public static final Logger log = LoggerFactory.getLogger(SSTable.class);

    private final String fileName;

    private static final String FILE_PATH = "sstable/";


    public SparseIndex(Long fileName) {
        this.fileName = FILE_PATH + "Index" + "_" + fileName.toString() + ".csv";
    }

    public void write(String key, int offset) {
        Path filePath = Paths.get(fileName);
        if (!Files.exists(filePath)) {
            try {
                Files.createFile(filePath);
                log.info("Created file: {}", fileName);
            } catch (IOException e) {
                log.error("Error creating file: {}", e.getMessage());
                return;
            }
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
            writer.write("Key: " + key + ", Offset: " + offset);
            writer.newLine();
            log.info("Data written to file successfully.");
        } catch (IOException e) {
            log.error("Error writing to file: {}", e.getMessage());
        }
    }

    public Optional<Integer> getOffset(String key) {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            int low = 0;
            int high = getLineCount(fileName) - 1;

            while (low <= high) {
                int mid = (low + high) / 2;
                line = readLineFromFile(fileName, mid);
                String midKey = getKey(line);

                int comparison = midKey.compareTo(key);
                if (comparison < 0) {
                    low = mid + 1;
                } else if (comparison > 0) {
                    high = mid - 1;
                } else {
                    // Key found, extract and return the offset
                    return Optional.of(getOffsetFromLine(line));
                }
            }
        } catch (IOException e) {
            log.error("Error reading file: {}", e.getMessage());
        }
        return Optional.empty();
    }

    // Helper method to extract key from a line
    private String getKey(String line) {
        return line.split(",")[0].trim().split(":")[1].trim();
    }

    // Helper method to extract offset from a line
    private int getOffsetFromLine(String line) {
        return Integer.parseInt(line.split(",")[1].trim().split(":")[1].trim());
    }

    private int getLineCount(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        long lineCount = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(path.toFile()))) {
            while (reader.readLine() != null) {
                lineCount++;
            }
        }
        return (int) lineCount;
    }


    // Helper method to read a specific line from a file
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