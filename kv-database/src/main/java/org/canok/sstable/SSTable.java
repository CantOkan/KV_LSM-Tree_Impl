package org.canok.sstable;


import org.canok.lsmtree.data.DataRecord;
import org.canok.sstable.immutable.SparseIndex;
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

            writer.flush();
            writer.close();
            BufferedReader reader = new BufferedReader(new FileReader(fileName));

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

    public List<DataRecord> getAllData() {
        List<DataRecord> allData = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                DataRecord dataRecord = getDataRecordFromLine(line);
                if (dataRecord != null) {
                    allData.add(dataRecord);
                }
            }
        } catch (IOException e) {
            log.error("Error reading file: {}", e.getMessage());
        }
        return allData;
    }

    private DataRecord getDataRecordFromLine(String line) {
        String[] parts = line.split(",");
        if (parts.length != 2) {
            log.warn("Invalid line format: {}", line);
            return null;
        }

        String[] keyValuePair = parts[0].split(":");
        if (keyValuePair.length != 2 || !keyValuePair[0].trim().equals("Key")) {
            log.warn("Invalid key-value format: {}", parts[0]);
            return null;
        }

        String keyString = keyValuePair[1].trim();
        try {
            Integer key = Integer.parseInt(keyString);
            String value = parts[1].trim();
            return new DataRecord(key, value);
        } catch (NumberFormatException e) {
            log.warn("Invalid key format: {}", keyString);
            return null;
        }
    }

    public String getFileName() {
        return fileName;
    }
}
