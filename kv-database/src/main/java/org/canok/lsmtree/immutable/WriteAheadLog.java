package org.canok.lsmtree.immutable;

import org.canok.lsmtree.data.DataRecord;
import org.canok.lsmtree.data.Operation;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class WriteAheadLog {

    static final String LSM_TREE_COMMIT_LOG = "writeAheadLog.csv";

    public void insert(DataRecord dataRecord, Operation operation) {

        String[] rowData = {dataRecord.getKey().toString(), dataRecord.getValue(), operation.name};

        try (FileWriter writer = new FileWriter(LSM_TREE_COMMIT_LOG, true)) {
            if (!isExitsAndNotEmpty()) {
                init(writer);
            }
            writer.append(String.join(",", rowData));
            writer.append("\n");

            System.out.printf("Data: %s and Opereation:%s insert into writeAheadLog \n", dataRecord.toString(), operation);
        } catch (IOException e) {
            System.err.println("Error writing to CSV file: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public boolean isExits(){
        return new File(LSM_TREE_COMMIT_LOG).exists();
    }
    public boolean isExitsAndNotEmpty() {
        File file = new File(LSM_TREE_COMMIT_LOG);
        return file.exists() && file.length() > 0;
    }

    public List<String[]> retrieveRows()  {

        List<String[]> rows = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(LSM_TREE_COMMIT_LOG))) {
            reader.readLine();

            String line;
            while ((line = reader.readLine()) != null) {
                String[] cells = line.split(",");
                rows.add(cells);
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
        }

        return rows;
    }

    public void clear() {
        try {
            String header = "";
            BufferedReader reader = new BufferedReader(new FileReader(LSM_TREE_COMMIT_LOG));
            String line;
            if ((line = reader.readLine()) != null) {
                header = line + "\n";
            }
            reader.close();

            FileWriter writer = new FileWriter(LSM_TREE_COMMIT_LOG);
            writer.write(header);
            writer.close();

            System.out.println("WriteAheadLog file cleared successfully.");
        } catch (IOException e) {
            System.err.println("Error clearing CSV file: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void init(FileWriter writer) throws IOException {
        String[] header = {
                "Key", "Value", "Operation"
        };
        writer.append(String.join(",", header));
        writer.append("\n");
    }
}
