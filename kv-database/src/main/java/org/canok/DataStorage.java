package org.canok;

import org.canok.lsmtree.LsmTree;
import org.canok.lsmtree.data.DataRecord;
import org.canok.sstable.SSTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DataStorage {

    public static final Logger log = LoggerFactory.getLogger(DataStorage.class);

    private SSTable ssTable;
    private LsmTree lsmTree;
    private List<SSTable> ssTableList;
    private final int ssTableCompactionSize;

    public DataStorage(LsmTree lsmTree, int ssTableCompactionSize) {
        this.lsmTree = lsmTree;
        this.ssTableList = new ArrayList<>();
        this.ssTableCompactionSize = ssTableCompactionSize;
    }

    public Optional<String> getValue(Integer key) {
        Optional<String> result = lsmTree.getValue(key);
        if (!result.isPresent()) {
            return findValueFromSStables(key);
        }
        return result;
    }

    public Optional<String> findValueFromSStables(Integer key) {
        ssTableList.sort(Comparator.comparing(SSTable::getFileName).reversed());
        for (SSTable ssTable : ssTableList) {
            Optional<String> ssTableValue = ssTable.getValue(key);
            if (ssTableValue.isPresent()) {
                return ssTableValue;
            }
        }
        return Optional.empty();
    }

    public void generateSSTable() {
        long currentTimestamp = System.currentTimeMillis();
        this.ssTable = new SSTable(currentTimestamp);
        checkCompaction();
        ssTableList.add(ssTable);
    }

    public void insert(DataRecord dataRecord) {
        if (!lsmTree.isMemAvailable()) {
            generateSSTable();
            lsmTree.flush(ssTable);
        }
        lsmTree.insert(dataRecord);
    }

    public void delete(Integer key) {
        lsmTree.delete(key);
    }
    public void update(DataRecord dataRecord){
        lsmTree.update(dataRecord);
    }

    /**
     * Performs compaction on created SSTables and merges them into compactionSSTable
     */
    public void performCompaction() {
        log.info("Performs Compaction on SSTables");

        ssTableList.sort(Comparator.comparing(SSTable::getFileName));
        Map<Integer, String> mergedSSTable = new HashMap<>();

        for (SSTable table : ssTableList) {
            List<DataRecord> records = table.getAllData();
            for (DataRecord dataRecord: records){
                mergedSSTable.put(dataRecord.getKey(),dataRecord.getValue());
            }
        }
        createCompactionSSTable(mergedSSTable);
    }

    public void createCompactionSSTable(Map<Integer, String> mergedSSTable){
        long currentTimestamp = System.currentTimeMillis();
        SSTable compactedSSTable = new SSTable(currentTimestamp);

        String[][] datas = new String[mergedSSTable.size()][2];
        int index = 0;
        for (Map.Entry<Integer, String> entry : mergedSSTable.entrySet()) {
            datas[index][0] = String.valueOf(entry.getKey());
            datas[index][1] = entry.getValue();
            index++;
        }
        compactedSSTable.write(datas);
        ssTableList.clear();;
        ssTableList.add(compactedSSTable);
    }

    public void checkCompaction() {
        if (ssTableList.size() >= ssTableCompactionSize) {
            performCompaction();
        }
    }


}
