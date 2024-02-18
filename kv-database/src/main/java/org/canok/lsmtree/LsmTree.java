package org.canok.lsmtree;

import org.canok.lsmtree.data.DataRecord;
import org.canok.lsmtree.data.Operation;
import org.canok.lsmtree.immutable.WriteAheadLog;
import org.canok.lsmtree.memtable.Memtable;
import org.canok.sstable.SSTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;


public class LsmTree {

    public static final Logger log = LoggerFactory.getLogger(LsmTree.class);

    private final Memtable memtable;
    private final int memTableSize;
    private final WriteAheadLog writeAheadLog;

    public LsmTree(Integer memtableSize) {
        this.memtable = new Memtable();
        this.writeAheadLog = new WriteAheadLog();
        this.memTableSize = memtableSize;
        this.initialize();
    }

    /**
     * Initializes the LSM-Tree with writeAheadLog's commits if they are exists
     */
    public void initialize() {
        if (writeAheadLog.isExitsAndNotEmpty()) {
            List<String[]> rows = writeAheadLog.retrieveRows();

            log.info("fills tree with WriteAheadLog");

            for (String[] row : rows) {
                fillTree(row[0], row[1], Operation.valueOf(row[2]));
            }
        }

    }

    /**
     * fills Tree with writeAheadLog's commits
     *
     * @param key
     * @param value
     * @param operation previously done operations (*writeAheadLog is immutable*)
     */
    public void fillTree(String key, String value, Operation operation) {
        switch (operation) {
            case Insert:
                insert(new DataRecord(Integer.valueOf(key), value));
                break;
            case Update:
                update(new DataRecord(Integer.valueOf(key), value));
                break;
            case Delete:
                delete(Integer.valueOf(key));
                break;
        }
    }

    public void insert(DataRecord dataRecord) {
        memtable.insert(dataRecord);
        writeAheadLog.insert(dataRecord, Operation.Insert);
        log.info("Insert into memtable: " + dataRecord);
    }

    public void delete(Integer key) {
        String value = memtable.remove(key);
        writeAheadLog.insert(new DataRecord(key, value), Operation.Delete);
        log.info("Delete: {} from memtable: ", key);
    }

    public void update(DataRecord dataRecord) {
        memtable.update(dataRecord);
        writeAheadLog.insert(dataRecord, Operation.Update);
        log.info("Update: {} from memtable: ", dataRecord);
    }

    public Optional<String> getValue(Integer key) {
        String value = memtable.getValue(key);
        return value == null ? Optional.empty() : Optional.of(value);
    }

    public void flush(SSTable ssTable) {
        log.info("flushes tree and persist data into SSTable");
        String[][] insertedDatas = memtable.getAll();
        ssTable.write(insertedDatas);
        memtable.clear();
        writeAheadLog.clear();
    }


    public boolean isMemAvailable() {
        return memtable.getSize() < memTableSize;
    }

}
