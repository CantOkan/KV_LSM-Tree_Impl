package org.canok;

import org.canok.lsmtree.LsmTree;
import org.canok.lsmtree.data.DataRecord;


public class Main {
    private static final int MEMTABLE_SIZE = 5;
    private static final int SSTALBE_COMPACTION_SIZE = 2;

    public static void main(String[] args) {

        LsmTree lsmTree = new LsmTree(MEMTABLE_SIZE);

        DataStorage dataStorage= new DataStorage(lsmTree,SSTALBE_COMPACTION_SIZE);

        DataRecord dataRecord = new DataRecord(1, "oldValue");
        dataStorage.insert(dataRecord);


        dataRecord = new DataRecord(2, "oldValue");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(3, "oldValue");
        dataStorage.insert(dataRecord);


        dataRecord = new DataRecord(4, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(5, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(1, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(2, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(3, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(9, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(10, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(11, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(12, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(13, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(14, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(15, "valuenew");
        dataStorage.insert(dataRecord);

        dataRecord = new DataRecord(16, "valuenew");
        dataStorage.insert(dataRecord);

        dataStorage.getValue(9);
    }
}
