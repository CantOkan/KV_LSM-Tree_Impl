package org.canok;

import org.canok.lsmtree.LsmTree;
import org.canok.lsmtree.data.DataRecord;
import org.canok.lsmtree.sstable.SSTable;


public class Main {
    private static final int MEMTABLE_SIZE = 5;
    private static final int SSTALBE_SIZE = 5;

    public static void main(String[] args) {

        LsmTree lsmTree = new LsmTree(MEMTABLE_SIZE);


        DataRecord dataRecord = new DataRecord(16, "value15");
        lsmTree.insert(dataRecord);




        lsmTree.getValue(1);


    }
}
