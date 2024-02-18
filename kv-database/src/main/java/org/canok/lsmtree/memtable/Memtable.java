package org.canok.lsmtree.memtable;

import lombok.Data;
import org.canok.lsmtree.data.DataRecord;

import java.util.Map;
import java.util.TreeMap;

public class Memtable {
    private final Map<Integer, String> balancedTree;

    public Memtable() {
        balancedTree = new TreeMap<>();
    }

    public void insert(DataRecord dataRecord) {
        balancedTree.put(dataRecord.getKey(), dataRecord.getValue());
    }

    public String remove(Integer key) {
        return balancedTree.remove(key);
    }

    public String getValue(Integer key) {
        return balancedTree.get(key);
    }

    public long getSize() {
        return balancedTree.size();
    }

    public void clear() {
        balancedTree.clear();
    }

    public void update(DataRecord dataRecord) {
        balancedTree.replace(dataRecord.getKey(), dataRecord.getValue());
    }

    public String[][] getAll() {
        String[][] stringArray = new String[balancedTree.size()][2];
        int index = 0;
        for (Map.Entry<Integer, String> entry : balancedTree.entrySet()) {
            stringArray[index][0] = String.valueOf(entry.getKey());
            stringArray[index][1] = entry.getValue();
            index++;
        }
        return stringArray;
    }


}
