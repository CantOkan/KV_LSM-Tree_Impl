# KV_LSM-Tree_Impl
A simple key-value store implementation with LSM-Tree and SSTable 

## MEMETABLE

----
To implement Memtable, the built-in Red-Black Tree Java collection “TreeMap” is used.  
To ensure durability, Write Ahead Logs are persisted at:
```http
writeAheadLog.csv
```

## SSTable

----
SSTables are stored under the sstable folder.
Compaction is done when the given size of the limit for sstable is exceeded.


### Parameters:
Indicates the in-memory(lsm-tree) limit before flush inputs into SSTable:
```http
MEMTABLE_SIZE
```
Indicates the limit of existing sstables before compaction:
```http
SSTALBE_COMPACTION_SIZE
```
