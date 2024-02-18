package org.canok.lsmtree.data;

public enum Operation {

    Insert("Insert"),
    Delete("Delete"),
    Update("Update"),
    Read("Read");

    public String name;

    Operation(String operation) {
        this.name=operation;
    }
}
