package org.canok.lsmtree.data;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DataRecord {
    private Integer key;
    private String value;
}
