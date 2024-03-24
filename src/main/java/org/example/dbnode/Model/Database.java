package org.example.dbnode.Model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Database {
    private String name;
    private Map<String,Collection> collectionMap;

    public Database(String name) {
        this.name = name;
        this.collectionMap = new HashMap<>();
    }
}
