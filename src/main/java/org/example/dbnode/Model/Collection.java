package org.example.dbnode.Model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Collection {
    private String name;
    private Map<String,Document> documentMap;
    private Schema schema;
}
