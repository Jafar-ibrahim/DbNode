package org.example.dbnode.Util.DataTypes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.example.dbnode.Indexing.BPlusTree.BPlusTree;
import org.example.dbnode.Indexing.InvertedPropertyIndex;

public class InvertedIndexFactory {
    private final Map<String, InvertedPropertyIndex> bTreeMap;

    private static class InstanceHolder {
        private static final InvertedIndexFactory INSTANCE = new InvertedIndexFactory();
    }

    public static InvertedIndexFactory getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public InvertedIndexFactory(){
        bTreeMap = new HashMap<>();
    }

    public InvertedPropertyIndex get(String documentDataType) {
        return switch (documentDataType) {
            case "string" -> new InvertedPropertyIndex<String>();
            case "integer" -> new InvertedPropertyIndex<Integer>();
            case "number" -> new InvertedPropertyIndex<Double>();
            case "boolean" -> new InvertedPropertyIndex<Boolean>();
            default -> throw new IllegalArgumentException("Unsupported data type: " + documentDataType);
        };
    }
}