package org.example.dbnode.Indexing;

import lombok.Getter;
import org.example.dbnode.Indexing.BPlusTree.BPlusTree;

import java.util.ArrayList;
import java.util.List;

@Getter
public class InvertedPropertyIndex<T extends Comparable<T>> {
    private final BPlusTree<T,List<String>> index ;

    public InvertedPropertyIndex() {
        index = new BPlusTree<>();
    }

    public void insert(T propertyValue, String documentId) {
        List<String> documentIds = index.search(propertyValue);
        if (documentIds == null) {
            documentIds = new ArrayList<>();
        }
        documentIds.add(documentId);
        index.insert(propertyValue, documentIds);
    }

    public List<String> search(T propertyValue) {
        List<String> documentIds = index.search(propertyValue);
        if (documentIds == null) {
            return new ArrayList<>();
        }
        return documentIds;
    }

    public void delete(T propertyValue) {
        index.delete(propertyValue);
    }
}