package org.example.dbnode.Indexing;

import org.example.dbnode.Indexing.BPlusTree.BPlusTree;

public class PropertyIndex {
    private BPlusTree<String, String> bPlusTree;

    public PropertyIndex() {
        this.bPlusTree = new BPlusTree<>();
    }

    public BPlusTree<String, String> getBPlusTree() {
        return bPlusTree;
    }

    public void setBPlusTree(BPlusTree<String, String> bPlusTree) {
        this.bPlusTree = bPlusTree;
    }

    public void insert(String propertyValue, String index) {
        bPlusTree.insert(propertyValue, index);
    }

    public void delete(String propertyValue) {
        bPlusTree.delete(propertyValue);
    }

    public String search(String propertyValue) {
        return bPlusTree.search(propertyValue);
    }
}