package org.example.dbnode.Indexing;
import org.example.dbnode.Indexing.BPlusTree.BPlusTree;

public class CollectionIndex {
    private BPlusTree<String, Integer> bPlusTree;

    public CollectionIndex() {
        this.bPlusTree = new BPlusTree<>();
    }

    public BPlusTree<String, Integer> getBPlusTree() {
        return bPlusTree;
    }

    public void setBPlusTree(BPlusTree<String, Integer> bPlusTree) {
        this.bPlusTree = bPlusTree;
    }

    public void insert(String key, Integer value) {
        bPlusTree.insert(key, value);
    }

    public void delete(String key) {
        bPlusTree.delete(key);
    }

    public Integer search(String key) {
        return bPlusTree.search(key);
    }
    public int getSize(){
        return bPlusTree.getAllEntries().size();
    }
}