package org.example.dbnode.Indexing.BPlusTree;// Searching on a B+ tree in Java

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different,
 * so there are two different classes for each kind of node.
 * @param <TKey> the content type of the key
 * @param <TValue> the content type of the value
 */
public class BPlusTree<TKey extends Comparable<TKey>, TValue> {
    private Node<TKey> root;

    public BPlusTree() {
        this.root = new LeafNode<TKey, TValue>();
    }

    /**
     * Insert a new key and its associated value into the B+ tree.
     */
    public void insert(TKey key, TValue value) {
        LeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
        leaf.insertKey(key, value);
        if (leaf.isNodeOverflowing()) {
            Node<TKey> n = leaf.handleOverflow();
            if (n != null)
                this.root = n;
        }
    }

    public List<Map.Entry<TKey, TValue>> getAllEntries() {
        List<Map.Entry<TKey, TValue>> entries = new ArrayList<>();
        LeafNode<TKey, TValue> current = findLeftMostLeaf();
        while (current != null) {
            for (int i = 0; i < current.getNumberOfKeys(); i++) {
                entries.add(new AbstractMap.SimpleEntry<>(current.getKeyAt(i), current.getValue(i)));
            }
            current = (LeafNode<TKey, TValue>) current.getRightSibling();
        }
        return entries;
    }
    private LeafNode<TKey, TValue> findLeftMostLeaf() {
        Node<TKey> node = this.root;
        while (!(node instanceof LeafNode)) {
            node = ((InternalNode<TKey>) node).getChild(0);
        }
        return (LeafNode<TKey, TValue>) node;
    }

    /**
     * Search a key value on the tree and return its associated value.
     */
    public TValue search(TKey key) {
        LeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
        int index = leaf.search(key);
        return (index == -1) ? null : leaf.getValue(index);
    }


    /**
     * Delete a key and its associated value from the tree.
     */
    public void delete(TKey key) {
        LeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
        if (leaf.delete(key) && leaf.isUnderflow()) {
            Node<TKey> n = leaf.handleUnderflow();
            if (n != null)
                this.root = n;
        }
    }

    public void clearTree() {
        clearTreeRecursive(root);
        root = new LeafNode<TKey, TValue>();
    }

    // Helper method to recursively clear the tree
    private void clearTreeRecursive(Node<TKey> node) {
        if (node instanceof InternalNode<TKey> internalNode) {
            for (int i = 0; i < internalNode.getNumberOfKeys() + 1; i++) {
                clearTreeRecursive(internalNode.getChild(i));
            }
        }
        node.clear(); // Implement a clear method in the Node class to remove entries
    }

    /**
     * Search the leaf node which should contain the specified key
     */
    private LeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
        Node<TKey> node = this.root;
        while (!(node instanceof LeafNode)) {
            node = ((InternalNode<TKey>) node).getChild(node.search(key));
        }
        return (LeafNode<TKey, TValue>) node;
    }
}