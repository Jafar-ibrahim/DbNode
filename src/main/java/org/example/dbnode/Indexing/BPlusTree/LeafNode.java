package org.example.dbnode.Indexing.BPlusTree;


class LeafNode<T extends Comparable<T>, TValue> extends Node<T> {
    protected final static int LEAF_ORDER = 4;
    private final Object[] values;

    public LeafNode() {
        this.nodeKeys = new Object[LEAF_ORDER + 1];
        this.values = new Object[LEAF_ORDER + 1];
    }

    @SuppressWarnings("unchecked")
    public TValue getValue(int index) {
        return (TValue) this.values[index];
    }

    public void setValue(int index, TValue value) {
        this.values[index] = value;
    }

    @Override
    public int search(T key) {
        for (int i = 0; i < this.getNumberOfKeys(); ++i) {
            int cmp = this.getKeyAt(i).compareTo(key);
            if (cmp == 0) {
                return i;
            } else if (cmp > 0) {
                return -1;
            }
        }
        return -1;
    }


    /* The codes below are used to support insertion operation */

    public void insertKey(T key, TValue value) {
        int index = 0;
        while (index < this.getNumberOfKeys() && this.getKeyAt(index).compareTo(key) < 0)
            ++index;
        this.insertAt(index, key, value);
    }

    private void insertAt(int index, T key, TValue value) {
        // move space for the new key
        for (int i = this.getNumberOfKeys() - 1; i >= index; --i) {
            this.setKey(i + 1, this.getKeyAt(i));
            this.setValue(i + 1, this.getValue(i));
        }

        // insert new key and value
        this.setKey(index, key);
        this.setValue(index, value);
        ++this.numberOfKeys;
    }


    /**
     * When splits a leaf node, the middle key is kept on new node and be pushed to parent node.
     */
    @Override
    protected Node<T> split() {
        int midIndex = this.getNumberOfKeys() / 2;

        LeafNode<T, TValue> newRNode = new LeafNode<>();
        for (int i = midIndex; i < this.getNumberOfKeys(); ++i) {
            newRNode.setKey(i - midIndex, this.getKeyAt(i));
            newRNode.setValue(i - midIndex, this.getValue(i));
            this.setKey(i, null);
            this.setValue(i, null);
        }
        newRNode.numberOfKeys = this.getNumberOfKeys() - midIndex;
        this.numberOfKeys = midIndex;

        return newRNode;
    }

    @Override
    protected Node<T> promoteKey(T key, Node<T> leftChild, Node<T> rightNode) {
        throw new UnsupportedOperationException();
    }


    /* The codes below are used to support deletion operation */

    public boolean delete(T key) {
        int index = this.search(key);
        if (index == -1)
            return false;
        this.deleteAt(index);
        return true;
    }

    private void deleteAt(int index) {
        int i;
        for (i = index; i < this.getNumberOfKeys() - 1; ++i) {
            this.setKey(i, this.getKeyAt(i + 1));
            this.setValue(i, this.getValue(i + 1));
        }
        this.setKey(i, null);
        this.setValue(i, null);
        --this.numberOfKeys;
    }

    @Override
    protected void transferKeys(Node<T> borrower, Node<T> lender, int borrowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Node<T> mergeNodes(Node<T> leftChild, Node<T> rightChild) {
        throw new UnsupportedOperationException();
    }

    /**
     * Notice that the key sunk from parent is being abandoned.
     */
    @Override
    protected void mergeWithSibling(T sinkKey, Node<T> rightSibling) {
        LeafNode<T, TValue> siblingLeaf = (LeafNode<T, TValue>) rightSibling;
        int j = this.getNumberOfKeys();
        for (int i = 0; i < siblingLeaf.getNumberOfKeys(); ++i) {
            this.setKey(j + i, siblingLeaf.getKeyAt(i));
            this.setValue(j + i, siblingLeaf.getValue(i));
        }
        this.numberOfKeys += siblingLeaf.getNumberOfKeys();
        this.setRightSibling(siblingLeaf.rightSibling);
        if (siblingLeaf.rightSibling != null)
            siblingLeaf.rightSibling.setLeftSibling(this);
    }

    @Override
    protected T borrowFromSibling(T middleKey, Node<T> sibling, int transferIndex) {
        LeafNode<T, TValue> siblingNode = (LeafNode<T, TValue>) sibling;
        this.insertKey(siblingNode.getKeyAt(transferIndex), siblingNode.getValue(transferIndex));
        siblingNode.deleteAt(transferIndex);
        return transferIndex == 0 ? sibling.getKeyAt(0) : this.getKeyAt(0);
    }

    @Override
    public void clear() {
        // Implement logic to clear entries in the LeafNode
        for (int i = 0; i < this.getNumberOfKeys(); i++) {
            this.setKey(i, null);
            this.setValue(i, null);
        }
        this.numberOfKeys = 0;
    }
}