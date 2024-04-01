package org.example.dbnode.Indexing.BPlusTree;

class InternalNode<T extends Comparable<T>> extends Node<T>{
    protected final static int NODE_CAPACITY = 4;
    protected Object[] childrenPointers;

    public InternalNode() {
        this.nodeKeys = new Object[NODE_CAPACITY + 1];
        this.childrenPointers = new Object[NODE_CAPACITY + 2];
    }

    @SuppressWarnings("unchecked")
    public Node<T> getChild(int index) {
        return (Node<T>) this.childrenPointers[index];
    }

    public void setChild(int index, Node<T> child) {
        this.childrenPointers[index] = child;
        if (child != null)
            child.setParent(this);
    }

    @Override
    public int search(T key) {
        int index;
        for (index = 0; index < this.getNumberOfKeys(); ++index) {
            int cmp = this.getKeyAt(index).compareTo(key);
            if (cmp == 0) {
                return index + 1;
            } else if (cmp > 0) {
                return index;
            }
        }

        return index;
    }


    /* The codes below are used to support insertion operation */

    private void insertAt(int index, T key, Node<T> leftChild, Node<T> rightChild) {
        // move space for the new key
        for (int i = this.getNumberOfKeys() + 1; i > index; --i) {
            this.setChild(i, this.getChild(i - 1));
        }
        for (int i = this.getNumberOfKeys(); i > index; --i) {
            this.setKey(i, this.getKeyAt(i - 1));
        }

        // insert the new key
        this.setKey(index, key);
        this.setChild(index, leftChild);
        this.setChild(index + 1, rightChild);
        this.numberOfKeys += 1;
    }

    /**
     * When splits an internal node, the middle key is kicked out and be pushed to parent node.
     */
    @Override
    protected Node<T> split() {
        int midIndex = this.getNumberOfKeys() / 2;

        InternalNode<T> newRNode = new InternalNode<>();
        for (int i = midIndex + 1; i < this.getNumberOfKeys(); ++i) {
            newRNode.setKey(i - midIndex - 1, this.getKeyAt(i));
            this.setKey(i, null);
        }
        for (int i = midIndex + 1; i <= this.getNumberOfKeys(); ++i) {
            newRNode.setChild(i - midIndex - 1, this.getChild(i));
            newRNode.getChild(i - midIndex - 1).setParent(newRNode);
            this.setChild(i, null);
        }
        this.setKey(midIndex, null);
        newRNode.numberOfKeys = this.getNumberOfKeys() - midIndex - 1;
        this.numberOfKeys = midIndex;

        return newRNode;
    }

    @Override
    protected Node<T> promoteKey(T key, Node<T> leftChild, Node<T> rightNode) {
        // find the target position of the new key
        int index = this.search(key);

        // insert the new key
        this.insertAt(index, key, leftChild, rightNode);

        // check whether current node need to be split
        if (this.isNodeOverflowing()) {
            return this.handleOverflow();
        } else {
            return this.getParentNode() == null ? this : null;
        }
    }


    /* The codes below are used to support delete operation */

    private void deleteAt(int index) {
        int i;
        for (i = index; i < this.getNumberOfKeys() - 1; ++i) {
            this.setKey(i, this.getKeyAt(i + 1));
            this.setChild(i + 1, this.getChild(i + 2));
        }
        this.setKey(i, null);
        this.setChild(i + 1, null);
        --this.numberOfKeys;
    }


    @Override
    protected void transferKeys(Node<T> borrower, Node<T> donor, int transferIndex) {
        int borrowerChildIndex = 0;
        while (borrowerChildIndex < this.getNumberOfKeys() + 1 && this.getChild(borrowerChildIndex) != borrower)
            ++borrowerChildIndex;

        if (transferIndex == 0) {
            // borrow a key from right sibling
            T upKey = borrower.borrowFromSibling(this.getKeyAt(borrowerChildIndex), donor, transferIndex);
            this.setKey(borrowerChildIndex, upKey);
        } else {
            // borrow a key from left sibling
            T upKey = borrower.borrowFromSibling(this.getKeyAt(borrowerChildIndex - 1), donor, transferIndex);
            this.setKey(borrowerChildIndex - 1, upKey);
        }
    }

    @Override
    protected Node<T> mergeNodes(Node<T> leftChild, Node<T> rightChild) {
        int index = 0;
        while (index < this.getNumberOfKeys() && this.getChild(index) != leftChild)
            ++index;
        T middleKey = this.getKeyAt(index);

        // merge two children and the sink key into the left child node
        leftChild.mergeWithSibling(middleKey, rightChild);

        // remove the sink key, keep the left child and abandon the right child
        this.deleteAt(index);

        // check whether you need to propagate borrow or fusion to parent
        if (this.isUnderflow()) {
            if (this.getParentNode() == null) {
                // current node is root, only remove keys or delete the whole root node
                if (this.getNumberOfKeys() == 0) {
                    leftChild.setParent(null);
                    return leftChild;
                } else {
                    return null;
                }
            }
            return this.handleUnderflow();
        }
        return null;
    }


    @Override
    protected void mergeWithSibling(T middleKey, Node<T> rightSibling) {
        InternalNode<T> rightSiblingNode = (InternalNode<T>) rightSibling;

        int j = this.getNumberOfKeys();
        this.setKey(j++, middleKey);

        for (int i = 0; i < rightSiblingNode.getNumberOfKeys(); ++i) {
            this.setKey(j + i, rightSiblingNode.getKeyAt(i));
        }
        for (int i = 0; i < rightSiblingNode.getNumberOfKeys() + 1; ++i) {
            this.setChild(j + i, rightSiblingNode.getChild(i));
        }
        this.numberOfKeys += 1 + rightSiblingNode.getNumberOfKeys();

        this.setRightSibling(rightSiblingNode.rightSibling);
        if (rightSiblingNode.rightSibling != null)
            rightSiblingNode.rightSibling.setLeftSibling(this);
    }

    @Override
    protected T borrowFromSibling(T middleKey, Node<T> sibling, int transferIndex) {
        InternalNode<T> siblingNode = (InternalNode<T>) sibling;

        T upKey;
        if (transferIndex == 0) {
            // borrow the first key from right sibling, append it to tail
            int index = this.getNumberOfKeys();
            this.setKey(index, middleKey);
            this.setChild(index + 1, siblingNode.getChild(transferIndex));
            this.numberOfKeys += 1;
            upKey = siblingNode.getKeyAt(0);
            siblingNode.deleteAt(transferIndex);
        } else {
            // borrow the last key from left sibling, insert it to head
            this.insertAt(0, middleKey, siblingNode.getChild(transferIndex + 1), this.getChild(0));
            upKey = siblingNode.getKeyAt(transferIndex);
            siblingNode.deleteAt(transferIndex);
        }
        return upKey;
    }

    @Override
    public void clear() {
        for (int i = 0; i < this.getNumberOfKeys(); i++) {
            this.setKey(i, null);
            this.setChild(i, null);
        }
        // Clear the last child pointer
        this.setChild(this.getNumberOfKeys(), null);
        this.numberOfKeys = 0;
    }
}