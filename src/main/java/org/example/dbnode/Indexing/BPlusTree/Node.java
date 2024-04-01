package org.example.dbnode.Indexing.BPlusTree;


abstract class Node<T extends Comparable<T>> {
    protected Object[] nodeKeys;
    protected int numberOfKeys;
    protected Node<T> parentNode;
    protected Node<T> leftSibling;
    protected Node<T> rightSibling;


    protected Node() {
        this.numberOfKeys = 0;
        this.parentNode = null;
        this.leftSibling = null;
        this.rightSibling = null;
    }

    public int getNumberOfKeys() {
        return this.numberOfKeys;
    }

    @SuppressWarnings("unchecked")
    public T getKeyAt(int index) {
        return (T) this.nodeKeys[index];
    }

    public void setKey(int index, T key) {
        this.nodeKeys[index] = key;
    }

    public Node<T> getParentNode() {
        return this.parentNode;
    }

    public void setParent(Node<T> parentNode) {
        this.parentNode = parentNode;
    }


    /**
     * Search a key on current node, if found the key then return its position,
     * otherwise return -1 for a leaf node,
     * return the child node index which should contain the key for an internal node.
     */
    public abstract int search(T key);



    /* The codes below are used to support insertion operation */

    public boolean isNodeOverflowing() {
        return this.getNumberOfKeys() == this.nodeKeys.length;
    }

    public Node<T> handleOverflow() {
        int midIndex = this.getNumberOfKeys() / 2;
        T upKey = this.getKeyAt(midIndex);
        Node<T> newRNode = this.split();
        if (this.getParentNode() == null) {
            this.setParent(new InternalNode<>());
        }
        newRNode.setParent(this.getParentNode());
        // maintain links of sibling nodes
        newRNode.setLeftSibling(this);
        newRNode.setRightSibling(this.rightSibling);
        if (this.getRightSibling() != null)
            this.getRightSibling().setLeftSibling(newRNode);
        this.setRightSibling(newRNode);
        // push up a key to parent internal node
        return this.getParentNode().promoteKey(upKey, this, newRNode);
    }

    protected abstract Node<T> split();

    protected abstract Node<T> promoteKey(T key, Node<T> left, Node<T> right);


    /* The codes below are used to support deletion operation */

    public boolean isUnderflow() {
        return this.getNumberOfKeys() < (this.nodeKeys.length / 2);
    }

    public boolean canLendAKey() {
        return this.getNumberOfKeys() > (this.nodeKeys.length / 2);
    }

    public Node<T> getLeftSibling() {
        if (this.leftSibling != null && this.leftSibling.getParentNode() == this.getParentNode())
            return this.leftSibling;
        return null;
    }

    public void setLeftSibling(Node<T> sibling) {
        this.leftSibling = sibling;
    }

    public Node<T> getRightSibling() {
        if (this.rightSibling != null && this.rightSibling.getParentNode() == this.getParentNode())
            return this.rightSibling;
        return null;
    }

    public void setRightSibling(Node<T> sibling) {
        this.rightSibling = sibling;
    }

    public Node<T> handleUnderflow() {
        if (this.getParentNode() == null)
            return null;

        // try to borrow a key from sibling
        Node<T> leftSibling = this.getLeftSibling();
        if (leftSibling != null && leftSibling.canLendAKey()) {
            this.getParentNode().transferKeys(this, leftSibling, leftSibling.getNumberOfKeys() - 1);
            return null;
        }

        Node<T> rightSibling = this.getRightSibling();
        if (rightSibling != null && rightSibling.canLendAKey()) {
            this.getParentNode().transferKeys(this, rightSibling, 0);
            return null;
        }

        // Can not borrow a key from any sibling, then do fusion with sibling
        if (leftSibling != null) {
            return this.getParentNode().mergeNodes(leftSibling, this);
        } else {
            return this.getParentNode().mergeNodes(this, rightSibling);
        }
    }

    protected abstract void transferKeys(Node<T> borrower, Node<T> donor, int transferIndex);

    protected abstract Node<T> mergeNodes(Node<T> leftChild, Node<T> rightChild);

    protected abstract void mergeWithSibling(T middleKey, Node<T> rightSibling);

    protected abstract T borrowFromSibling(T middleKey, Node<T> sibling, int transferIndex);
    public abstract void clear();
}