package org.example.dbnode.Exception;

public class CollectionDeletionFailedException extends Exception{
    public CollectionDeletionFailedException() {
        super("Failed to delete collection, associated schema, account directory entries, or index.");
    }
}
