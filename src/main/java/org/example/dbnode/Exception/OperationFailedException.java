package org.example.dbnode.Exception;

public class OperationFailedException extends Exception{
    public OperationFailedException(String operation) {
        super("Failed to "+operation+".");
    }
}
