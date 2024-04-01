package org.example.dbnode.Exception;

public class VersionMismatchException extends Exception{
    public VersionMismatchException(String operation) {
        super("Version mismatch , failed to "+operation+".");
    }
}
