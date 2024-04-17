package org.example.dbnode.Exception;

public class VersionMismatchException extends Exception{
    public VersionMismatchException() {
        super("Version mismatch , failed to update document property.");
    }
}
