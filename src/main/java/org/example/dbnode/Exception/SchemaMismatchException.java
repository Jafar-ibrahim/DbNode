package org.example.dbnode.Exception;

public class SchemaMismatchException extends Exception{
    public SchemaMismatchException() {
        super("Validation failed: Document schema does not match collection schema.");
    }
}
