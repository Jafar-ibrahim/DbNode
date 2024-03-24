package org.example.dbnode.Exception;

public class InvalidDatabaseNameException extends IllegalArgumentException{
    public InvalidDatabaseNameException() {
        super("Database name cannot be null or empty.");
    }
}
