package org.example.dbnode.Exception;

public class DatabaseAlreadyExistsException extends Exception{
    public DatabaseAlreadyExistsException() {
        super("Database already exists.");
    }
}
