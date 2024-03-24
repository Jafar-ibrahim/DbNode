package org.example.dbnode.Exception;

public class InvalidResourceNameException extends IllegalArgumentException{
    public InvalidResourceNameException(String resource) {
        super(resource+" name cannot be null or empty.");
    }
}
