package org.example.dbnode.Exception;

public class ResourceAlreadyExistsException extends Exception{
    public ResourceAlreadyExistsException(String resource) {
        super(resource+" already exists.");
    }
}
