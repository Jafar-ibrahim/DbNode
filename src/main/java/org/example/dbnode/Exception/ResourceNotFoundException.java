package org.example.dbnode.Exception;

public class ResourceNotFoundException extends Exception{
    public ResourceNotFoundException(String resource) {
        super(resource+" does not exist.");
    }
}
