package org.example.dbnode.Exception;

public class ResourseNotExistsException extends Exception{
    public ResourseNotExistsException(String resource) {
        super(resource+" does not exist.");
    }
}
