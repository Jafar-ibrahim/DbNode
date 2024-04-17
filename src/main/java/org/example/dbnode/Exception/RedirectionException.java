package org.example.dbnode.Exception;

public class RedirectionException extends RuntimeException {
    private final int errorCode;

    public RedirectionException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
