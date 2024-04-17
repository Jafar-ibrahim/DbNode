package org.example.dbnode.Controller;

import org.example.dbnode.Exception.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(RedirectionException.class)
    public ResponseEntity<String> handleRedirectionException(RedirectionException e) {
        return new ResponseEntity<>(e.getMessage(), HttpStatus.valueOf(e.getErrorCode()));
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<String> handleResourceNotFoundException(ResourceNotFoundException e) {
        return new ResponseEntity<>(e.getMessage(), HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(ResourceAlreadyExistsException.class)
    public ResponseEntity<String> handleResourceAlreadyExistsException(ResourceAlreadyExistsException e) {
        return new ResponseEntity<>(e.getMessage(), HttpStatus.CONFLICT);
    }

    @ExceptionHandler(InvalidResourceNameException.class)
    public ResponseEntity<String> handleInvalidResourceNameException(InvalidResourceNameException e) {
        return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(OperationFailedException.class)
    public ResponseEntity<String> handleOperationFailedException(OperationFailedException e) {
        return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(VersionMismatchException.class)
    public ResponseEntity<String> handleVersionMismatchException(VersionMismatchException e) {
        return new ResponseEntity<>(e.getMessage(), HttpStatus.CONFLICT);
    }
    @ExceptionHandler(SchemaMismatchException.class)
    public ResponseEntity<String> handleSchemaMismatchException(SchemaMismatchException e) {
        return new ResponseEntity<>(e.getMessage(), HttpStatus.CONFLICT);
    }
    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<String> handleAccessDeniedException(AccessDeniedException e) {
        return new ResponseEntity<>("Access Denied", HttpStatus.FORBIDDEN);
    }

}
