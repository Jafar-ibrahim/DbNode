package org.example.dbnode.Service.Interfaces;

import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;

public interface UserService {
    void addUser(String username, String password) throws OperationFailedException, ResourceAlreadyExistsException;
    void deleteUser(String username) throws OperationFailedException, ResourceNotFoundException;
    void addAdmin(String username, String password) throws OperationFailedException, ResourceAlreadyExistsException;
}