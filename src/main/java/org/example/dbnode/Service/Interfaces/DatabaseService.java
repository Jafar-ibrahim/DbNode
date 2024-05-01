package org.example.dbnode.Service.Interfaces;

import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;

import java.io.IOException;
import java.util.List;

public interface DatabaseService {
    void createDatabase(String databaseName) throws ResourceAlreadyExistsException, ResourceNotFoundException;
    void deleteDatabase(String databaseName) throws IOException, ResourceNotFoundException;
    List<String> getAllDatabases();
    void deleteAllDatabases() throws IOException, ResourceNotFoundException;
}