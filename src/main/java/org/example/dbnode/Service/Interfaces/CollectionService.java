package org.example.dbnode.Service.Interfaces;

import com.fasterxml.jackson.databind.JsonNode;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Model.Schema;

import java.io.IOException;
import java.util.List;

public interface CollectionService {
    Schema getCollectionSchema(String databaseName, String collectionName) throws ResourceNotFoundException;
    void createCollection(String databaseName, String collectionName, JsonNode jsonSchema) throws ResourceAlreadyExistsException, IOException, ResourceNotFoundException, OperationFailedException;
    void deleteCollection(String databaseName, String collectionName) throws ResourceNotFoundException, IOException, OperationFailedException;
    List<String> readCollections(String databaseName);
}
