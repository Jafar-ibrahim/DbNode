package org.example.dbnode.Service.Interfaces;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Exception.SchemaMismatchException;
import org.example.dbnode.Exception.VersionMismatchException;
import org.example.dbnode.Model.Document;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface DocumentService {
    Document createDocument(String databaseName, String collectionName, ObjectNode documentJson, Optional<String> documentIdOpt) throws OperationFailedException, IOException, ResourceNotFoundException, SchemaMismatchException;
    void deleteDocumentById(String databaseName, String collectionName, String documentId) throws OperationFailedException, ResourceNotFoundException;
    void updateDocument(String databaseName, String collectionName, String documentId, ObjectNode updatedProperties) throws OperationFailedException, ResourceNotFoundException, VersionMismatchException;
    String readDocumentProperty(String databaseName, String collectionName, String documentId, String propertyName) throws ResourceNotFoundException;
    Optional<Document> fetchDocument(String databaseName, String collectionName, String documentId);
    List<JsonNode> fetchAllDocumentsFromCollection(String databaseName, String collectionName) throws ResourceNotFoundException;
    List<JsonNode> fetchAllDocumentsByPropertyValue(String databaseName, String collectionName, String propertyName, String propertyValue) throws ResourceNotFoundException;
    List<String> fetchAllDocumentsIdsByPropertyValue(String databaseName, String collectionName, String propertyName, String propertyValue) throws ResourceNotFoundException;
    List<String> fetchAllDocumentsIdsFromCollection(String databaseName, String collectionName) throws ResourceNotFoundException;
}