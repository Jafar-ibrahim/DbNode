package org.example.dbnode.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.DatabaseDiskCRUD;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Exception.SchemaMismatchException;
import org.example.dbnode.Exception.VersionMismatchException;
import org.example.dbnode.Model.Document;
import org.example.dbnode.Model.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@Log4j2
public class DocumentService {
    private final DatabaseDiskCRUD databaseDiskCRUD;
    private final CollectionService collectionService;

    @Autowired
    public DocumentService(DatabaseDiskCRUD databaseDiskCRUD, CollectionService collectionService) {
        this.databaseDiskCRUD = databaseDiskCRUD;
        this.collectionService = collectionService;
    }

    public Document createDocument(String databaseName, String collectionName, ObjectNode documentJson,Optional<String> documentIdOpt) throws OperationFailedException, IOException, ResourceNotFoundException, SchemaMismatchException {
        log.info("adding document to collection: " + collectionName + " in database: " + databaseName);
        Document document = new Document(documentJson);
        Schema schema = collectionService.getCollectionSchema(databaseName, collectionName);
        if (!schema.validateDocument(documentJson)) {
            throw new SchemaMismatchException();
        }
        String documentIdString = documentIdOpt.orElse(UUID.randomUUID().toString());
        // This method contains Document Indexing implicitly
        return databaseDiskCRUD.createDocument(databaseName, collectionName, document,documentIdString);
    }

    public void deleteDocument(String databaseName, String collectionName, String documentId) throws OperationFailedException, ResourceNotFoundException {
        log.info("deleting document: " + documentId + " from collection: " + collectionName + " in database: " + databaseName);
        databaseDiskCRUD.deleteDocumentFromCollection(databaseName, collectionName, documentId);
    }

    public void updateDocument(String databaseName, String collectionName, String documentId,ObjectNode updatedProperties) throws OperationFailedException, ResourceNotFoundException, VersionMismatchException {
        //String newValueCasted = DataTypeCaster.getInstance().castToDataType(newValue.toString(), databaseName, collectionName, propertyName).toString();
        log.info("updating document: " + documentId + " in collection: " + collectionName + " in database: " + databaseName);
        databaseDiskCRUD.updateDocument(databaseName, collectionName, documentId, updatedProperties);
    }

    public String readDocumentProperty(String databaseName, String collectionName, String documentId, String propertyName) throws ResourceNotFoundException {
        log.info("reading " + propertyName + " property from document: " + documentId + " in collection: " + collectionName + " in database: " + databaseName);
        return databaseDiskCRUD.readDocumentProperty(databaseName, collectionName, documentId, propertyName);
    }

    public Optional<Document> fetchDocument(String databaseName, String collectionName, String documentId){
        log.info("fetching document: " + documentId + " from collection: " + collectionName + " in database: " + databaseName);
        return databaseDiskCRUD.fetchDocumentFromDatabase(databaseName, collectionName, documentId);
    }

    public List<JsonNode> fetchAllDocumentsFromCollection(String databaseName, String collectionName) throws ResourceNotFoundException {
        log.info("fetching all documents from collection: " + collectionName + " in database: " + databaseName);
        return databaseDiskCRUD.fetchAllDocumentsFromCollection(databaseName, collectionName);
    }
}
