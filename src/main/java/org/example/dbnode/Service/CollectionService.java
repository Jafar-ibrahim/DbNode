package org.example.dbnode.Service;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.DatabaseDiskCRUD;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Model.DatabaseRegistry;
import org.example.dbnode.Model.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
@Log4j2
@Service
public class CollectionService {
    private final DatabaseDiskCRUD databaseDiskCRUD;
    private final DatabaseRegistry databaseRegistry;

    @Autowired
    public CollectionService(DatabaseDiskCRUD databaseDiskCRUD) {
        this.databaseDiskCRUD = databaseDiskCRUD;
        databaseRegistry = DatabaseRegistry.getInstance();
    }

    public Schema getCollectionSchema(String databaseName, String collectionName) throws ResourceNotFoundException {
        log.info("getting schema of collection: " + collectionName + " in database: " + databaseName);
        return databaseDiskCRUD.getCollectionSchema(databaseName, collectionName);
    }

    public void createCollection(String databaseName, String collectionName, JsonNode jsonSchema) throws ResourceAlreadyExistsException, IOException, ResourceNotFoundException {
        log.info("creating collection: " + collectionName + " in database: " + databaseName);
        databaseRegistry.addCollection(databaseName, collectionName, jsonSchema);
        databaseDiskCRUD.createCollectionFromJsonSchema(databaseName, collectionName, jsonSchema);
    }

    public void deleteCollection(String databaseName, String collectionName) throws ResourceNotFoundException, IOException, OperationFailedException {
        log.info("deleting collection: " + collectionName + " from database: " + databaseName);
        databaseDiskCRUD.deleteCollection(databaseName, collectionName);
        databaseRegistry.deleteCollection(databaseName, collectionName);
    }

    public List<String> readCollections(String databaseName) {
        log.info("reading collections in database: " + databaseName);
        return databaseDiskCRUD.readCollections(databaseName);
    }

}
