package org.example.dbnode.Model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.DatabaseDiskCRUD;
import org.example.dbnode.Exception.InvalidResourceNameException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Indexing.IndexingManager;
import org.example.dbnode.Service.FileService;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
@Log4j2
@Component
public class DatabaseRegistry {
    private final ConcurrentHashMap<String, Database> databases;
    private final FileService fileService ;

    private DatabaseRegistry() throws ResourceNotFoundException {
        this.databases = new ConcurrentHashMap<>();
        DatabaseDiskCRUD databaseDiskCRUD = new DatabaseDiskCRUD();
        fileService = FileService.getInstance();
        for(String database :fileService.getAllDatabases()){
            databases.put(database,new Database(database));
            for(String collection: databaseDiskCRUD.readCollections(database)){
                Schema schema = databaseDiskCRUD.getCollectionSchema(database,collection);
                databases.get(database).getCollectionMap().put(collection,new Collection(collection,schema));
            }
        }
    }

    private static final class InstanceHolder {
        private static final DatabaseRegistry instance;

        static {
            try {
                instance = new DatabaseRegistry();
            } catch (ResourceNotFoundException e) {
                log.error("Error initializing DatabaseRegistry");
                throw new RuntimeException(e);
            }
        }
    }

    public static DatabaseRegistry getInstance() {
        return InstanceHolder.instance;
    }

    public synchronized void addDatabase(String databaseName) throws ResourceAlreadyExistsException {
        if (fileService.invalidResourceName(databaseName)) {
            throw new InvalidResourceNameException("Database");
        }
        if (databaseExists(databaseName)) {
            throw new ResourceAlreadyExistsException("Database");
        }
        Database database = new Database(databaseName);
        databases.put(databaseName, database);
    }

    public Database getOrCreateDatabase(String databaseName) {
        if (fileService.invalidResourceName(databaseName)) {
            throw new InvalidResourceNameException("Database");
        }
        return databases.computeIfAbsent(databaseName, Database::new);
    }

    public List<String> readDatabases() {
        return new ArrayList<>(databases.keySet());
    }
    public boolean databaseExists(String databaseName) {
        return databases.containsKey(databaseName);
    }

    public void addCollection(String databaseName, String collectionName, JsonNode jsonSchema) throws ResourceNotFoundException, IOException {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new InvalidResourceNameException("Database");
        }
        if (collectionName == null || collectionName.trim().isEmpty()) {
            throw new InvalidResourceNameException("Collection");
        }
        if (jsonSchema == null) {
            throw new IllegalArgumentException("Schema");
        }
        if (!databaseExists(databaseName)) {
            throw new ResourceNotFoundException("Database");
        }
        Schema schema = Schema.of(String.valueOf(jsonSchema));
        databases.get(databaseName).getCollectionMap().put(collectionName, new Collection(collectionName, schema));
    }
    public void deleteCollection(String databaseName, String collectionName) {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new InvalidResourceNameException("Database");
        }
        if (collectionName == null || collectionName.trim().isEmpty()) {
            throw new InvalidResourceNameException("Collection");
        }
        if (databaseExists(databaseName)) {
            databases.get(databaseName).getCollectionMap().remove(collectionName);
        }
    }
    public boolean collectionExists(String databaseName, String collectionName) {
        return databaseExists(databaseName) && databases.get(databaseName).getCollectionMap().containsKey(collectionName);
    }

    public void deleteDatabase(String databaseName) {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new InvalidResourceNameException("Database");
        }
        databases.remove(databaseName);
        IndexingManager.getInstance().deleteAllIndexes(); //clear all the indexes if
    }
}
