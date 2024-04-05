package org.example.dbnode.Model;

import org.example.dbnode.Exception.InvalidResourceNameException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Indexing.IndexingManager;
import org.example.dbnode.Service.FileService;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DatabaseRegistry {
    private final ConcurrentHashMap<String, Database> databases;
    private final FileService fileService ;

    private DatabaseRegistry() {
        this.databases = new ConcurrentHashMap<>();
        fileService = FileService.getInstance();
        for(String database :fileService.getAllDatabases()){
            databases.put(database,new Database(database));
        }
    }

    private static final class InstanceHolder {
        private static final DatabaseRegistry instance = new DatabaseRegistry();
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

    public void deleteDatabase(String databaseName) {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new InvalidResourceNameException("Database");
        }
        databases.remove(databaseName);
        IndexingManager.getInstance().deleteAllIndexes(); //clear all the indexes if
    }
}
