package org.example.dbnode.Model;

import org.example.dbnode.Exception.InvalidDatabaseNameException;
import org.example.dbnode.Exception.InvalidResourceNameException;
import org.example.dbnode.Service.FileService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseRegistry {
    private final Map<String, Database> databases;
    private static DatabaseRegistry instance;

    private DatabaseRegistry() {
        this.databases = new HashMap<>();
    }

    public static DatabaseRegistry getInstance() {
        if (instance == null)
            instance = new DatabaseRegistry();
        return instance;
    }

    public synchronized void createDatabase(String databaseName) {
        if (FileService.invalidResourceName(databaseName)) {
            throw new InvalidResourceNameException("Database");
        }
        if (databases.containsKey(databaseName)) {
            return;
        }
        Database database = new Database(databaseName);
        databases.put(databaseName, database);
    }

    public Database getOrCreateDatabase(String databaseName) {
        if (FileService.invalidResourceName(databaseName)) {
            throw new InvalidResourceNameException("Database");
        }
        return databases.computeIfAbsent(databaseName, Database::new);
    }

    public List<String> readDatabases() {
        return new ArrayList<>(databases.keySet());
    }

    /*public void deleteDatabase(String databaseName) {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new InvalidDatabaseNameException();
        }
        databases.remove(databaseName);
        IndexManager.getInstance().deleteAllIndexes(); //clear all the indexes if
    }*/
}
