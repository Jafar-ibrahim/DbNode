package org.example.dbnode.Model;

import org.example.dbnode.Exception.InvalidDatabaseNameException;
import org.example.dbnode.Exception.InvalidResourceNameException;
import org.example.dbnode.Service.FileService;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class DatabaseRegistry {
    private final ConcurrentHashMap<String, Database> databases;
    private static DatabaseRegistry instance;

    private DatabaseRegistry() {
        this.databases = new ConcurrentHashMap<>();
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
    public boolean databaseExists(String databaseName) {
        return databases.containsKey(databaseName);
    }

    /*public void deleteDatabase(String databaseName) {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new InvalidDatabaseNameException();
        }
        databases.remove(databaseName);
        IndexManager.getInstance().deleteAllIndexes(); //clear all the indexes if
    }*/
}
