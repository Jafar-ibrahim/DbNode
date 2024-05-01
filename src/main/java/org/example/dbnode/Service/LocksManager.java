package org.example.dbnode.Service;

import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Indexing.IndexingManager;
import org.example.dbnode.DatabaseRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

@Component
@Log4j2
public class LocksManager {
    private final ConcurrentHashMap<String, ReentrantLock> databaseLocks;
    private final ConcurrentHashMap<String, ReentrantLock> collectionLocks;
    private final ConcurrentHashMap<String, ReentrantLock> documentLocks;
    private final DatabaseRegistry databaseRegistry;
    private final IndexingManager indexingManager;

    @Autowired
    public LocksManager(DatabaseRegistry databaseRegistry, IndexingManager indexingManager) {
        this.databaseLocks = new ConcurrentHashMap<>();
        this.collectionLocks = new ConcurrentHashMap<>();
        this.documentLocks = new ConcurrentHashMap<>();
        this.databaseRegistry = databaseRegistry;
        this.indexingManager = indexingManager;
    }

    public void deleteDatabaseLock(String databaseName) {
        databaseLocks.remove(databaseName);
    }

    public void deleteCollectionLock(String databaseName,String collectionName) {
        collectionLocks.remove(indexingManager.getCollectionIndexKey(databaseName,collectionName));
    }

    public void deleteDocumentLock(String databaseName,String collectionName,String documentId) {
        documentLocks.remove(indexingManager.getDocumentIndexKey(databaseName,collectionName,documentId));
    }

    public ReentrantLock getDatabaseLock(String databaseName) throws ResourceNotFoundException {
        ReentrantLock lock = databaseLocks.get(databaseName);
        if (lock == null) {
            if (databaseRegistry.databaseExists(databaseName)) {
                ReentrantLock newLock = new ReentrantLock();
                lock = databaseLocks.putIfAbsent(databaseName, newLock);
                if (lock == null) {
                    lock = newLock;
                }
            }else {
                log.error("Error obtaining lock for database: " + databaseName);
                throw new ResourceNotFoundException("Database Lock");
            }
        }
        return lock;
    }

    public ReentrantLock getCollectionLock(String databaseName, String collectionName) throws ResourceNotFoundException {
        String key = indexingManager.getCollectionIndexKey(databaseName, collectionName);
        ReentrantLock lock = collectionLocks.get(key);
        if (lock == null) {
            if (databaseRegistry.collectionExists(databaseName,collectionName)) {
                ReentrantLock newLock = new ReentrantLock();
                lock = collectionLocks.putIfAbsent(databaseName, newLock);
                if (lock == null) {
                    lock = newLock;
                }
            }else {
                log.error("Error obtaining lock for Collection: " + collectionName + " in database: " + databaseName);
                throw new ResourceNotFoundException("Collection Lock");
            }
        }
        return lock;
    }
    public ReentrantLock getDocumentLock(String databaseName, String collectionName, String documentId) {
        String key = indexingManager.getDocumentIndexKey(databaseName, collectionName, documentId);
        ReentrantLock lock = documentLocks.get(key);
        if (lock == null) {
            ReentrantLock newLock = new ReentrantLock();
            lock = documentLocks.putIfAbsent(key, newLock);
            if (lock == null) {
                lock = newLock;
            }
        }
        return lock;
    }
}