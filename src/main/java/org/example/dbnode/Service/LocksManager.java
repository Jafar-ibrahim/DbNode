package org.example.dbnode.Service;

import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Indexing.IndexingManager;
import org.example.dbnode.Model.DatabaseRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

@Component
@Log4j2
public class LocksManager {
    private final ConcurrentHashMap<String, ReentrantLock> databaseLocks;
    private final ConcurrentHashMap<String, ReentrantLock> collectionLocks;
    private final ConcurrentHashMap<String, ReentrantLock> documentLocks;
    private final IndexingManager indexingManager;

    private LocksManager() {
        this.databaseLocks = new ConcurrentHashMap<>();
        this.collectionLocks = new ConcurrentHashMap<>();
        this.documentLocks = new ConcurrentHashMap<>();
        this.indexingManager = IndexingManager.getInstance();
    }

    private static final class InstanceHolder {
        private static final LocksManager instance = new LocksManager();
    }

    public static LocksManager getInstance() {
        return InstanceHolder.instance;
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
            if (DatabaseRegistry.getInstance().databaseExists(databaseName)) {
                ReentrantLock newLock = new ReentrantLock();
                lock = databaseLocks.putIfAbsent(databaseName, newLock);
                if (lock == null) {
                    lock = newLock;
                }
            } else {
                log.error("Error obtaining lock for database: " + databaseName);
                throw new ResourceNotFoundException("Database");
            }
        }
        return lock;
    }

    public ReentrantLock getCollectionLock(String databaseName, String collectionName) throws ResourceNotFoundException {
        try {
            String key = indexingManager.getCollectionIndexKey(databaseName, collectionName);
            return collectionLocks.computeIfAbsent(key, k -> {
                if (indexingManager.collectionIndexExists(databaseName, collectionName)) {
                    return new ReentrantLock();
                } else {
                    log.error("Error obtaining lock for collection: " + collectionName + " in database: " + databaseName);
                    throw new RuntimeException(new ResourceNotFoundException("Collection"));
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof ResourceNotFoundException) {
                throw (ResourceNotFoundException) e.getCause();
            } else {
                throw e;
            }
        }
    }
    public ReentrantLock getDocumentLock(String databaseName, String collectionName, String documentId) throws ResourceNotFoundException {
        String key = indexingManager.getDocumentIndexKey(databaseName, collectionName, documentId);
        ReentrantLock lock = documentLocks.get(key);
        if (lock == null) {
            if (indexingManager.documentExistsInCollectionIndex(databaseName, collectionName, documentId)) {
                ReentrantLock newLock = new ReentrantLock();
                lock = documentLocks.putIfAbsent(key, newLock);
                if (lock == null) {
                    lock = newLock;
                }
            } else {
                log.error("Error obtaining lock for document with id: " + documentId + " in collection: " + collectionName + " in database: " + databaseName);
                throw new ResourceNotFoundException("Document");
            }
        }
        return lock;
    }
}