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
    private static LocksManager instance;

    private LocksManager() {
        this.databaseLocks = new ConcurrentHashMap<>();
        this.collectionLocks = new ConcurrentHashMap<>();
        this.documentLocks = new ConcurrentHashMap<>();
        this.indexingManager = IndexingManager.getInstance();
    }

    public static LocksManager getInstance() {
        if (instance == null)
            instance = new LocksManager();
        return instance;
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

    public ReentrantLock createDatabaseLockThenLock(String databaseName) {
        ReentrantLock lock = new ReentrantLock();
        databaseLocks.put(databaseName, lock);
        lock.lock();
        return lock;
    }

    public ReentrantLock createCollectionLockThenLock(String databaseName, String collectionName) {
        ReentrantLock lock = new ReentrantLock();
        collectionLocks.put(indexingManager.getCollectionIndexKey(databaseName,collectionName), lock);
        lock.lock();
        return lock;
    }

    public ReentrantLock createDocumentLockThenLock(String databaseName, String collectionName, String documentId) {
        ReentrantLock lock = new ReentrantLock();
        documentLocks.put(indexingManager.getDocumentIndexKey(databaseName,collectionName,documentId), lock);
        lock.lock();
        return lock;
    }

    public ReentrantLock getDatabaseLock(String databaseName) throws ResourceNotFoundException {
        ReentrantLock lock = databaseLocks.get(databaseName);
        if (lock == null) {
            if(DatabaseRegistry.getInstance().databaseExists(databaseName))
                databaseLocks.put(databaseName, new ReentrantLock());
            else {
                log.error("Error obtaining lock for database: "+databaseName);
                throw new ResourceNotFoundException("Database lock");
            }
        }
        return lock;
    }

    public ReentrantLock getCollectionLock(String databaseName,String collectionName) throws ResourceNotFoundException {
        ReentrantLock lock = collectionLocks.get(collectionName);
        if (lock == null) {
            if(indexingManager.collectionIndexExists(databaseName,collectionName))
                collectionLocks.put(indexingManager.getCollectionIndexKey(databaseName,collectionName), new ReentrantLock());
            else{
                log.error("Error obtaining lock for collection: "+collectionName+" in database: "+databaseName);
                throw new ResourceNotFoundException("Collection lock");
            }
        }
        return lock;
    }

    public ReentrantLock getDocumentLock(String databaseName,String collectionName,String documentId) throws ResourceNotFoundException {
        ReentrantLock lock = documentLocks.get(documentId);
        if (lock == null) {
            if (indexingManager.documentExistsInCollectionIndex(databaseName,collectionName,documentId))
                documentLocks.put(indexingManager.getDocumentIndexKey(databaseName,collectionName,documentId), new ReentrantLock());
            else{
                log.error("Error obtaining lock for document with id: "+documentId+" in collection: "+collectionName+" in database: "+databaseName);
                throw new ResourceNotFoundException("Document lock");
            }
        }
        return lock;
    }

}
