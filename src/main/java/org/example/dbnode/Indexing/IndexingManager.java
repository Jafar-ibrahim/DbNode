package org.example.dbnode.Indexing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.DatabaseDiskCRUD;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Model.DatabaseRegistry;
import org.example.dbnode.Model.Schema;
import org.example.dbnode.Service.FileService;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
@Log4j2
public class IndexingManager {

    private final Map<String, CollectionIndex> collectionsIndexMap = new ConcurrentHashMap<>();
    private final Map<String, PropertyIndex> propertyIndexMap = new ConcurrentHashMap<>();
    private final FileService fileService;

    private IndexingManager() {
        this.fileService = FileService.getInstance();
        DatabaseRegistry databaseRegistry = DatabaseRegistry.getInstance();
        List<String> allDatabases = databaseRegistry.readDatabases();
        for (String dbName : allDatabases) {
            loadAllIndexes(dbName);
        }
    }
    private static final class InstanceHolder {
        private static final IndexingManager instance = new IndexingManager();
    }
    public static IndexingManager getInstance() {
        return InstanceHolder.instance;
    }
    public void createCollectionIndex(String databaseName, String collectionName) {
        String key = getCollectionIndexKey(databaseName, collectionName);
        if (!collectionsIndexMap.containsKey(key)) {
            CollectionIndex collectionIndex = new CollectionIndex();
            collectionsIndexMap.put(key, collectionIndex);
            fileService.createCollectionIndexFile(databaseName, collectionName);
        }
    }
    public CollectionIndex getCollectionIndex(String databaseName, String collectionName) {
        String key = getCollectionIndexKey(databaseName, collectionName);
        CollectionIndex collectionIndex = collectionsIndexMap.get(key);
        if (collectionIndex == null) {
            throw new IllegalArgumentException("Index does not exist.");
        }
        return collectionIndex;
    }

    public void insertDocumentIntoCollectionIndex(String databaseName , String collectionName, String documentId) {
        CollectionIndex collectionIndex = getCollectionIndex(databaseName,collectionName);
        Integer existingValue = collectionIndex.search(documentId);
        if (existingValue == null) {
            int index = collectionIndex.getSize();
            collectionIndex.insert(documentId, index);
            String indexFilePath = fileService.getCollectionIndexFile(databaseName, collectionName).getPath();
            fileService.appendToIndexFile(indexFilePath, documentId, String.valueOf(index));
        }else {
            log.error("Entry already exists in the index for collection: " + collectionName);
        }
    }

    public void deleteDocumentFromCollectionIndex(String databaseName, String collectionName, String documentId) {
        CollectionIndex collectionIndex = getCollectionIndex(databaseName,collectionName);
        Integer deletedIndex = collectionIndex.search(documentId);

        if (deletedIndex == null) {
            log.error("Document not found in the index, deletion failed.");
            throw new IllegalArgumentException("Document not found in the index.");
        }

        collectionIndex.delete(documentId);
        updateIndexes(collectionIndex, deletedIndex);
        fileService.rewriteIndexFile(databaseName, collectionName, collectionIndex);
    }
    public boolean deleteCollectionIndex(String databaseName,String collectionName) {
        String key = getCollectionIndexKey(databaseName, collectionName);
        if (collectionsIndexMap.containsKey(key)) {
            // Delete all property indexes for this collection
            propertyIndexMap.entrySet().removeIf(entry -> entry.getKey().startsWith(key));

            collectionsIndexMap.remove(key);
            File indexesDirectory = fileService.getCollectionIndexesDirectory(databaseName, collectionName);
            try {
                fileService.deleteRecursively(indexesDirectory.toPath());
                return true;
            } catch (IOException e) {
                log.error("Failed to delete collection index directory for collection : "+collectionName);
                return false;
            }
        }else
            return true;
    }

    private void updateIndexes(@NotNull CollectionIndex collectionIndex, Integer deletedIndex) {
        List<Map.Entry<String, Integer>> allEntries = new ArrayList<>(collectionIndex.getBPlusTree().getAllEntries());
        Map<String, Integer> updatedEntries = new HashMap<>();

        for (Map.Entry<String, Integer> entry : allEntries) {
            int currentIndex = entry.getValue();
            updatedEntries.put(entry.getKey(), currentIndex > deletedIndex ? currentIndex - 1 : currentIndex);
        }

        updatedEntries.remove(deletedIndex);
        collectionIndex.getBPlusTree().clearTree();

        for (Map.Entry<String, Integer> entry : updatedEntries.entrySet()) {
            collectionIndex.insert(entry.getKey(), entry.getValue());
        }
    }
    public Integer searchInCollectionIndex(String databaseName,String collectionName, String documentId) throws ResourceNotFoundException {
        Integer result =  getCollectionIndex(databaseName,collectionName).search(documentId);
        if (result == null) {
            log.error("Document not found in the index.");
            throw new ResourceNotFoundException("Document (in " + collectionName+")");
        }
        return result;
    }

    public void createPropertyIndex(String databaseName,String collectionName, String propertyName) {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        if (!propertyIndexMap.containsKey(propertyIndexKey)) {
            PropertyIndex index = new PropertyIndex();
            propertyIndexMap.put(propertyIndexKey, index);
            fileService.createPropertyIndexFile(databaseName, collectionName, propertyName);
        }
    }

    public PropertyIndex getPropertyIndex(String databaseName,String collectionName, String propertyName) {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        PropertyIndex index = propertyIndexMap.get(propertyIndexKey);
        if (index == null) {
            throw new IllegalArgumentException("Index does not exist.");
        }
        return index;
    }

    public void insertIntoPropertyIndex(String databaseName, String collectionName, String propertyName, String propertyValue, String documentId) {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        PropertyIndex propertyIndex = propertyIndexMap.get(propertyIndexKey);
        if(propertyIndex == null){
            createPropertyIndex(databaseName,collectionName,propertyName);
            propertyIndex = propertyIndexMap.get(propertyIndexKey);
        }
        if (!propertyValue.equals(propertyIndex.search(documentId))) {
            propertyIndex.insert(documentId, propertyValue);
            String indexFilePath = fileService.getPropertyIndexFile(databaseName, collectionName, propertyName).getPath();
            fileService.appendToIndexFile(indexFilePath, documentId, propertyValue);
            log.info("Inserted new entry in property index for collection: " + collectionName + " property: " + propertyName);
        } else {
            log.error("Entry already exists in property index for collection: " + collectionName + " property: " + propertyName);
        }
    }
    public String searchInPropertyIndex(String databaseName,String collectionName, String propertyName, String documentId) {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        PropertyIndex propertyIndex = propertyIndexMap.get(propertyIndexKey);
        if (propertyIndex == null) {
            throw new IllegalArgumentException("Property Index does not exist");
        }
        return propertyIndex.search(documentId);
    }
    public void deleteFromPropertyIndex(String databaseName,String collectionName, String propertyName, String documentId) {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        PropertyIndex propertyIndex = propertyIndexMap.get(propertyIndexKey);
        if (propertyIndex == null) {
            log.error("Property Index does not exist.");
            throw new IllegalArgumentException("Property Index does not exist.");
        }
        propertyIndex.delete(documentId);
        fileService.rewritePropertyIndexFile(fileService.getPropertyIndexFile(databaseName, collectionName, propertyName)
                                            , propertyIndex);
    }
    public void loadAllIndexes(String databaseName) {
        log.info("Loading all indexes from disk...");
        File indexesDirectory = fileService.getRootIndexesDirectory(databaseName);
        if(!indexesDirectory.exists()){
            return;
        }
        File[] collectionDirectories = indexesDirectory.listFiles();
        if (collectionDirectories != null) {


            for (File collectionDir : collectionDirectories) {
                String collectionName = collectionDir.getName();

                File[] filesInDir = collectionDir.listFiles();
                if (filesInDir != null) {
                    for (File indexFile : filesInDir) {
                        if (fileService.isCollectionIndexFile(indexFile.getName())) {
                            loadCollectionIndex(databaseName, indexFile.getName());
                        } else if (fileService.isPropertyIndexFile(indexFile.getName())) {
                            loadPropertyIndex(databaseName,collectionName ,indexFile.getName());
                        }
                    }
                }
            }
        } else {
            log.error("loading indexes Failed, no collections found.");
        }
    }

    public void loadCollectionIndex(String databaseName , @NotNull String collectionFileName) {
        String collectionName = collectionFileName.substring(0, collectionFileName.length() - 21);
        File indexFile = fileService.getCollectionIndexFile(databaseName,collectionName);
        if (!indexFile.exists()){
            log.error("Index file does not exist for collection: " + collectionName);
            return;
        }
        CollectionIndex collectionIndex = new CollectionIndex();
        collectionsIndexMap.put(getCollectionIndexKey(databaseName,collectionName), collectionIndex);
        Map<String, Integer> indexData = fileService.readCollectionIndexFile(indexFile);
        for (Map.Entry<String, Integer> entry : indexData.entrySet()) {
            collectionIndex.insert(entry.getKey(), entry.getValue());
        }
        log.info("loaded index for collection: " + collectionName);
    }

    public void loadPropertyIndex(String databaseName,String collectionName , @NotNull String indexFileName) {
        String[] split = indexFileName.split("_");
        String propertyName = split[0];
        File indexFile = fileService.getPropertyIndexFile(databaseName,collectionName,propertyName);
        if (!indexFile.exists()){
            log.error("Index file does not exist for collection: " + collectionName + " property: " + propertyName);
            return;
        }
        PropertyIndex propertyIndex = new PropertyIndex();
        propertyIndexMap.put(getPropertyIndexKey(databaseName,collectionName,propertyName), propertyIndex);
        Map<String, String> indexData = fileService.readPropertyIndexFile(indexFile);
        for (Map.Entry<String, String> entry : indexData.entrySet()) {
            propertyIndex.insert(entry.getKey(), entry.getValue());
        }
        log.info("loaded property index for collection: " + collectionName + " property: " + propertyName);
    }

    public void deleteDocumentRelatedIndexes(String databaseName,String collectionName, String documentId) throws ResourceNotFoundException {
        DatabaseDiskCRUD databaseDiskCRUD = DatabaseDiskCRUD.getInstance();
        Schema collectionSchema = databaseDiskCRUD.getCollectionSchema(databaseName, collectionName);
        // Delete from all property indexes
        for (String propertyName : collectionSchema.getProperties().keySet()) {
            deleteFromPropertyIndex(databaseName,collectionName, propertyName, documentId);
        }
        // Delete from collection index
        deleteDocumentFromCollectionIndex(databaseName,collectionName, documentId);
    }


    public boolean collectionIndexExists(String databaseName,String collectionName) {
        String key = getCollectionIndexKey(databaseName, collectionName);
        return collectionsIndexMap.containsKey(key);
    }
    public boolean documentExistsInCollectionIndex(String databaseName,String collectionName, String documentId) {
        return getCollectionIndex(databaseName,collectionName).search(documentId) != null;
    }

    public boolean propertyIndexExists(String databaseName,String collectionName, String propertyName) {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        return propertyIndexMap.containsKey(propertyIndexKey);
    }
    public String getCollectionIndexKey(String databaseName,String collectionName) {
        return databaseName + "::" + collectionName;
    }
    public String getPropertyIndexKey(String databaseName,String collectionName, String propertyName) {
        return databaseName + "::" + collectionName + "::" + propertyName;
    }
    public String getDocumentIndexKey(String databaseName,String collectionName, String documentId) {
        return databaseName + "::" + collectionName + "::" + documentId;
    }
    public void deleteAllIndexes(){
        collectionsIndexMap.clear();
        propertyIndexMap.clear();
    }

}
