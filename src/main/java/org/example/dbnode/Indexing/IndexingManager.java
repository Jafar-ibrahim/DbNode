package org.example.dbnode.Indexing;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.DatabaseDiskCRUD;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.DatabaseRegistry;
import org.example.dbnode.Model.Document;
import org.example.dbnode.Model.Schema;
import org.example.dbnode.Service.FileService;
import org.example.dbnode.Util.DataTypes.DataTypeCaster;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
@Log4j2
@Component
public class IndexingManager {

    private final Map<String, CollectionIndex> collectionsIndexMap;
    private final Map<String, PropertyIndex> propertyIndexMap;
    private final Map<String, InvertedPropertyIndex> invertedPropertyIndexMap;
    private final DatabaseDiskCRUD databaseDiskCRUD;
    private final FileService fileService;
    private final DatabaseRegistry databaseRegistry;
    private final DataTypeCaster dataTypeCaster;

    @Autowired
    public IndexingManager(@Lazy DatabaseDiskCRUD databaseDiskCRUD, FileService fileService, @Lazy DatabaseRegistry databaseRegistry, DataTypeCaster dataTypeCaster) {
        this.collectionsIndexMap = new ConcurrentHashMap<>();
        this.propertyIndexMap = new ConcurrentHashMap<>();
        this.invertedPropertyIndexMap = new ConcurrentHashMap<>();
        this.databaseDiskCRUD = databaseDiskCRUD;
        this.fileService = fileService;
        this.databaseRegistry = databaseRegistry;
        this.dataTypeCaster = dataTypeCaster;
    }
    
    public void init() throws ResourceNotFoundException {
        List<String> allDatabases = databaseRegistry.readDatabases();
        if (allDatabases.isEmpty()) {
            log.warn("No databases found, skipping index loading.");
            return;
        }
        for (String dbName : allDatabases) {
            loadAllIndexes(dbName);
        }
    }
    public void createCollectionIndex(String databaseName, String collectionName) {
        String key = getCollectionIndexKey(databaseName, collectionName);
        if (!collectionsIndexMap.containsKey(key)) {
            CollectionIndex collectionIndex = new CollectionIndex();
            collectionsIndexMap.put(key, collectionIndex);
            fileService.createCollectionIndexFile(databaseName, collectionName);
        }
    }
    public CollectionIndex getCollectionIndex(String databaseName, String collectionName) throws ResourceNotFoundException {
        String key = getCollectionIndexKey(databaseName, collectionName);
        CollectionIndex collectionIndex = collectionsIndexMap.get(key);
        if (collectionIndex == null) {
            throw new ResourceNotFoundException("Collection Index");
        }
        return collectionIndex;
    }

    public void insertDocumentIntoCollectionIndex(String databaseName , String collectionName, String documentId) throws ResourceNotFoundException {
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

    public void deleteDocumentFromCollectionIndex(String databaseName, String collectionName, String documentId) throws ResourceNotFoundException {
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

    public void createInvertedPropertyIndex(String databaseName,String collectionName, String propertyName) throws ResourceNotFoundException {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        if (!invertedPropertyIndexMap.containsKey(propertyIndexKey)) {
            String propertyDataType = dataTypeCaster.getDataType(databaseName, collectionName, propertyName);
            InvertedPropertyIndex index;
            switch (Objects.requireNonNull(propertyDataType).toUpperCase()) {
                case "STRING" -> index = new InvertedPropertyIndex<String>();
                case "INTEGER" -> index = new InvertedPropertyIndex<Integer>();
                case "NUMBER" -> index = new InvertedPropertyIndex<Double>();
                case "BOOLEAN" -> index = new InvertedPropertyIndex<Boolean>();
                default -> throw new IllegalArgumentException("Invalid data type for property: " + propertyName);
            }
            invertedPropertyIndexMap.put(propertyIndexKey, index);
        }
    }

    public PropertyIndex getPropertyIndex(String databaseName,String collectionName, String propertyName) throws ResourceNotFoundException {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        PropertyIndex index = propertyIndexMap.get(propertyIndexKey);
        if (index == null) {
            throw new ResourceNotFoundException("Property Index");
        }
        return index;
    }

    public InvertedPropertyIndex getInvertedPropertyIndex(String databaseName, String collectionName, String propertyName) throws ResourceNotFoundException {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        InvertedPropertyIndex invertedPropertyIndex = invertedPropertyIndexMap.get(propertyIndexKey);
        if (invertedPropertyIndex == null) {
            throw new ResourceNotFoundException("Inverted Property Index");
        }
        return invertedPropertyIndex;
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
    @SuppressWarnings("unchecked")
    public void insertIntoInvertedPropertyIndex(String databaseName, String collectionName, String propertyName, String propertyValue, String documentId) {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        InvertedPropertyIndex invertedPropertyIndex = invertedPropertyIndexMap.get(propertyIndexKey);
        if(invertedPropertyIndex == null){
            try {
                createInvertedPropertyIndex(databaseName,collectionName,propertyName);
            } catch (ResourceNotFoundException e) {
                log.error("Failed to create inverted property index.");
                return;
            }
            invertedPropertyIndex = invertedPropertyIndexMap.get(propertyIndexKey);
        }

        try {
            Object propertyValueCasted = dataTypeCaster.castToDataType(propertyValue, databaseName, collectionName, propertyName);
            if (propertyValueCasted instanceof String) {
                invertedPropertyIndex.insert((String) propertyValueCasted, documentId);
            } else if (propertyValueCasted instanceof Integer) {
                invertedPropertyIndex.insert((Integer) propertyValueCasted, documentId);
            } else if (propertyValueCasted instanceof Double) {
                invertedPropertyIndex.insert((Double) propertyValueCasted, documentId);
            } else if (propertyValueCasted instanceof Boolean) {
                invertedPropertyIndex.insert((Boolean) propertyValueCasted, documentId);
            } else {
                log.error("Failed to cast property value to a valid data type.");
            }
            log.info("Inserted new entry in inverted property index for collection: " + collectionName + " property: " + propertyName + " value: " + propertyValue);
        }catch (Exception e){
            log.error("Failed to insert into inverted property index.");
            throw new RuntimeException("Failed to insert into inverted property index.");
        }
    }
    public String searchInPropertyIndex(String databaseName,String collectionName, String propertyName, String documentId) throws ResourceNotFoundException {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        PropertyIndex propertyIndex = propertyIndexMap.get(propertyIndexKey);
        if (propertyIndex == null) {
            throw new ResourceNotFoundException("Property Index");
        }
        return propertyIndex.search(documentId);
    }
    @SuppressWarnings("unchecked")
    public List<String> searchInInvertedPropertyIndex(String databaseName, String collectionName, String propertyName, String propertyValue) throws ResourceNotFoundException {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        InvertedPropertyIndex invertedPropertyIndex = invertedPropertyIndexMap.get(propertyIndexKey);

        if (invertedPropertyIndex == null) {
            throw new ResourceNotFoundException("Inverted Property Index");
        }
        Object propertyValueCasted = dataTypeCaster.castToDataType(propertyValue, databaseName, collectionName, propertyName);
        if (propertyValueCasted instanceof String) {
            return invertedPropertyIndex.search((String) propertyValueCasted);
        } else if (propertyValueCasted instanceof Integer) {
            return invertedPropertyIndex.search((Integer) propertyValueCasted);
        } else if (propertyValueCasted instanceof Double) {
            return invertedPropertyIndex.search((Double) propertyValueCasted);
        } else if (propertyValueCasted instanceof Boolean) {
            return invertedPropertyIndex.search((Boolean) propertyValueCasted);
        } else {
            log.error("Failed to cast property value to a valid data type.");
            return new ArrayList<>();
        }
    }
    public void deleteFromPropertyIndex(String databaseName,String collectionName, String propertyName, String documentId) throws ResourceNotFoundException {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        PropertyIndex propertyIndex = propertyIndexMap.get(propertyIndexKey);
        if (propertyIndex == null) {
            log.error("Property Index does not exist.");
            throw new ResourceNotFoundException("Property Index");
        }
        propertyIndex.delete(documentId);
        fileService.rewritePropertyIndexFile(fileService.getPropertyIndexFile(databaseName, collectionName, propertyName), propertyIndex);
    }
    @SuppressWarnings("unchecked")
    public void deleteDocumentFromInvertedPropertyIndex(String databaseName, String collectionName, String propertyName,String propertyValue, String documentId) throws ResourceNotFoundException {
        String propertyIndexKey = getPropertyIndexKey(databaseName, collectionName, propertyName);
        InvertedPropertyIndex invertedPropertyIndex = invertedPropertyIndexMap.get(propertyIndexKey);
        if (invertedPropertyIndex == null) {
            log.error("Inverted Property Index does not exist.");
            throw new ResourceNotFoundException("Inverted Property Index");
        }
        Object propertyValueCasted = dataTypeCaster.castToDataType(propertyValue, databaseName, collectionName, propertyName);
        if (propertyValueCasted instanceof String) {
            invertedPropertyIndex.search("\""+ propertyValueCasted +"\"").remove(documentId);
        } else if (propertyValueCasted instanceof Integer) {
            invertedPropertyIndex.search((Integer) propertyValueCasted).remove(documentId);
        } else if (propertyValueCasted instanceof Double) {
            invertedPropertyIndex.search((Double) propertyValueCasted).remove(documentId);
        } else if (propertyValueCasted instanceof Boolean) {
            invertedPropertyIndex.search((Boolean) propertyValueCasted).remove(documentId);
        } else {
            log.error("Failed to cast property value to a valid data type.");
        }
    }
    public void loadAllIndexes(String databaseName) throws ResourceNotFoundException {
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

    @SuppressWarnings("unchecked")
    public void loadPropertyIndex(String databaseName,String collectionName , @NotNull String indexFileName) throws ResourceNotFoundException {
        String[] split = indexFileName.split("_");
        String propertyName = split[0];
        File indexFile = fileService.getPropertyIndexFile(databaseName,collectionName,propertyName);
        if (!indexFile.exists()){
            log.error("Index file does not exist for collection: " + collectionName + " property: " + propertyName);
            return;
        }
        String key = getPropertyIndexKey(databaseName, collectionName, propertyName);
        PropertyIndex propertyIndex = new PropertyIndex();
        propertyIndexMap.put(key, propertyIndex);

        InvertedPropertyIndex invertedPropertyIndex = invertedPropertyIndexMap.get(key);
        if(invertedPropertyIndex == null){
            createInvertedPropertyIndex(databaseName,collectionName,propertyName);
            invertedPropertyIndex = invertedPropertyIndexMap.get(key);
        }
        String propertyType = dataTypeCaster.getDataType(databaseName, collectionName, propertyName);
        Map<String, String> indexData = fileService.readPropertyIndexFile(indexFile);

        switch (Objects.requireNonNull(propertyType).toUpperCase()) {
            case "STRING" -> {
                for (Map.Entry<String, String> entry : indexData.entrySet()) {
                    propertyIndex.insert(entry.getKey(), entry.getValue());
                    invertedPropertyIndex.insert(entry.getValue(), entry.getKey());
                }
            }
            case "INTEGER" -> {
                for (Map.Entry<String, String> entry : indexData.entrySet()) {
                    propertyIndex.insert(entry.getKey(), entry.getValue());
                    invertedPropertyIndex.insert(Integer.parseInt(entry.getValue()), entry.getKey());
                }
            }
            case "NUMBER" -> {
                for (Map.Entry<String, String> entry : indexData.entrySet()) {
                    propertyIndex.insert(entry.getKey(), entry.getValue());
                    invertedPropertyIndex.insert(Double.parseDouble(entry.getValue()), entry.getKey());
                }
            }
            case "BOOLEAN" -> {
                for (Map.Entry<String, String> entry : indexData.entrySet()) {
                    propertyIndex.insert(entry.getKey(), entry.getValue());
                    invertedPropertyIndex.insert(Boolean.parseBoolean(entry.getValue()), entry.getKey());
                }
            }
            default -> log.error("Failed to cast property value to a valid data type.");
        }
        log.info("loaded property index for collection: " + collectionName + " property: " + propertyName);
    }

    public void deleteDocumentRelatedIndexes(String databaseName,String collectionName, String documentId) throws ResourceNotFoundException {
        Document document = databaseDiskCRUD.fetchDocumentFromDatabase(databaseName, collectionName, documentId)
                                            .orElseThrow(() -> new ResourceNotFoundException("Document with id : "+documentId));
        ObjectNode documentContent = document.getContent();
        Schema collectionSchema = databaseDiskCRUD.getCollectionSchema(databaseName, collectionName);
        // Delete from all property indexes
        for (String propertyName : collectionSchema.getProperties().keySet()) {
            String propertyValue = documentContent.get(propertyName).asText();

            deleteFromPropertyIndex(databaseName,collectionName, propertyName, documentId);
            deleteDocumentFromInvertedPropertyIndex(databaseName,collectionName, propertyName, propertyValue, documentId);
        }
        // Delete from collection index
        deleteDocumentFromCollectionIndex(databaseName,collectionName, documentId);
    }


    public boolean collectionIndexExists(String databaseName,String collectionName) {
        String key = getCollectionIndexKey(databaseName, collectionName);
        return collectionsIndexMap.containsKey(key);
    }
    public boolean documentExistsInCollectionIndex(String databaseName,String collectionName, String documentId) throws ResourceNotFoundException {
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
        invertedPropertyIndexMap.clear();
    }

}
