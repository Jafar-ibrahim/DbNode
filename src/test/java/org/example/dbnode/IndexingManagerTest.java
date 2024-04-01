package org.example.dbnode;

import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Indexing.CollectionIndex;
import org.example.dbnode.Indexing.IndexingManager;
import org.example.dbnode.Indexing.PropertyIndex;
import org.example.dbnode.Service.FileService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

public class IndexingManagerTest {

    private static IndexingManager indexingManager;
    private static FileService fileService;
    @BeforeAll
    public static void setup() {
        fileService = Mockito.mock(FileService.class);
        indexingManager = IndexingManager.getInstance();
    }

    @Test
    public void createIndex_createsNewIndex_whenIndexDoesNotExist() {
        String databaseName = "testDB";
        String collectionName = "testCollection";

        indexingManager.createCollectionIndex(databaseName, collectionName);

        CollectionIndex collectionIndex = indexingManager.getCollectionIndex(databaseName, collectionName);
        assertNotNull(collectionIndex);
    }

    @Test
    public void getCollectionIndex_throwsException_whenIndexDoesNotExist() {
        String databaseName = "testDB";
        String collectionName = "testCollection";

        assertThrows(IllegalArgumentException.class, () -> indexingManager.getCollectionIndex(databaseName, collectionName));
    }

    @Test
    public void insertIntoCollectionIndex_insertsNewEntry_whenEntryDoesNotExist() throws ResourceNotFoundException {
        String databaseName = "testDB";
        String collectionName = "testCollection";
        String documentId = "doc1";
        int index = 1;

        indexingManager.createCollectionIndex(databaseName, collectionName);
        indexingManager.insertDocumentIntoCollectionIndex(databaseName, collectionName, documentId);

        Integer result = indexingManager.searchInCollectionIndex(databaseName, collectionName, documentId);
        assertEquals(index, result);
    }

    @Test
    public void deleteFromCollectionIndex_deletesEntry_whenEntryExists() throws ResourceNotFoundException {
        String databaseName = "testDB";
        String collectionName = "testCollection";
        String documentId = "doc1";
        int index = 1;

        indexingManager.createCollectionIndex(databaseName, collectionName);
        indexingManager.insertDocumentIntoCollectionIndex(databaseName, collectionName, documentId);
        indexingManager.deleteDocumentFromCollectionIndex(databaseName, collectionName, documentId);

        Integer result = indexingManager.searchInCollectionIndex(databaseName, collectionName, documentId);
        assertNull(result);
    }

    @Test
    public void createPropertyIndex_createsNewPropertyIndex_whenPropertyIndexDoesNotExist() {
        String databaseName = "testDB";
        String collectionName = "testCollection";
        String propertyName = "property1";

        indexingManager.createPropertyIndex(databaseName, collectionName, propertyName);

        PropertyIndex propertyIndex = indexingManager.getPropertyIndex(databaseName, collectionName, propertyName);
        assertNotNull(propertyIndex);
    }

    @Test
    public void getPropertyIndex_throwsException_whenPropertyIndexDoesNotExist() {
        String databaseName = "testDB";
        String collectionName = "testCollection";
        String propertyName = "property1";

        assertThrows(IllegalArgumentException.class, () -> indexingManager.getPropertyIndex(databaseName, collectionName, propertyName));
    }

    @Test
    public void insertIntoPropertyIndex_insertsNewEntry_whenEntryDoesNotExist() {
        String databaseName = "testDB";
        String collectionName = "testCollection";
        String propertyName = "property1";
        String documentId = "doc1";
        String propertyValue = "value1";

        indexingManager.createPropertyIndex(databaseName, collectionName, propertyName);
        indexingManager.insertIntoPropertyIndex(databaseName, collectionName, propertyName, propertyValue, documentId);

        String result = indexingManager.searchInPropertyIndex(databaseName, collectionName, propertyName, documentId);
        assertEquals(propertyValue, result);
    }

    @Test
    public void deleteFromPropertyIndex_deletesEntry_whenEntryExists() {
        String databaseName = "testDB";
        String collectionName = "testCollection";
        String propertyName = "property1";
        String documentId = "doc1";
        String propertyValue = "value1";

        indexingManager.createPropertyIndex(databaseName, collectionName, propertyName);
        indexingManager.insertIntoPropertyIndex(databaseName, collectionName, propertyName, propertyValue, documentId);
        indexingManager.deleteFromPropertyIndex(databaseName, collectionName, propertyName, documentId);

        String result = indexingManager.searchInPropertyIndex(databaseName, collectionName, propertyName, documentId);
        assertNull(result);
    }
    //******************** Loading Tests ************************

    @Test
    public void loadAllIndexes_loadsIndexes_whenIndexesExistOnDisk() {
        String databaseName = "testDB";
        String collectionName = "collection1";
        String propertyName = "property1";
        String documentId = "doc1";
        int collectionIndexValue = 1;
        String propertyIndexValue = "value1";

        // Create and populate indexes
        indexingManager.createCollectionIndex(databaseName, collectionName);
        indexingManager.insertDocumentIntoCollectionIndex(databaseName, collectionName, documentId);
        indexingManager.createPropertyIndex(databaseName, collectionName, propertyName);
        indexingManager.insertIntoPropertyIndex(databaseName, collectionName, propertyName, propertyIndexValue, documentId);

        // Load indexes from disk
        indexingManager.loadAllIndexes(databaseName);

        // Verify that the indexes were loaded correctly
        CollectionIndex collectionIndex = indexingManager.getCollectionIndex(databaseName, collectionName);
        assertNotNull(collectionIndex);
        assertEquals(collectionIndexValue, collectionIndex.search(documentId));

        PropertyIndex propertyIndex = indexingManager.getPropertyIndex(databaseName, collectionName, propertyName);
        assertNotNull(propertyIndex);
        assertEquals(propertyIndexValue, propertyIndex.search(documentId));
    }

    @Test
    public void loadCollectionIndex_loadsIndex_whenIndexExistsOnDisk() {
        String databaseName = "testDB";
        String collectionName = "collection1";
        File indexFile = Mockito.mock(File.class);
        when(fileService.getCollectionIndexFile(databaseName, collectionName)).thenReturn(indexFile);
        when(indexFile.exists()).thenReturn(true);
        when(indexFile.getPath()).thenReturn("src/main/resources/databases/"+databaseName +"/indexes/" + collectionName +"/"+collectionName+ "_collection_index.txt");

        Map<String, Integer> indexData = new ConcurrentHashMap<>();
        indexData.put("doc1", 1);
        when(fileService.readCollectionIndexFile(indexFile)).thenReturn(indexData);

        indexingManager.loadCollectionIndex(databaseName, collectionName + "_collection_index.txt");

        CollectionIndex collectionIndex = indexingManager.getCollectionIndex(databaseName, collectionName);
        assertNotNull(collectionIndex);
        assertEquals(1, collectionIndex.search("doc1"));
    }

    @Test
    public void loadPropertyIndex_doesNothing_whenIndexDoesNotExistOnDisk() {
        String databaseName = "testDB";
        String collectionName = "collection1";
        String propertyName = "property1";
        File indexFile = Mockito.mock(File.class);
        when(fileService.getPropertyIndexFile(databaseName, collectionName, propertyName)).thenReturn(indexFile);
        when(indexFile.exists()).thenReturn(false);
        when(indexFile.getPath()).thenReturn("src/main/resources/databases/"+databaseName +"/indexes/" + collectionName +"/"+propertyName+ "_property_index.txt");


        indexingManager.loadPropertyIndex(databaseName, collectionName,propertyName + "_property_index.txt");
        indexingManager.deleteFromPropertyIndex(databaseName, collectionName, propertyName, "doc1");
        indexingManager.deleteAllIndexes();
        indexingManager.loadPropertyIndex(databaseName, collectionName,propertyName + "_property_index.txt");

        assertThrows(IllegalArgumentException.class, () -> indexingManager.getPropertyIndex(databaseName, collectionName, propertyName));
    }
}
