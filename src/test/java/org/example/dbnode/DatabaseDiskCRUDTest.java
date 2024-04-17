package org.example.dbnode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Exception.*;
import org.example.dbnode.Model.Document;
import org.example.dbnode.Model.TestModel;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
@Log4j2
class DatabaseDiskCRUDTest {

    private static DatabaseDiskCRUD databaseDiskCRUD;

    @BeforeAll
    static void setup() throws IOException, ResourceNotFoundException {
        databaseDiskCRUD = DatabaseDiskCRUD.getInstance();
    }

    @Test
    void createDatabaseSuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        assertTrue(databaseDiskCRUD.readDatabases().contains("testDB"));
    }

    @Test
    void createDatabaseWithInvalidName() {
        assertThrows(InvalidResourceNameException.class, () -> databaseDiskCRUD.createDatabase("invalid/name"));
    }

    @Test
    void createDatabaseThatAlreadyExists() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        assertThrows(ResourceAlreadyExistsException.class, () -> databaseDiskCRUD.createDatabase("testDB"));
    }

    @Test
    void deleteDatabaseSuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.deleteDatabase("testDB");
        assertFalse(databaseDiskCRUD.readDatabases().contains("testDB"));
    }

    @Test
    void deleteDatabaseThatDoesNotExist() {
        assertThrows(ResourceNotFoundException.class, () -> databaseDiskCRUD.deleteDatabase("nonexistentDB"));
    }
    @Test
    void createCollectionSuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", TestModel.class);
        System.out.println(databaseDiskCRUD.readCollections("testDB"));
        assertTrue(databaseDiskCRUD.readCollections("testDB").contains("testCollection"));
    }

    @Test
    void createCollectionWithInvalidName() {
        assertThrows(InvalidResourceNameException.class, () -> databaseDiskCRUD.createCollectionFromClass("testDB", "invalid/name", TestModel.class));
    }

    @Test
    void createCollectionThatAlreadyExists() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", TestModel.class);
        assertThrows(ResourceAlreadyExistsException.class, () -> databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", TestModel.class));
    }

    @Test
    void deleteCollectionSuccessfully() throws Exception {
        //databaseDiskCRUD.createDatabase("testDB");
        //databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", TestModel.class);
        databaseDiskCRUD.deleteCollection("testDB", "testCollection");
        assertFalse(databaseDiskCRUD.readCollections("testDB").contains("testCollection"));
    }

    @Test
    void deleteCollectionThatDoesNotExist() {
        assertThrows(ResourceNotFoundException.class, () -> databaseDiskCRUD.deleteCollection("testDB", "nonexistentCollection"));
    }
    @Test
    void addDocumentToCollectionSuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", TestModel.class);
        TestModel testModel = new TestModel();
        testModel.setName("Test Name");
        Document document = new Document(testModel.toJson(testModel));

        document = databaseDiskCRUD.createDocument("testDB", "testCollection", document,UUID.randomUUID().toString());
        assertNotNull(databaseDiskCRUD.fetchDocumentFromDatabase("testDB", "testCollection", document.getId()));
    }

    @Test
    void addDocumentToNonexistentCollection() {
        TestModel testModel = new TestModel();
        testModel.setName("Test Name");
        Document document = new Document(testModel.toJson(testModel));
        assertThrows(ResourceNotFoundException.class, () -> databaseDiskCRUD.createDocument("testDB", "nonexistentCollection", document,UUID.randomUUID().toString()));
    }

    @Test
    void deleteDocumentFromCollectionSuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", Document.class);
        TestModel testModel = new TestModel();
        testModel.setName("Test Name");
        Document document = new Document(testModel.toJson(testModel));
        document= databaseDiskCRUD.createDocument("testDB", "testCollection", document,UUID.randomUUID().toString());
        databaseDiskCRUD.deleteDocumentFromCollection("testDB", "testCollection", document.getId());
        assertEquals(Optional.empty(),databaseDiskCRUD.fetchDocumentFromDatabase("testDB", "testCollection", document.getId()));
    }

    @Test
    void deleteDocumentFromNonexistentCollection() {
        assertThrows(ResourceNotFoundException.class, () -> databaseDiskCRUD.deleteDocumentFromCollection("testDB", "nonexistentCollection", "testDoc"));
    }

    @Test
    void updateDocumentPropertySuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", Document.class);
        TestModel testModel = new TestModel();
        testModel.setName("Test Name");
        Document document = new Document(testModel.toJson(testModel));
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode updatedProperties = mapper.createObjectNode();
        updatedProperties.put("name", "Jafar");
        updatedProperties.put("_version", 0);
        document = databaseDiskCRUD.createDocument("testDB", "testCollection", document, UUID.randomUUID().toString());
        databaseDiskCRUD.updateDocument("testDB", "testCollection", document.getId(),updatedProperties);
        ObjectNode updatedDocument = databaseDiskCRUD.fetchNodeById("testDB", "testCollection", document.getId());
        assertEquals("testValue", updatedDocument.get("testProperty").asText());
    }

    @Test
    void updateDocumentPropertyInNonexistentCollection() {
        Document document = new Document();
        document.setId("testDoc");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode updatedProperties = mapper.createObjectNode();
        updatedProperties.put("name", "Jafar");
        updatedProperties.put("_version", 0);
        assertThrows(ResourceNotFoundException.class, () -> databaseDiskCRUD.updateDocument("testDB", "nonexistentCollection", document.getId(),updatedProperties));
    }

    /*@AfterAll
    static void cleanup() throws IOException, ResourceNotFoundException {
        log.info("cleaning up...");
        databaseDiskCRUD.deleteDatabase("testDB");
    }*/
}
