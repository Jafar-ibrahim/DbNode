package org.example.dbnode;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Exception.*;
import org.example.dbnode.Model.Document;
import org.example.dbnode.Model.testModel;
import org.junit.jupiter.api.*;
import org.springframework.http.HttpStatus;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
@Log4j2
class DatabaseDiskCRUDTest {

    private static DatabaseDiskCRUD databaseDiskCRUD;

    @BeforeAll
    static void setup() throws IOException, ResourceNotFoundException {
        databaseDiskCRUD = new DatabaseDiskCRUD();
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
    void addDocumentToCollectionSuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", testModel.class);
        testModel testModel = new testModel();
        testModel.setId("testDoc");
        testModel.setName("Test Name");
        Document document = new Document(testModel.toJson(testModel));
        Document document2 = new Document(testModel.toJson(testModel));

        databaseDiskCRUD.addDocumentToCollection("testDB", "testCollection", document);
        databaseDiskCRUD.addDocumentToCollection("testDB", "testCollection", document2);
        //assertNotNull(databaseDiskCRUD.fetchDocumentFromDatabase("testDB", "testCollection", "testDoc"));
    }

    @Test
    void addDocumentToNonexistentCollection() {
        Document document = new Document();
        document.setId("testDoc");
        assertThrows(ResourceNotFoundException.class, () -> databaseDiskCRUD.addDocumentToCollection("testDB", "nonexistentCollection", document));
    }

    @Test
    void deleteDocumentFromCollectionSuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", Document.class);
        Document document = new Document();
        document.setId("testDoc");
        databaseDiskCRUD.addDocumentToCollection("testDB", "testCollection", document);
        databaseDiskCRUD.deleteDocumentFromCollection("testDB", "testCollection", "testDoc");
        assertNull(databaseDiskCRUD.fetchDocumentFromDatabase("testDB", "testCollection", "testDoc"));
    }

    @Test
    void deleteDocumentFromNonexistentCollection() {
        assertThrows(ResourceNotFoundException.class, () -> databaseDiskCRUD.deleteDocumentFromCollection("testDB", "nonexistentCollection", "testDoc"));
    }

    @Test
    void updateDocumentPropertySuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", Document.class);
        Document document = new Document();
        document.setId("testDoc");
        databaseDiskCRUD.addDocumentToCollection("testDB", "testCollection", document);
        databaseDiskCRUD.updateDocumentProperty("testDB", "testCollection", document, document.getVersion(),"testProperty", "testValue");
        ObjectNode updatedDocument = databaseDiskCRUD.fetchNodeById("testDB", "testCollection", "testDoc");
        assertEquals("testValue", updatedDocument.get("testProperty").asText());
    }

    @Test
    void updateDocumentPropertyInNonexistentCollection() {
        Document document = new Document();
        document.setId("testDoc");
        assertThrows(ResourceNotFoundException.class, () -> databaseDiskCRUD.updateDocumentProperty("testDB", "nonexistentCollection", document, document.getVersion(),"testProperty", "testValue"));
    }

    /*@AfterAll
    static void cleanup() throws IOException, ResourceNotFoundException {
        log.info("cleaning up...");
        databaseDiskCRUD.deleteDatabase("testDB");
    }*/
}
