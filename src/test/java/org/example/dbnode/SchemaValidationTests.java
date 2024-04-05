package org.example.dbnode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Exception.*;
import org.example.dbnode.Model.Document;
import org.example.dbnode.Model.Schema;
import org.example.dbnode.Model.TestModel;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
@Log4j2
public class SchemaValidationTests {
    private static DatabaseDiskCRUD databaseDiskCRUD;
    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void setup() throws IOException, ResourceNotFoundException {
        databaseDiskCRUD = DatabaseDiskCRUD.getInstance();
    }
    @Test
    void createCollectionFromClassAndValidateSchemaSuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", TestModel.class);
        TestModel testModel = new TestModel();
        testModel.setId("1");
        testModel.setName("Test Name");
        ObjectNode testModelNode = mapper.valueToTree(testModel);
        Schema schema = databaseDiskCRUD.getCollectionSchema("testDB", "testCollection");
        assertTrue(schema.validateDocument(testModelNode));
    }

    @Test
    void createCollectionFromJsonSchemaAndValidateSchemaSuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        JsonNode schemaNode = Schema.fromClass(TestModel.class);
        databaseDiskCRUD.createCollectionFromJsonSchema("testDB", "testCollection", schemaNode);
        TestModel testModel = new TestModel();
        testModel.setId("1");
        testModel.setName("Test Name");
        ObjectNode testModelNode = mapper.valueToTree(testModel);
        Schema schema = databaseDiskCRUD.getCollectionSchema("testDB", "testCollection");
        assertTrue(schema.validateDocument(testModelNode));
    }

    @Test
    void validateSchemaWithInvalidObject() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", TestModel.class);
        TestModel testModel = new TestModel();
        testModel.setName("Test Name");
        ObjectNode testModelNode = mapper.valueToTree(testModel);
        Schema schema = databaseDiskCRUD.getCollectionSchema("testDB", "testCollection");
        assertFalse(schema.validateDocument(testModelNode));
    }

    @Test
    void validateSchemaWithNonexistentCollection() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        TestModel testModel = new TestModel();
        testModel.setId("1");
        testModel.setName("Test Name");
        ObjectNode testModelNode = mapper.valueToTree(testModel);
        assertThrows(ResourceNotFoundException.class, () -> databaseDiskCRUD.getCollectionSchema("testDB", "nonexistentCollection").validateDocument(testModelNode));
    }

    @Test
    void validateSchemaWithObjectNodeSuccessfully() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", TestModel.class);
        ObjectNode testModelNode = mapper.createObjectNode();
        testModelNode.put("id", "1");
        testModelNode.put("name", "Test Name");
        Schema schema = databaseDiskCRUD.getCollectionSchema("testDB", "testCollection");
        assertTrue(schema.validateDocument(testModelNode));
    }

    @Test
    void validateSchemaWithInvalidObjectNode() throws Exception {
        databaseDiskCRUD.createDatabase("testDB");
        databaseDiskCRUD.createCollectionFromClass("testDB", "testCollection", TestModel.class);
        ObjectNode testModelNode = mapper.createObjectNode();
        testModelNode.put("id", "1");
        testModelNode.put("name", 123); // invalid value for name
        Schema schema = databaseDiskCRUD.getCollectionSchema("testDB", "testCollection");
        assertFalse(schema.validateDocument(testModelNode));
    }
    @AfterEach
    void cleanup() throws IOException, ResourceNotFoundException {
        log.info("cleaning up...");
        databaseDiskCRUD.deleteDatabase("testDB");
    }
}
