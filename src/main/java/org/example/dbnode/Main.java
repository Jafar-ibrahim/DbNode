package org.example.dbnode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Indexing.IndexingManager;
import org.example.dbnode.Model.Schema;
import org.example.dbnode.Model.TestModel;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws ResourceAlreadyExistsException, ResourceNotFoundException, IOException, OperationFailedException {
        DatabaseDiskCRUD databaseDiskCRUD = DatabaseDiskCRUD.getInstance();
        IndexingManager indexingManager = IndexingManager.getInstance();
        //databaseDiskCRUD.createDatabase("test");
        //databaseDiskCRUD.deleteDatabase("test");
        //System.out.println(databaseDiskCRUD.readDatabases());
        TestModel testModel = new TestModel();
        testModel.setId("1");
        testModel.setName("test");
        JsonNode schema = Schema.fromClass(TestModel.class);
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter(); // Key part
        System.out.println(writer.writeValueAsString(schema));
        ObjectNode testModelNode = (ObjectNode) mapper.readTree(writer.writeValueAsString(testModel));
        System.out.println(testModelNode);
        Schema schema1 = Schema.of(writer.writeValueAsString(schema));
        System.out.println(schema1.validateDocument(mapper.valueToTree(testModel)));

        //databaseDiskCRUD.createCollectionFromClass("test", "testCollection", testModel.class);
        //databaseDiskCRUD.deleteCollection("test","testCollection");

    }
}
