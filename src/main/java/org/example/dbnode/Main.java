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
import org.example.dbnode.Model.testModel;

import java.io.IOException;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws ResourceAlreadyExistsException, ResourceNotFoundException, IOException, OperationFailedException {
        DatabaseDiskCRUD databaseDiskCRUD = new DatabaseDiskCRUD();
        IndexingManager indexingManager = IndexingManager.getInstance();
        //databaseDiskCRUD.createDatabase("test");
        //databaseDiskCRUD.deleteDatabase("test");
        //System.out.println(databaseDiskCRUD.readDatabases());
        JsonNode schema = Schema.of(testModel.class);
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter(); // Key part
        //System.out.println(writer.writeValueAsString(schema));
        Schema schema1 = Schema.convertJsonToSchema(writer.writeValueAsString(schema));
        System.out.println(Arrays.toString(schema1.getRequired()));
        System.out.println(schema1.getProperties());
        System.out.println(schema1.toJson());
        //databaseDiskCRUD.createCollectionFromClass("test", "testCollection", testModel.class);
        //databaseDiskCRUD.deleteCollection("test","testCollection");

    }
}
