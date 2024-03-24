package org.example.dbnode;

import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws ResourceAlreadyExistsException, ResourceNotFoundException, IOException, OperationFailedException {
        DatabaseDiskCRUD databaseDiskCRUD = new DatabaseDiskCRUD();

        //databaseDiskCRUD.createDatabase("test");
        //databaseDiskCRUD.deleteDatabase("test");
        //System.out.println(databaseDiskCRUD.readDatabases());
        /*JsonNode schema = Schema.of(testModel.class);
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter(); // Key part
        System.out.println(writer.writeValueAsString(schema));*/
        //databaseDiskCRUD.createCollectionFromClass("test", "testCollection", testModel.class);
        databaseDiskCRUD.deleteCollection("test","testCollection");
    }
}
