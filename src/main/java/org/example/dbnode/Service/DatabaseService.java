package org.example.dbnode.Service;

import lombok.extern.log4j.Log4j2;
import org.example.dbnode.DatabaseDiskCRUD;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.DatabaseRegistry;
import org.example.dbnode.Service.Interface.IDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Log4j2
@Service
public class DatabaseService implements IDatabaseService {
    private final DatabaseDiskCRUD databaseDiskCRUD;
    private final DatabaseRegistry databaseRegistry;

    @Autowired
    public DatabaseService(DatabaseDiskCRUD databaseDiskCRUD, DatabaseRegistry databaseRegistry) {
        this.databaseDiskCRUD = databaseDiskCRUD;
        this.databaseRegistry = databaseRegistry;
    }

    public void createDatabase(String databaseName) throws ResourceAlreadyExistsException, ResourceNotFoundException {
        log.info("creating database: " + databaseName);
        databaseRegistry.addDatabase(databaseName);
        databaseDiskCRUD.createDatabase(databaseName);
    }

    public void deleteDatabase(String databaseName) throws IOException, ResourceNotFoundException {
        log.info("deleting database: " + databaseName);
        databaseDiskCRUD.deleteDatabase(databaseName);
        databaseRegistry.deleteDatabase(databaseName);
    }
    public List<String> getAllDatabases() {
        log.info("reading all databases");
        return databaseRegistry.readDatabases();
    }
    public void deleteAllDatabases() throws IOException, ResourceNotFoundException {
        log.info("deleting all databases");
        for (String databaseName : databaseRegistry.readDatabases()) {
            deleteDatabase(databaseName);
        }
    }
}
