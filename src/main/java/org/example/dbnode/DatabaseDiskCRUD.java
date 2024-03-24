package org.example.dbnode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.InvalidResourceNameException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Model.Document;
import org.example.dbnode.Model.Schema;
import org.example.dbnode.Service.FileService;
import org.springframework.http.HttpStatus;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

@Log4j2
public class DatabaseDiskCRUD {

    private final FileService fileService;

    public DatabaseDiskCRUD() {
        fileService = FileService.getInstance();
    }

    public void createDatabase(String databaseName) throws ResourceAlreadyExistsException {
        if(FileService.invalidResourceName(databaseName)) throw new InvalidResourceNameException("Database");
        File dbDirectory = fileService.getDatabaseDirectory(databaseName);
        if (fileService.directoryNotExist(dbDirectory)) {
            fileService.createDirectoryIfNotExist(dbDirectory.toPath());
            File schemasDirectory = fileService.getSchemasPath(databaseName);
            File collectionsDirectory = fileService.getCollectionsPath(databaseName);
            fileService.createDirectoryIfNotExist(schemasDirectory.toPath());
            fileService.createDirectoryIfNotExist(collectionsDirectory.toPath());
        } else {
            log.error("Database creation failed : database already exists");
            throw new ResourceAlreadyExistsException("Database");
        }
    }
    public void deleteDatabase(String databaseName) throws ResourceNotFoundException,IOException{
        File dbDirectory = fileService.getDatabaseDirectory(databaseName);
        if (fileService.directoryNotExist(dbDirectory)) {
            log.error("Database deletion failed : database does not exist");
            throw new ResourceNotFoundException("Database");
        }
        fileService.deleteDirectory(dbDirectory);
        log.info("Database deleted successfully.");
    }
    public List<String> readDatabases() {
        File rootDirectory = fileService.getRootPath();
        if (fileService.directoryNotExist(rootDirectory)) {
            return Collections.emptyList();
        }
        String[] directories = rootDirectory.list((current, name) -> new File(current, name).isDirectory());
        if (directories == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(directories);
    }
    public void createCollectionFromJsonSchema(String databaseName,String collectionName, ObjectNode jsonSchema) throws ResourceAlreadyExistsException, IOException, ResourceNotFoundException {
        File schemaFile = createCollection(databaseName,collectionName);
        // Write schema on disk
        fileService.writePrettyJson(schemaFile, jsonSchema);
        log.info("Collection created successfully.");
    }
    public void createCollectionFromClass(String databaseName,String collectionName, Class<?> clazz) throws ResourceAlreadyExistsException, IOException, ResourceNotFoundException {
        File schemaFile = createCollection(databaseName,collectionName);
        JsonNode schema = Schema.of(clazz);
        // Write schema on disk
        fileService.writePrettyJson(schemaFile, schema);
        log.info("Collection created successfully.");
    }
    private File createCollection(String databaseName,String collectionName) throws ResourceNotFoundException, ResourceAlreadyExistsException, IOException {
        if(FileService.invalidResourceName(collectionName)) throw new InvalidResourceNameException("Collection");
        File dbDirectory = fileService.getDatabaseDirectory(databaseName);
        if (fileService.directoryNotExist(dbDirectory)) {
            log.error("Collection creation failed: database does not exist.");
            throw new ResourceNotFoundException("Database");
        }
        File collectionFile = fileService.getCollectionFile(databaseName,collectionName);
        File schemaFile = fileService.getSchemaFilePath(databaseName,collectionName);
        if (fileService.fileExists(collectionFile.getPath())) {
            log.error("Collection creation failed: collection already exists.");
            throw new ResourceAlreadyExistsException("Collection");
        }
        fileService.createDirectoryIfNotExist(schemaFile.getParentFile().toPath());
        fileService.createDirectoryIfNotExist(collectionFile.getParentFile().toPath());
        // Empty Json for Collection
        fileService.writeToFile(collectionFile,"[]");

        return schemaFile;
    }
    public void deleteCollection(String databaseName,String collectionName) throws ResourceNotFoundException, OperationFailedException {
        try {
            File databaseFile = fileService.getDatabaseDirectory(databaseName);
            if (fileService.directoryNotExist(databaseFile)) {
                log.error("Collection deletion failed : database does not exist");
                throw new ResourceNotFoundException("Database");
            }
            File collectionFile = fileService.getCollectionFile(databaseName,collectionName),
                 schemaFile = fileService.getSchemaFilePath(databaseName,collectionName),
                 indexDirectory = fileService.getIndexesFile(databaseName,collectionName);
            if (!fileService.fileExists(collectionFile.getPath()) || !fileService.fileExists(schemaFile.getPath())) {
                log.error("Collection deletion failed : Collection/Schema or both do not exist");
                throw new ResourceNotFoundException("Collection/Schema");
            }
            boolean collectionDeletionFailed = !fileService.deleteFile(collectionFile);
            boolean schemaDeletionFailed = !fileService.deleteFile(schemaFile);
            fileService.deleteRecursively(indexDirectory.toPath());
            if (collectionDeletionFailed && schemaDeletionFailed) {
                throw new OperationFailedException("delete collection, associated schema, account directory entries, or index");
            }
            log.info("Collection, its schema, associated account directory entries, and indexes have been successfully deleted");
        }catch (IOException e){
            e.printStackTrace();
            log.error("Failed to delete collection, associated schema, account directory entries, or index.");
        }
    }
    public List<String> readCollections(String databaseName) {
        File dbDirectory = fileService.getDatabaseDirectory(databaseName);
        if (fileService.directoryNotExist(dbDirectory)) {
            return Collections.emptyList();
        }
        File[] collectionFiles = dbDirectory.listFiles((dir, name) -> name.endsWith(".json"));
        if (collectionFiles == null) {
            return Collections.emptyList();
        }
        List<String> collectionNames = new ArrayList<>();
        for (File collectionFile : collectionFiles) {
            String fileName = collectionFile.getName();
            int extensionIndex = fileName.lastIndexOf(".");
            if (extensionIndex >= 0) {
                String collectionName = fileName.substring(0, extensionIndex);
                collectionNames.add(collectionName);
            }
        }
        return collectionNames;
    }
    // write a method to Add document to collection

    public void addDocumentToCollection(String databaseName,String collectionName, Document document) throws IOException, OperationFailedException {
        ObjectMapper mapper = new ObjectMapper();
        File collectionFile = fileService.getCollectionFile(databaseName,collectionName);
        List<JsonNode> collectionDocs = mapper.readValue(collectionFile, mapper.getTypeFactory().constructCollectionType(List.class, JsonNode.class));

        if (collectionDocs == null) {
            throw new OperationFailedException("read the existing collection");
        }
        ObjectNode documentData = document.getContent();
        documentData.put("_version", 0);
        try (RandomAccessFile file = new RandomAccessFile(collectionFile, "rw")) {
            long endPosition = file.length() - 1;
            file.seek(endPosition);
            if (!collectionDocs.isEmpty())
                file.writeBytes(",\n");

            mapper.writerWithDefaultPrettyPrinter().writeValue(file, collectionFile);
            file.writeBytes("]");

        }
        log.info("Added document to collection successfully");

    }
    // write a method to delete document from collection
    public void deleteDocumentFromCollection(String databaseName,String collectionName, String documentId) throws IOException, OperationFailedException {
        ObjectMapper mapper = new ObjectMapper();
        File collectionFile = fileService.getCollectionFile(databaseName,collectionName);
        List<JsonNode> collectionDocs = mapper.readValue(collectionFile, mapper.getTypeFactory().constructCollectionType(List.class, JsonNode.class));

        if (collectionDocs == null) {
            throw new OperationFailedException("read the existing collection");
        }
        collectionDocs.removeIf(document -> document.get("id").asText().equals(documentId));
        try (RandomAccessFile file = new RandomAccessFile(collectionFile, "rw")) {
            file.setLength(0);
            mapper.writerWithDefaultPrettyPrinter().writeValue(file, collectionDocs);
        }
        log.info("Deleted document from collection successfully");
    }



}
