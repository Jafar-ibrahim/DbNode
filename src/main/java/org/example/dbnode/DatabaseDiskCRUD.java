package org.example.dbnode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Exception.*;
import org.example.dbnode.Indexing.IndexingManager;
import org.example.dbnode.Model.Document;
import org.example.dbnode.Model.Schema;
import org.example.dbnode.Service.FileService;
import org.example.dbnode.Service.LocksManager;
import org.springframework.http.HttpStatus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public class DatabaseDiskCRUD {

    private final FileService fileService;
    private final IndexingManager indexingManager = IndexingManager.getInstance();
    private final LocksManager locksManager = LocksManager.getInstance();

    public DatabaseDiskCRUD() {
        fileService = FileService.getInstance();
    }

    public void createDatabase(String databaseName) throws ResourceAlreadyExistsException {
        if (FileService.invalidResourceName(databaseName)){
            throw new InvalidResourceNameException("Database");
        }
        ReentrantLock databaseLock = locksManager.createDatabaseLockThenLock(databaseName);
        try{
            File dbDirectory = fileService.getDatabaseDirectory(databaseName);
            if (fileService.directoryNotExist(dbDirectory)) {
                fileService.createDirectoryIfNotExist(dbDirectory.toPath());
                File schemasDirectory = fileService.getSchemasPath(databaseName);
                File collectionsDirectory = fileService.getCollectionsPath(databaseName);
                fileService.createDirectoryIfNotExist(schemasDirectory.toPath());
                fileService.createDirectoryIfNotExist(collectionsDirectory.toPath());
                log.info("Database "+databaseName+" created successfully.");
            } else {
                log.error("Database creation failed : database already exists");
                throw new ResourceAlreadyExistsException("Database");
            }
        }finally {
            databaseLock.unlock();
        }

    }

    public void deleteDatabase(String databaseName) throws ResourceNotFoundException, IOException {
        ReentrantLock databaseLock = locksManager.getDatabaseLock(databaseName);
        try{
            File dbDirectory = fileService.getDatabaseDirectory(databaseName);
            if (fileService.directoryNotExist(dbDirectory)) {
                log.error("Database deletion failed : database does not exist");
                throw new ResourceNotFoundException("Database");
            }
            fileService.deleteDirectory(dbDirectory);
            log.info("Database "+databaseName+" deleted successfully.");
        }finally {
            databaseLock.unlock();
        }
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

    public void createCollectionFromJsonSchema(String databaseName, String collectionName, ObjectNode jsonSchema) throws ResourceAlreadyExistsException, IOException, ResourceNotFoundException {
        File schemaFile = createCollection(databaseName, collectionName);
        // Write schema on disk
        fileService.writePrettyJson(schemaFile, jsonSchema);
        log.info("Collection created successfully.");
    }

    public void createCollectionFromClass(String databaseName, String collectionName, Class<?> clazz) throws ResourceAlreadyExistsException, IOException, ResourceNotFoundException {
        File schemaFile = createCollection(databaseName, collectionName);
        JsonNode schema = Schema.of(clazz);
        // Write schema on disk
        fileService.writePrettyJson(schemaFile, schema);
        log.info("Collection created successfully.");
    }

    private File createCollection(String databaseName, String collectionName) throws ResourceNotFoundException, ResourceAlreadyExistsException, IOException {
        ReentrantLock collectionLock = locksManager.createCollectionLockThenLock(databaseName, collectionName);

        try {
            if (FileService.invalidResourceName(collectionName)) {
                throw new InvalidResourceNameException("Collection");
            }
            File dbDirectory = fileService.getDatabaseDirectory(databaseName);
            if (fileService.directoryNotExist(dbDirectory)) {
                log.error("Collection creation failed: database does not exist.");
                throw new ResourceNotFoundException("Database");
            }
            File collectionFile = fileService.getCollectionFile(databaseName, collectionName);
            File schemaFile = fileService.getSchemaFilePath(databaseName, collectionName);
            if (fileService.fileExists(collectionFile.getPath())) {
                log.error("Collection creation failed: collection already exists.");
                throw new ResourceAlreadyExistsException("Collection");
            }
            fileService.createDirectoryIfNotExist(schemaFile.getParentFile().toPath());
            fileService.createDirectoryIfNotExist(collectionFile.getParentFile().toPath());
            // Empty Json for Collection
            fileService.writeToFile(collectionFile, "[]");

            // index the collection
            indexingManager.createCollectionIndex(databaseName, collectionName);
            return schemaFile;
        }finally {
            collectionLock.unlock();
        }
    }

    public void deleteCollection(String databaseName, String collectionName) throws ResourceNotFoundException, OperationFailedException {
        ReentrantLock collectionLock = locksManager.getCollectionLock(databaseName, collectionName);
        collectionLock.lock();
        try{
            File databaseFile = fileService.getDatabaseDirectory(databaseName);
            if (fileService.directoryNotExist(databaseFile)) {
                log.error("Collection deletion failed : database does not exist");
                throw new ResourceNotFoundException("Database");
            }
            File collectionFile = fileService.getCollectionFile(databaseName, collectionName),
                    schemaFile = fileService.getSchemaFilePath(databaseName, collectionName);
            if (!fileService.fileExists(collectionFile.getPath()) || !fileService.fileExists(schemaFile.getPath())) {
                log.error("Collection deletion failed : Collection/Schema or both do not exist");
                throw new ResourceNotFoundException("Collection/Schema");
            }
            boolean collectionDeletionFailed = !fileService.deleteFile(collectionFile);
            boolean schemaDeletionFailed = !fileService.deleteFile(schemaFile);
            boolean indexesDeletionFailed = indexingManager.deleteCollectionIndex(databaseName, collectionName);
            if (collectionDeletionFailed || schemaDeletionFailed || indexesDeletionFailed){
                throw new OperationFailedException("delete collection, associated schema, account directory entries, or index");
            }
            log.info("Collection, its schema, associated account directory entries, and indexes have been successfully deleted");
        }finally {
            collectionLock.unlock();
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

    public void addDocumentToCollection(String databaseName, String collectionName, Document document) throws IOException, OperationFailedException, ResourceNotFoundException {
        ReentrantLock collectionLock;
        ReentrantLock documentLock;

        collectionLock = locksManager.getCollectionLock(databaseName, collectionName);
        documentLock = locksManager.createDocumentLockThenLock(databaseName, collectionName, document.getId());

        collectionLock.lock();
        try {
            ObjectMapper mapper = new ObjectMapper();
            File collectionFile = fileService.getCollectionFile(databaseName, collectionName);
            ArrayNode collectionDocs = (ArrayNode) mapper.readTree(collectionFile);
            if (collectionDocs == null) {
                throw new OperationFailedException("read the existing collection");
            }
            ObjectNode documentData = document.getContent();
            documentData= fileService.stampVersionOnDocument(documentData);
            documentData = fileService.assignIdForDocument(documentData);

            fileService.writeDocumentToCollection(collectionFile, documentData);

            indexingManager.insertDocumentIntoCollectionIndex(databaseName, collectionName, document.getId());
            log.info("Document with id " + document.getId() + " added to collection " + collectionName);
        }finally {
            collectionLock.unlock();
            documentLock.unlock();
        }
    }


    public void deleteDocumentFromCollection(String databaseName, String collectionName, String documentId) throws  OperationFailedException, ResourceNotFoundException {

        ReentrantLock collectionLock = locksManager.getCollectionLock(databaseName, collectionName),
                      documentLock = locksManager.getDocumentLock(databaseName, collectionName, documentId);

        ObjectMapper mapper = new ObjectMapper();
        File collectionFile = fileService.getCollectionFile(databaseName, collectionName);
        ArrayNode collectionDocs;
        try {
            collectionDocs = (ArrayNode) mapper.readTree(collectionFile);
        } catch (FileNotFoundException e) {
            throw new ResourceNotFoundException("File with ID " + documentId + "in collection " + collectionName);
        }catch (IOException e){
            throw new OperationFailedException("read the existing collection");
        }
        if (collectionDocs == null) {
            throw new OperationFailedException("read the existing collection");
        }

        int index = getDocumentIndex(databaseName, collectionName, documentId);

        collectionLock.lock();
        documentLock.lock();
        try {
            collectionDocs.remove(index);
            fileService.rewriteCollectionFile(collectionFile, collectionDocs);
            indexingManager.deleteDocumentFromCollectionIndex(databaseName, collectionName, documentId);
        }finally {
            collectionLock.unlock();
            documentLock.unlock();
        }
        locksManager.deleteDocumentLock(databaseName, collectionName, documentId);
        log.info("Deleted document from collection successfully");
    }

    public void updateDocumentProperty(String databaseName,
                                       String collectionName,
                                       Document document,
                                       Long expectedVersion,
                                       String propertyName, Object newValue) throws OperationFailedException, ResourceNotFoundException, VersionMismatchException {

            String documentId = document.getId();

            ReentrantLock collectionLock = locksManager.getCollectionLock(databaseName, collectionName),
                          documentLock = locksManager.getDocumentLock(databaseName, collectionName, documentId);

            ObjectMapper mapper = new ObjectMapper();
            ArrayNode jsonArray = fileService.readJsonArrayFile(fileService.getCollectionFile(databaseName, collectionName));
            IndexingManager indexManager = IndexingManager.getInstance();
            int index = getDocumentIndex(databaseName, collectionName, documentId);

            ObjectNode documentData = (ObjectNode) jsonArray.get(index);
            if (documentData.has("_version") && !Objects.equals(document.getVersion(), expectedVersion)) {
                throw new VersionMismatchException("update document property");
            }

            documentLock.lock();
            collectionLock.lock();
        try {
            fileService.incrementDocumentVersion(documentData);

            JsonNode jsonValue = mapper.valueToTree(newValue);
            documentData.set(propertyName, jsonValue);
            File collectionFile = fileService.getCollectionFile(databaseName, collectionName);
            boolean writeStatus = fileService.writeJsonArrayFile(collectionFile.toPath(), jsonArray);
            if (!writeStatus) {
                throw new OperationFailedException("update document");
            }
            if (documentData.has(propertyName)) {
                indexManager.deleteFromPropertyIndex(databaseName, collectionName, propertyName, documentData.get(propertyName).asText());
            }
            indexManager.insertIntoPropertyIndex(databaseName, collectionName, propertyName, newValue.toString(), documentId);

            collectionLock.unlock();
            documentLock.unlock();

            log.info("Document with id " + documentId + " updated successfully in " + collectionName, HttpStatus.ACCEPTED);
        } catch (Exception e) {
            log.error("Error updating document property: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
            throw new OperationFailedException("update document property");
        }finally {
            collectionLock.unlock();
            documentLock.unlock();
        }
    }
    private int getDocumentIndex(String databaseName, String collectionName, String documentId) throws ResourceNotFoundException {
        return IndexingManager.getInstance().searchInCollectionIndex(databaseName, collectionName, documentId);
    }

    public ObjectNode fetchNodeById(String databaseName, String collectionName, String documentId) throws ResourceNotFoundException {
        ArrayNode jsonArray = fileService.readJsonArrayFile(fileService.getCollectionFile(databaseName, collectionName));
        int index = getDocumentIndex(databaseName, collectionName, documentId);
        if (index < 0) {
            return null;
        }
        return (ObjectNode) jsonArray.get(index);
    }

    public Document fetchDocumentFromDatabase(String databaseName, String collectionName, String documentId) throws ResourceNotFoundException {
        ObjectNode jsonObject = fetchNodeById(databaseName, collectionName, documentId);
        if (jsonObject == null) {
            return null;
        }
        return new Document(jsonObject);
    }
}
