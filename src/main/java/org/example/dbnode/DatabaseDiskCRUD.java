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
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
@Component
public class DatabaseDiskCRUD {

    private final FileService fileService;
    private final IndexingManager indexingManager;
    private final LocksManager locksManager;

    private static final class InstanceHolder {
        private static final DatabaseDiskCRUD instance = new DatabaseDiskCRUD();
    }
    public static DatabaseDiskCRUD getInstance() {
        return DatabaseDiskCRUD.InstanceHolder.instance;
    }
    public DatabaseDiskCRUD() {
        indexingManager = IndexingManager.getInstance();
        locksManager = LocksManager.getInstance();
        fileService = FileService.getInstance();
    }

    public void createDatabase(String databaseName) throws ResourceAlreadyExistsException, ResourceNotFoundException {
        if (fileService.invalidResourceName(databaseName)){
            throw new InvalidResourceNameException("Database");
        }
        ReentrantLock databaseLock = locksManager.getDatabaseLock(databaseName);
        databaseLock.lock();
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
        } catch (Exception e) {
            // If an exception is thrown during document creation, remove the lock
            locksManager.deleteDatabaseLock(databaseName);
            throw e;
        }finally {
            databaseLock.unlock();
        }
    }

    public void deleteDatabase(String databaseName) throws ResourceNotFoundException, IOException {
        ReentrantLock databaseLock = locksManager.getDatabaseLock(databaseName);
        databaseLock.lock();
        try{
            File dbDirectory = fileService.getDatabaseDirectory(databaseName);
            if (fileService.directoryNotExist(dbDirectory)) {
                log.error("Database deletion failed : database does not exist");
                throw new ResourceNotFoundException("Database");
            }
            fileService.deleteDirectory(dbDirectory);
            locksManager.deleteDatabaseLock(databaseName);
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

    public void createCollectionFromJsonSchema(String databaseName, String collectionName, JsonNode jsonSchema) throws ResourceAlreadyExistsException, IOException, ResourceNotFoundException {
        File schemaFile = createCollection(databaseName, collectionName);
        // Write schema on disk
        fileService.writePrettyJson(schemaFile, jsonSchema);
        log.info("Collection created successfully.");
    }

    public void createCollectionFromClass(String databaseName, String collectionName, Class<?> clazz) throws ResourceAlreadyExistsException, IOException, ResourceNotFoundException {
        File schemaFile = createCollection(databaseName, collectionName);
        JsonNode schema = Schema.fromClass(clazz);
        // Write schema on disk
        fileService.writePrettyJson(schemaFile, schema);
        log.info("Collection created successfully.");
    }

    private File createCollection(String databaseName, String collectionName) throws ResourceNotFoundException, ResourceAlreadyExistsException, IOException {
        ReentrantLock collectionLock = locksManager.getCollectionLock(databaseName, collectionName);
        collectionLock.lock();
        try {
            if (fileService.invalidResourceName(collectionName)) {
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
        } catch (Exception e) {
            // If an exception is thrown during creation, remove the lock
            locksManager.deleteCollectionLock(databaseName, collectionName);
            throw e;
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
            boolean indexesDeletionFailed = !indexingManager.deleteCollectionIndex(databaseName, collectionName);
            if (collectionDeletionFailed || schemaDeletionFailed || indexesDeletionFailed){
                throw new OperationFailedException("delete collection, associated schema, account directory entries, or index");
            }
            log.info("Collection, its schema, associated account directory entries, and indexes have been successfully deleted");
        }finally {
            collectionLock.unlock();
        }

    }

    public List<String> readCollections(String databaseName) {
        File dbDirectory = fileService.getCollectionsPath(databaseName);
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

    public Document createDocument(String databaseName, String collectionName, Document document , String documentId) throws IOException, OperationFailedException, ResourceNotFoundException {
        ReentrantLock collectionLock;
        ReentrantLock documentLock;

        collectionLock = locksManager.getCollectionLock(databaseName, collectionName);
        documentLock = locksManager.getDocumentLock(databaseName, collectionName, documentId);

        collectionLock.lock();
        documentLock.lock();
        try {
            ObjectMapper mapper = new ObjectMapper();
            File collectionFile = fileService.getCollectionFile(databaseName, collectionName);
            ArrayNode collectionDocs = (ArrayNode) mapper.readTree(collectionFile);
            if (collectionDocs == null) {
                locksManager.deleteDocumentLock(databaseName, collectionName, documentId);
                throw new OperationFailedException("read the existing collection");
            }
            ObjectNode documentData = document.getContent();
            documentData = fileService.stampVersionOnDocument(documentData);
            documentData = fileService.assignIdForDocument(documentData,documentId);
            document.setContent(documentData);

            fileService.writeDocumentToCollection(collectionFile, documentData);
            // Index the document
            indexingManager.insertDocumentIntoCollectionIndex(databaseName, collectionName, document.getId());
            // Index the properties
            final ObjectNode finalDocumentData = documentData;
            finalDocumentData.fieldNames().forEachRemaining(fieldName -> {
                // Exclude _id and _version from indexing
                if (!fieldName.equals("_id") && !fieldName.equals("_version")) {
                    JsonNode fieldValue = finalDocumentData.get(fieldName);
                    indexingManager.insertIntoPropertyIndex(databaseName, collectionName, fieldName, fieldValue.toString(), document.getId());
                }
            });

            log.info("Document with id " + document.getId() + " added to collection " + collectionName);
            return document;
        } catch (Exception e) {
            // If an exception is thrown during document creation, remove the lock
            locksManager.deleteDocumentLock(databaseName, collectionName, documentId);
            throw e;
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
            indexingManager.deleteDocumentRelatedIndexes(databaseName, collectionName, documentId);
        }finally {
            collectionLock.unlock();
            documentLock.unlock();
        }
        locksManager.deleteDocumentLock(databaseName, collectionName, documentId);
        log.info("Deleted document and its related indexes from collection successfully");
    }

    public void updateDocument(String databaseName,
                               String collectionName,
                               String documentId,
                               ObjectNode updatedProperties) throws OperationFailedException, ResourceNotFoundException, VersionMismatchException {

        Document document;
        Optional<Document> documentOptional = fetchDocumentFromDatabase(databaseName, collectionName, documentId);
        if (documentOptional.isEmpty()) {
            throw new ResourceNotFoundException("Document with ID " + documentId + " in collection " + collectionName);
        } else {
            document = documentOptional.get();
        }

        ReentrantLock collectionLock = locksManager.getCollectionLock(databaseName, collectionName),
                documentLock = locksManager.getDocumentLock(databaseName, collectionName, documentId);

        ObjectMapper mapper = new ObjectMapper();
        ArrayNode jsonArray = fileService.getCollectionDocuments(databaseName, collectionName);

        IndexingManager indexManager = IndexingManager.getInstance();
        int index = getDocumentIndex(databaseName, collectionName, documentId);
        ObjectNode documentData = (ObjectNode) jsonArray.get(index);

        Long expectedVersion = updatedProperties.get("_version").asLong();
        if (documentData.has("_version") && !Objects.equals(document.getVersion(), expectedVersion)) {
            throw new VersionMismatchException();
        }

        documentLock.lock();
        collectionLock.lock();
        try {
            fileService.incrementDocumentVersion(documentData);

            Iterator<Map.Entry<String, JsonNode>> fields = updatedProperties.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String propertyName = field.getKey();

                // For security measures, do not allow updating _id and _version fields
                if (propertyName.equals("_id") || propertyName.equals("_version")) {
                    continue;
                }

                String newValue = field.getValue().asText();

                documentData.set(propertyName, field.getValue());
                if (documentData.has(propertyName)) {
                    indexManager.deleteFromPropertyIndex(databaseName, collectionName, propertyName, documentData.get(propertyName).asText());
                }
                indexManager.insertIntoPropertyIndex(databaseName, collectionName, propertyName, newValue, documentId);
            }

            File collectionFile = fileService.getCollectionFile(databaseName, collectionName);
            boolean writeStatus = fileService.writeJsonArrayFile(collectionFile.toPath(), jsonArray);
            if (!writeStatus) {
                throw new OperationFailedException("update document");
            }

            log.info("Document with id " + documentId + " updated successfully in " + collectionName, HttpStatus.ACCEPTED);
        } catch (Exception e) {
            log.error("Error updating document property: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
            throw new OperationFailedException("update document property");
        } finally {
            collectionLock.unlock();
            documentLock.unlock();
        }
    }
    private int getDocumentIndex(String databaseName, String collectionName, String documentId) throws ResourceNotFoundException {
        return IndexingManager.getInstance().searchInCollectionIndex(databaseName, collectionName, documentId);
    }

    public String readDocumentProperty(String databaseName, String collectionName, String documentId, String propertyName) throws ResourceNotFoundException {
        return indexingManager.searchInPropertyIndex(databaseName, collectionName, propertyName, documentId);
    }

    public ObjectNode fetchNodeById(String databaseName, String collectionName, String documentId){
        ArrayNode jsonArray = fileService.readJsonArrayFile(fileService.getCollectionFile(databaseName, collectionName));
        int index;
        try {
            index = getDocumentIndex(databaseName, collectionName, documentId);
        } catch (ResourceNotFoundException e) {
            log.error("Document with ID " + documentId + " not found in collection " + collectionName);
            return null;
        }
        return (ObjectNode) jsonArray.get(index);
    }

    public Optional<Document> fetchDocumentFromDatabase(String databaseName, String collectionName, String documentId){
        ObjectNode jsonObject = fetchNodeById(databaseName, collectionName, documentId);
        if (jsonObject == null) {
            return Optional.empty();
        }
        return Optional.of(new Document(jsonObject));
    }
    public Schema getCollectionSchema(String databaseName, String collectionName) throws ResourceNotFoundException {
        File schemaFile = fileService.getSchemaFilePath(databaseName, collectionName);
        ObjectMapper mapper = new ObjectMapper();
        try {
            if (fileService.fileExists(schemaFile.getPath())) {
                return Schema.of(String.valueOf(mapper.readTree(schemaFile)));
            } else {
                log.error("Schema file does not exist");
                throw new ResourceNotFoundException("Schema file");
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Failed to read schema file");
            return null;
        }
    }
    public List<JsonNode> fetchAllDocumentsFromCollection(String databaseName, String collectionName) throws ResourceNotFoundException {
        ArrayNode jsonArray = fileService.getCollectionDocuments(databaseName, collectionName);
        List<JsonNode> documents = new ArrayList<>();
        for (JsonNode jsonNode : jsonArray) {
            documents.add(jsonNode);
        }
        return documents;
    }
}
