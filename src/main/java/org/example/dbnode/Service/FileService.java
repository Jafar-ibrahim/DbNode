package org.example.dbnode.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.FileUtils;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Indexing.CollectionIndex;
import org.springframework.stereotype.Service;
import org.example.dbnode.Indexing.PropertyIndex;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

@Service
@Log4j2
public final class FileService {
    private static final String ROOT_PATH = "src/main/resources/databases";

    // To apply synchronization when needed in the first couple of requests
    // instead of synchronizing every request
    private static class Loader {
        static final FileService instance = new FileService();
    }
    public static FileService getInstance(){
        return Loader.instance;
    }

    public synchronized void createDirectoryIfNotExist(Path path){
        try {
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }
        } catch (IOException e) {
            System.err.println("Failed to create " + path.getFileName());
            e.printStackTrace();
        }
    }
    public <T extends JsonNode> void  writePrettyJson(File file, T jsonContent) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
        writer.writeValue(file, jsonContent);
    }
    public void writeToFile(File file, String content) throws IOException {
        Files.write(Paths.get(file.toURI()),content.getBytes());
    }
    public File getDatabaseDirectory(String dbName) {
        return new File(ROOT_PATH + "/" + dbName);
    }
    public File getSchemasPath(String databaseName) {
        return new File(getDatabaseDirectory(databaseName) + "/schemas/");
    }
    public File getCollectionsPath(String databaseName) {
        return new File(getDatabaseDirectory(databaseName) + "/collections/");
    }
    public File getRootPath(){
        return new File(ROOT_PATH);
    }
    public boolean fileExists(String filePath){
        Path path = Paths.get(filePath);
        return Files.exists(path);
    }
    public File getSchemaFilePath(String databaseName,String collectionName) {
        return new File(getDatabaseDirectory(databaseName) + "/schemas/" + collectionName+ "Schema" + ".json");
    }
    public File getRootIndexesDirectory(String databaseName) {
        return new File(getDatabaseDirectory(databaseName) + "/indexes/");
    }
    public File getCollectionIndexesDirectory(String databaseName, String collectionName){
        return new File(getDatabaseDirectory(databaseName) + "/indexes/" + collectionName);
    }
    public File getCollectionIndexFile(String databaseName, String collectionName) {
        return new File(getCollectionIndexesDirectory(databaseName,collectionName) + "/" + collectionName + "_collection_index.txt");
    }
    public File getPropertyIndexFile(String databaseName, String collectionName,String propertyName) {
        return new File(getCollectionIndexesDirectory(databaseName,collectionName) + "/" + propertyName + "_property_index.txt");
    }
    public File getCollectionFile(String databaseName,String collectionName) {
        return new File(getCollectionsPath(databaseName) + "/" + collectionName + ".json");
    }
    public void deleteDirectory(File directory) throws IOException {
        FileUtils.deleteDirectory(directory);
    }
    public boolean deleteFile(File file){
        return file.delete();
    }
    public void deleteRecursively(Path path) throws IOException {
        if (Files.exists(path)) {
            if (Files.isDirectory(path)) {
                try(Stream<Path> pathStream = Files.walk(path)) {
                    pathStream.sorted(Comparator.reverseOrder()) // to delete children first
                            .forEach(p -> {
                                try {
                                    deleteRecursively(p);
                                } catch (IOException e) {
                                    log.error("Recursive deletion failed");
                                }
                            });
                }
            }
            Files.delete(path);
        }
    }

    public void rewriteIndexFile(String databaseName, String collectionName, CollectionIndex collectionIndex) {
        File file = getCollectionIndexFile(databaseName, collectionName);
        List<Map.Entry<String, Integer>> allEntries = collectionIndex.getBPlusTree().getAllEntries();

        if (!allEntries.isEmpty()) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                for (Map.Entry<String, Integer> entry : allEntries) {
                    writer.write(entry.getKey() + "," + entry.getValue());
                    writer.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
                log.error("Failed to rewrite index file");
            }
        } else if (fileExists(file.getPath())) {
            file.delete();
        }
    }
    public void rewritePropertyIndexFile(File propertyIndexFile, PropertyIndex propertyIndex) {
        try {
            List<Map.Entry<String, String>> allEntries = propertyIndex.getBPlusTree().getAllEntries();
            if (allEntries.isEmpty()) {
                if (fileExists(propertyIndexFile.getPath())) {
                    propertyIndexFile.delete();
                    File parentDir = propertyIndexFile.getParentFile();
                    if (directoryNotExist(parentDir) && Objects.requireNonNull(parentDir.list()).length == 0) {
                        log.info("indexes directory is empty, deleting it..........");
                        deleteRecursively(propertyIndexFile.toPath());
                    }
                }
                return;
            }
            propertyIndexFile.createNewFile();
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(propertyIndexFile))) {
                for (Map.Entry<String, String> entry : allEntries) {
                    writer.write(entry.getKey() + "," + entry.getValue());
                    writer.newLine();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public Map<String, Integer> readCollectionIndexFile(File indexFile) {
        Map<String, Integer> indexData = new HashMap<>();
        try (Scanner scanner = new Scanner(indexFile)) {
            while (scanner.hasNextLine()) {
                String[] parts = scanner.nextLine().split(",", 2);
                if (parts.length >= 2) {
                    indexData.put(parts[0].trim(), Integer.parseInt(parts[1].trim()));
                }
            }
        } catch (IOException e) {
            log.error("Error reading index file.", e);
        }
        return indexData;
    }
    public Map<String, String> readPropertyIndexFile(File indexFile) {
        Map<String, String> indexData = new HashMap<>();
        try (Scanner scanner = new Scanner(indexFile)) {
            while (scanner.hasNextLine()) {
                String[] parts = scanner.nextLine().split(",", 2);
                if (parts.length >= 2) {
                    indexData.put(parts[0].trim(), parts[1].trim());
                }
            }
        } catch (IOException e) {
            log.error("Error reading property index file.", e);
        }
        return indexData;
    }
    public ArrayNode readJsonArrayFile(File file) {
        ObjectMapper mapper = new ObjectMapper();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            if (file.length() == 0) {
                return mapper.createArrayNode();
            }
            return (ArrayNode) mapper.readTree(reader);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Error while reading JSON file: " + e.getMessage());
            return null;
        }
    }

    public void rewriteCollectionFile(File collectionFile, ArrayNode jsonArray) {
        ObjectMapper mapper = new ObjectMapper();
        try (RandomAccessFile file = new RandomAccessFile(collectionFile, "rw")) {
            file.setLength(0);
            mapper.writerWithDefaultPrettyPrinter().writeValue(file, jsonArray);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Failed to rewrite collection file");
        }
    }
    public void writeDocumentToCollection(File collectionFile, ObjectNode document) throws ResourceNotFoundException, OperationFailedException {
        ObjectMapper mapper = new ObjectMapper();
        try (RandomAccessFile file = new RandomAccessFile(collectionFile, "rw")) {
            long endPosition = file.length() - 1;
            file.seek(endPosition);
            if (collectionFile.length() > 2) {
                file.seek(endPosition - 1);
                file.writeBytes(",");
            }
            String documentDataString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(document);
            // Add indentation to the document data string
            String indentedDocumentDataString = "\n\t" + documentDataString.replace("\n", "\n\t");
            file.writeBytes(indentedDocumentDataString);
            file.writeBytes("\n]");
        } catch (FileNotFoundException e) {
            throw new ResourceNotFoundException("Collection file");
        } catch (IOException e) {
            throw new OperationFailedException("write document to collection file");
        }
    }

    public boolean writeJsonArrayFile(Path filePath, ArrayNode jsonArray) {
        try {
            Files.createDirectories(filePath.getParent());
            ObjectMapper mapper = new ObjectMapper();
            Files.writeString(filePath, mapper.writeValueAsString(jsonArray));
            return true;
        } catch (IOException e) {
            log.error("Error while writing JSON file: " + e.getMessage());
            return false;
        }
    }
    public boolean isPropertyIndexFile(String fileName) {
        return fileName.endsWith("_property_index.txt");
    }
    public boolean isCollectionIndexFile(String fileName) {
        return fileName.endsWith("_collection_index.txt");
    }

    public void createRootIndexDirectory(String databaseName) {
        createDirectoryIfNotExist(new File(getDatabaseDirectory(databaseName) +"/indexes/").toPath());
    }
    public void createCollectionIndexDirectory(String databaseName, String collectionName) {
        createRootIndexDirectory(databaseName);
        createDirectoryIfNotExist(new File(getDatabaseDirectory(databaseName) + "/indexes/" + collectionName).toPath());
    }
    public void createPropertyIndexDirectory(String databaseName, String collectionName, String propertyName) {
        createCollectionIndexDirectory(databaseName, collectionName);
        createDirectoryIfNotExist(new File(getDatabaseDirectory(databaseName) + "/indexes/" + collectionName + "/" + propertyName).toPath());
    }
    public void createCollectionIndexFile(String databaseName, String collectionName) {
        createCollectionIndexDirectory(databaseName, collectionName);
        File file = getCollectionIndexFile(databaseName, collectionName);
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Failed to create collection index file");
        }
    }
    public void createPropertyIndexFile(String databaseName, String collectionName, String propertyName) {
        createCollectionIndexDirectory(databaseName, collectionName);
        File file = getPropertyIndexFile(databaseName, collectionName, propertyName);
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Failed to create property index file");
        }
    }
    public void appendToIndexFile(String path, Object key, String value) {
        try {
            Path filePath = Paths.get(path);
            Files.createDirectories(filePath.getParent());
            List<String> lines = Files.readAllLines(filePath);
            lines.removeIf(line -> line.startsWith(key.toString() + ","));
            lines.add(key + "," + value);
            Files.write(filePath, lines);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Failed to append to index file");
        }
    }
    public ObjectNode stampVersionOnDocument(ObjectNode document) {
        ObjectNode newDocument = JsonNodeFactory.instance.objectNode();
        newDocument.put("_version", 0);
        document.fields().forEachRemaining(field -> {
            if (!field.getKey().equals("_version")) {
                newDocument.put(field.getKey(), field.getValue());
            }
        });
        return newDocument;
    }
    public void incrementDocumentVersion(ObjectNode document) {
        document.put("_version", document.get("_version").asLong() + 1);
    }

    public ObjectNode assignIdForDocument(ObjectNode document) {
        ObjectNode newDocument = JsonNodeFactory.instance.objectNode();
        String id = document.has("_id") ? document.get("_id").asText() : UUID.randomUUID().toString();
        newDocument.put("_id", id);
        document.fields().forEachRemaining(field -> {
            if (!field.getKey().equals("_id")) {
                newDocument.set(field.getKey(), field.getValue());
            }
        });
        return newDocument;
    }

    public boolean directoryNotExist(File file) {
        return !file.isDirectory();
    }
    public static boolean invalidResourceName(String name){
        return name == null || name.trim().isEmpty() ||
               name.contains(" ") || name.contains("/") ||
               name.contains("\\");
    }
}
