package org.example.dbnode.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

@Service
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
    public File getIndexesFile(String databaseName,String collectionName){
        return new File(getDatabaseDirectory(databaseName) + "/indexes/" + collectionName + "_indexes");
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
                                    System.err.println("Recursive deletion failed");
                                }
                            });
                }
            }
            Files.delete(path);
        }
    }
    public boolean directoryNotExist(File file) {
        return !file.isDirectory();
    }
    public static boolean invalidResourceName(String name){
        return name == null || name.trim().isEmpty();
    }
}
