package org.example.dbnode.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Enum.Role;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Iterator;
@Log4j2
@Service
public class UserService {

    private final FileService fileService;
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public UserService(FileService fileService) {
        this.fileService = fileService;
    }

    public void addUser(String username, String password) throws OperationFailedException, ResourceAlreadyExistsException {
        File usersFile = fileService.getUsersFile();
        if (!usersFile.exists()) {
            fileService.writeJsonArrayFile(usersFile.toPath(), mapper.createArrayNode());
        }
        ArrayNode usersArray = fileService.readJsonArrayFile(usersFile);
        if (usersArray == null) {
            throw new OperationFailedException("read the file in database " + usersFile);
        }
        for (Object userObj : usersArray) {
            ObjectNode user = (ObjectNode) userObj;
            if (user.get("username").asText().equals(username)) {
                throw new ResourceAlreadyExistsException("user");
            }
        }
        User user = new User(username, password, Role.USER);
        usersArray.add(user.toJson());
        fileService.writeJsonArrayFile(usersFile.toPath(), usersArray);
        log.info("User added successfully with username: " + username);
    }


    public void deleteUser(String username) throws OperationFailedException, ResourceNotFoundException {
        File usersFile = fileService.getUsersFile();
        if (!usersFile.exists()) {
            throw new OperationFailedException("read the file in database " + usersFile);
        }
        ArrayNode usersArray = fileService.readJsonArrayFile(usersFile);
        if (usersArray == null) {
            throw new OperationFailedException("read the file in database " + usersFile);
        }
        boolean found = false;
        Iterator<JsonNode> iterator = usersArray.elements();
        while (iterator.hasNext()) {
            ObjectNode user = (ObjectNode) iterator.next();
            if (user.get("username").asText().equals(username)) {
                iterator.remove();
                found = true;
                break;
            }
        }
        if (!found) {
            throw new ResourceNotFoundException("User");
        }
        fileService.writeJsonArrayFile(usersFile.toPath(), usersArray);
        log.info("User with username (" + username + ") deleted successfully");
    }

    @SuppressWarnings("unchecked")
    public void addAdmin(String username, String password) throws OperationFailedException, ResourceAlreadyExistsException {
        File adminsFile = fileService.getAdminsFile();
        ArrayNode adminsArray;
        if (!adminsFile.exists()) {
            adminsArray = mapper.createArrayNode();
            fileService.writeJsonArrayFile(adminsFile.toPath(), adminsArray);
        } else {
            adminsArray = fileService.readJsonArrayFile(adminsFile);
            if (adminsArray == null) {
                throw new OperationFailedException("Error reading the file. in database " + adminsFile);
            }
        }
        for (JsonNode adminObj : adminsArray) {
            ObjectNode admin = (ObjectNode) adminObj;
            if (admin.get("username").asText().equals(username)) {
                log.error("Admin already exists with username: " + username);
                throw new ResourceAlreadyExistsException("admin");
            }
        }
        User admin = new User(username, password, Role.ADMIN);
        adminsArray.add(admin.toJson());
        fileService.writeJsonArrayFile(adminsFile.toPath(), adminsArray);
        log.info("Admin added successfully with username: " + username);
    }
}