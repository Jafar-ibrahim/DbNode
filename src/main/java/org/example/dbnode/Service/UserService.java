package org.example.dbnode.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Iterator;

@Service
public class UserService {

    private final FileService fileService;
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public UserService(FileService fileService) {
        this.fileService = fileService;
    }

    public void addUser(String accountNumber, String password) throws OperationFailedException, ResourceAlreadyExistsException {
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
            if (user.get("accountNumber").asText().equals(accountNumber)) {
                throw new ResourceAlreadyExistsException("user");
            }
        }
        ObjectNode user = mapper.createObjectNode();
        user.put("accountNumber", accountNumber);
        user.put("password", password); //hashing the password
        usersArray.add(user);
        fileService.writeJsonArrayFile(usersFile.toPath(), usersArray);
    }


    public void deleteUser(String accountNumber) throws OperationFailedException, ResourceNotFoundException {
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
            if (user.get("accountNumber").asText().equals(accountNumber)) {
                iterator.remove();
                found = true;
                break;
            }
        }
        if (!found) {
            throw new ResourceNotFoundException("User");
        }
        fileService.writeJsonArrayFile(usersFile.toPath(), usersArray);
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
                throw new ResourceAlreadyExistsException("admin");
            }
        }
        ObjectNode newAdmin = mapper.createObjectNode();
        newAdmin.put("username", username);
        newAdmin.put("password", password);
        adminsArray.add(newAdmin);
        fileService.writeJsonArrayFile(adminsFile.toPath(), adminsArray);
    }
}