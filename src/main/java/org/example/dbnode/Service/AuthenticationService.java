package org.example.dbnode.Service;

import com.fasterxml.jackson.databind.node.ArrayNode;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.example.dbnode.Model.User;
import org.example.dbnode.Service.FileService;
import org.example.dbnode.Service.Interface.IAuthenticationService;
import org.example.dbnode.Util.PasswordHashing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Optional;
import java.util.function.Function;

@Service
public class AuthenticationService implements IAuthenticationService {
    
    private final FileService fileService;

    @Autowired
    public AuthenticationService(FileService fileService) {
        this.fileService = fileService;
    }

    public boolean authenticateAdmin(String username, String password) {
        if (username.equals("admin") && password.equals("admin")) return true;
        return authenticate(username, password, fileService::getAdminByUsername);
    }
    public boolean authenticateUser(String username, String password) {
        return authenticate(username, password, fileService::getUserByUsername);
    }
    private boolean authenticate(String username, String password, Function<String, Optional<User>> getUserByUsername) {
        if (username == null || password == null) {
            throw new IllegalArgumentException("username or password is null");
        }
        Optional<User> userOptional = getUserByUsername.apply(username);
        if (userOptional.isEmpty()) {
            return false;
        }
        User user = userOptional.get();
        String fileUsername = user.getUsername();
        String hashedPassword = PasswordHashing.hashPassword(password);
        String storedPassword = user.getPassword();
        return fileUsername.equals(username) && storedPassword.equals(hashedPassword);
    }
}
