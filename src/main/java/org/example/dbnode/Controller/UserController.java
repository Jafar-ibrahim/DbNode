package org.example.dbnode.Controller;

import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Service.AuthenticationService;
import org.example.dbnode.Service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class UserController {

    private final AuthenticationService authenticationService;
    private final UserService userService;

    @Autowired
    public UserController(AuthenticationService authenticationService, UserService userService){
        this.authenticationService = authenticationService;
        this.userService = userService;
    }

    @PostMapping("/users")
    public ResponseEntity<String> addUser(@RequestParam("username") String username,
                                              @RequestParam("password") String password,
                                              @RequestHeader("adminUsername") String adminUsername,
                                              @RequestHeader("adminPassword") String adminPassword) throws OperationFailedException, ResourceAlreadyExistsException {
        if(!authenticationService.authenticateAdmin(adminUsername, adminPassword)){
            return new ResponseEntity<>("User is not authorized", HttpStatus.UNAUTHORIZED);
        }
        userService.addUser(username, password);
        return new ResponseEntity<>("User added successfully", HttpStatus.CREATED);
    }

    @DeleteMapping ("/users")
    public ResponseEntity<String> deleteUser(@RequestParam("username") String username,
                                                 @RequestHeader("adminUsername") String adminUsername,
                                                 @RequestHeader("adminPassword") String adminPassword) throws OperationFailedException, ResourceNotFoundException {
        if(!authenticationService.authenticateAdmin(adminUsername, adminPassword)){
            return new ResponseEntity<>("User is not authorized", HttpStatus.UNAUTHORIZED);
        }
        userService.deleteUser(username);
        return new ResponseEntity<>("User deleted successfully", HttpStatus.OK);
    }

    @PostMapping("/admins")
    public ResponseEntity<String> addAdmin(@RequestParam("username") String username,
                                           @RequestParam("password") String password) throws OperationFailedException, ResourceAlreadyExistsException {
        userService.addAdmin(username, password);
        return new ResponseEntity<>("Admin added successfully", HttpStatus.CREATED);
    }
}