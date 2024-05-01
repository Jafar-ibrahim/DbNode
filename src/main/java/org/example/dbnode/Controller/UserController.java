package org.example.dbnode.Controller;

import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Enum.Role;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Service.AuthenticationServiceImpl;
import org.example.dbnode.Service.Interfaces.AuthenticationService;
import org.example.dbnode.Service.Interfaces.UserService;
import org.example.dbnode.Service.UserServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
@Log4j2
@RestController
@RequestMapping("/api/users")
public class UserController {

    private final AuthenticationService authenticationService;
    private final UserService userService;

    @Autowired
    public UserController(AuthenticationServiceImpl authenticationService, UserServiceImpl userService){
        this.authenticationService = authenticationService;
        this.userService = userService;
    }
    @PreAuthorize("@authenticationServiceImpl.authenticateAdmin(#adminUsername, #adminPassword)")
    @PostMapping("/{username}")
    public ResponseEntity<String> addUser(@PathVariable("username") String username,
                                          @RequestHeader("password") String password,
                                          @RequestHeader("role") Role role,
                                          @RequestHeader("adminUsername") String adminUsername,
                                          @RequestHeader("adminPassword") String adminPassword,
                                          @RequestHeader(value = "token" ,required = false) String token ) throws OperationFailedException, ResourceAlreadyExistsException {

        log.info("Received request to register a new user with username: " + username+" and role: "+role);

        if (role == Role.ADMIN ) {
            userService.addAdmin(username, password);
        }else {
            userService.addUser(username, password);
        }
        return new ResponseEntity<>("User added successfully with username: " + username+" and role: "+role, HttpStatus.CREATED);
    }

    @PreAuthorize("@authenticationServiceImpl.authenticateAdmin(#adminUsername, #adminPassword)")
    @DeleteMapping ("/{username}")
    public ResponseEntity<String> deleteUser(@PathVariable("username") String username,
                                                 @RequestHeader("adminUsername") String adminUsername,
                                                 @RequestHeader("adminPassword") String adminPassword) throws OperationFailedException, ResourceNotFoundException {
        log.info("Received request to delete the user with username: " + username);
        userService.deleteUser(username);
        return new ResponseEntity<>("User with username ("+username+") deleted successfully", HttpStatus.OK);
    }
}