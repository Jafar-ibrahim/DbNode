package org.example.dbnode.Controller;

import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Broadcast.Broadcaster;
import org.example.dbnode.Exception.VersionMismatchException;
import org.example.dbnode.Model.Request;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Service.AuthenticationServiceImpl;
import org.example.dbnode.Service.DatabaseServiceImpl;
import org.example.dbnode.Service.Interfaces.AuthenticationService;
import org.example.dbnode.Service.Interfaces.DatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
@Log4j2
@EnableMethodSecurity(securedEnabled = true)
@RestController
@RequestMapping("/api/databases")
@SuppressWarnings("unused")
public class DatabaseController {

    private final AuthenticationService authenticationService;
    private final DatabaseService databaseService;

    @Autowired
    public DatabaseController(AuthenticationServiceImpl authenticationService, DatabaseServiceImpl databaseService){
        this.authenticationService = authenticationService;
        this.databaseService = databaseService;
    }

    @PreAuthorize("@authenticationServiceImpl.authenticateAdmin(#username, #password)")
    @PostMapping("/{db_name}")
    public ResponseEntity<String> createDatabase(@PathVariable("db_name") String dbName,
                                                 @RequestHeader(value = "isBroadcast", required = false, defaultValue = "false") Boolean isBroadcasted,
                                                 @RequestHeader("username") String username,
                                                 @RequestHeader("password") String password) throws ResourceAlreadyExistsException, ResourceNotFoundException, VersionMismatchException {

        String logMessage = "Received request to create database: ("+dbName+")";
        if(isBroadcasted){
            log.info("BROADCAST: "+logMessage);
        }else {
            log.info(logMessage);
        }
        databaseService.createDatabase(dbName);
        if (!isBroadcasted){
            Broadcaster.broadcast(
                    new Request()
                    .setMethod(HttpMethod.POST)
                    .addAuthHeaders(username, password)
                    .setUrl("http://nodeNODE_ID:9000/api/databases/"+dbName));
        }
        return new ResponseEntity<>("Database created successfully", HttpStatus.CREATED);
    }
    @PreAuthorize("@authenticationServiceImpl.authenticateAdmin(#username, #password)")
    @DeleteMapping("/{db_name}")
    public ResponseEntity<String> deleteDatabase(@PathVariable("db_name") String dbName,
                                                 @RequestHeader(value = "isBroadcast", required = false, defaultValue = "false") Boolean isBroadcasted,
                                                 @RequestHeader("username") String username,
                                                 @RequestHeader("password") String password) throws IOException, ResourceNotFoundException {

        String logMessage = "Received request to delete database: ("+dbName+")";
        if(isBroadcasted){
            log.info("BROADCAST: "+logMessage);
        }else {
            log.info(logMessage);
        }
        databaseService.deleteDatabase(dbName);
        if (!isBroadcasted){
            Broadcaster.broadcast(
                    new Request()
                            .setMethod(HttpMethod.DELETE)
                            .addAuthHeaders(username, password)
                            .setUrl("http://nodeNODE_ID:9000/api/databases/"+dbName));
        }
        return new ResponseEntity<>("Database deleted successfully", HttpStatus.OK);
    }

    @PreAuthorize("@authenticationServiceImpl.authenticateAdmin(#username, #password)")
    @GetMapping
    public ResponseEntity<List<String>> fetchExistingDatabases(

            @RequestHeader("username") String username,
            @RequestHeader("password") String password) {

        List<String> databases = databaseService.getAllDatabases();
        return new ResponseEntity<>(databases, HttpStatus.OK);
    }
}