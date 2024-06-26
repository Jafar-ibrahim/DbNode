package org.example.dbnode.Controller;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Broadcast.Broadcaster;
import org.example.dbnode.Model.Request;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Service.AuthenticationServiceImpl;
import org.example.dbnode.Service.CollectionServiceImpl;
import org.example.dbnode.Service.Interfaces.AuthenticationService;
import org.example.dbnode.Service.Interfaces.CollectionService;
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
@RequestMapping("/api/databases/{db_name}/collections")
@SuppressWarnings("unused")
public class CollectionController {

    private final AuthenticationService authenticationService;
    private final CollectionService collectionService;

    @Autowired
    public CollectionController(AuthenticationServiceImpl authenticationService, CollectionServiceImpl collectionService){
        this.authenticationService = authenticationService;
        this.collectionService = collectionService;
    }

    @PreAuthorize("@authenticationServiceImpl.authenticateAdmin(#username, #password)")
    @PostMapping("/{collection_name}")
    public ResponseEntity<String> createCollection(@PathVariable("db_name") String dbName,
                                                   @PathVariable("collection_name") String collectionName,
                                                   @RequestBody JsonNode schema,
                                                   @RequestHeader(value = "isBroadcast", required = false, defaultValue = "false") Boolean isBroadcasted,
                                                   @RequestHeader("username") String username,
                                                   @RequestHeader("password") String password) throws ResourceAlreadyExistsException, IOException, ResourceNotFoundException, OperationFailedException {

        String logMessage = "Received request to create collection: ("+collectionName+") in database: ("+dbName+")";
        if(isBroadcasted){
            log.info("BROADCAST: "+logMessage);
        }else {
            log.info(logMessage);
        }
        collectionService.createCollection(dbName, collectionName, schema);
        if (!isBroadcasted){
            Broadcaster.broadcast(
                    new Request()
                            .setMethod(HttpMethod.POST)
                            .addAuthHeaders(username, password)
                            .setBody(schema)
                            .setUrl("http://nodeNODE_ID:9000/api/databases/"+dbName+"/collections/"+collectionName));
        }
        return new ResponseEntity<>("Collection created successfully", HttpStatus.CREATED);
    }
    @PreAuthorize("@authenticationServiceImpl.authenticateAdmin(#username, #password)")
    @DeleteMapping("/{collection_name}")
    public ResponseEntity<String> deleteCollection(@PathVariable("db_name") String dbName,
                                                   @PathVariable("collection_name") String collectionName,
                                                   @RequestHeader(value = "isBroadcast", required = false, defaultValue = "false") Boolean isBroadcasted,
                                                   @RequestHeader("username") String username,
                                                   @RequestHeader("password") String password) throws OperationFailedException, IOException, ResourceNotFoundException {

        String logMessage = "Received request to delete collection: ("+collectionName+") from database: ("+dbName+")";
        if(isBroadcasted){
            log.info("BROADCAST: "+logMessage);
        }else {
            log.info(logMessage);
        }
        collectionService.deleteCollection(dbName, collectionName);
        if (!isBroadcasted){
            Broadcaster.broadcast(
                    new Request()
                            .setMethod(HttpMethod.DELETE)
                            .addAuthHeaders(username, password)
                            .setUrl("http://nodeNODE_ID:9000/api/databases/"+dbName+"/collections/"+collectionName));
        }
        return new ResponseEntity<>("Collection deleted successfully", HttpStatus.OK);
    }
    @PreAuthorize("@authenticationServiceImpl.authenticateAdmin(#username, #password)")
    @GetMapping
    public ResponseEntity<List<String>> fetchExistingCollections(
            @PathVariable("db_name") String dbName,
            @RequestHeader("username") String username,
            @RequestHeader("password") String password) {

        List<String> collections = collectionService.readCollections(dbName);
        return new ResponseEntity<>(collections, HttpStatus.OK);
    }
}