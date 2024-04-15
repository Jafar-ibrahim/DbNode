package org.example.dbnode.Controller;

import com.fasterxml.jackson.databind.JsonNode;
import org.example.dbnode.Broadcast.Broadcaster;
import org.example.dbnode.Broadcast.Request;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceAlreadyExistsException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Service.AuthenticationService;
import org.example.dbnode.Service.CollectionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
@EnableMethodSecurity(securedEnabled = true)
@RestController
@RequestMapping("/api/databases/{db_name}/collections")
@SuppressWarnings("unused")
public class CollectionController {

    private final AuthenticationService authenticationService;
    private final CollectionService collectionService;

    @Autowired
    public CollectionController(AuthenticationService authenticationService, CollectionService collectionService){
        this.authenticationService = authenticationService;
        this.collectionService = collectionService;
    }

    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @PostMapping("/{collection_name}")
    public ResponseEntity<String> createCollection(@PathVariable("db_name") String dbName,
                                                   @PathVariable("collection_name") String collectionName,
                                                   @RequestParam("schema") JsonNode schema,
                                                   @RequestHeader(value = "X-Broadcast", required = false, defaultValue = "false") Boolean isBroadcasted,
                                                   @RequestHeader("username") String username,
                                                   @RequestHeader("password") String password) throws ResourceAlreadyExistsException, IOException, ResourceNotFoundException {

        collectionService.createCollection(dbName, collectionName, schema);
        if (!isBroadcasted){
            Broadcaster.broadcast(
                    new Request()
                            .setMethod(HttpMethod.POST)
                            .addAuthHeaders(username, password)
                            .setUrl("http://nodeNODE_ID:9000/api/databases/"+dbName+"/collections/"+collectionName));
        }
        return new ResponseEntity<>("Collection created successfully", HttpStatus.CREATED);
    }
    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @DeleteMapping("/{collection_name}")
    public ResponseEntity<String> deleteCollection(@PathVariable("db_name") String dbName,
                                                   @PathVariable("collection_name") String collectionName,
                                                   @RequestHeader(value = "X-Broadcast", required = false, defaultValue = "false") Boolean isBroadcasted,
                                                   @RequestHeader("username") String username,
                                                   @RequestHeader("password") String password) throws OperationFailedException, IOException, ResourceNotFoundException {


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
    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @GetMapping
    public ResponseEntity<List<String>> fetchExistingCollections(
            @PathVariable("db_name") String dbName,
            @RequestHeader("username") String username,
            @RequestHeader("password") String password) {

        List<String> collections = collectionService.readCollections(dbName);
        return new ResponseEntity<>(collections, HttpStatus.OK);
    }
}