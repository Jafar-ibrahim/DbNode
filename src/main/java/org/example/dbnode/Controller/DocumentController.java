package org.example.dbnode.Controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Affinity.AffinityBalancer;
import org.example.dbnode.Affinity.RedirectionService;
import org.example.dbnode.Broadcast.Broadcaster;
import org.example.dbnode.Model.Request;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Exception.SchemaMismatchException;
import org.example.dbnode.Exception.VersionMismatchException;
import org.example.dbnode.Model.Document;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dbnode.Model.NodeInfo;
import org.example.dbnode.Service.AuthenticationService;
import org.example.dbnode.Service.DocumentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
@Log4j2
@EnableMethodSecurity(securedEnabled = true)
@SuppressWarnings("unused")
@RestController
@RequestMapping("/api/databases/{db_name}/collections/{collection_name}/documents")
public class DocumentController {

    private final AuthenticationService authenticationService;
    private final DocumentService documentService;

    @Autowired
    public DocumentController(AuthenticationService authenticationService, DocumentService documentService) {
        this.authenticationService = authenticationService;
        this.documentService = documentService;
    }

    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @PostMapping
    public ResponseEntity<String> createDocument(@PathVariable("db_name") String dbName,
                                                 @PathVariable("collection_name") String collectionName,
                                                 @RequestBody ObjectNode documentNode,
                                                 @RequestHeader("username") String username,
                                                 @RequestHeader("password") String password,
                                                 @RequestHeader(value = "isBroadcast", required = false, defaultValue = "false") Boolean isBroadcasted) throws SchemaMismatchException, OperationFailedException, IOException, ResourceNotFoundException {

        if(isBroadcasted){
            log.info("Received broadcast request to create document in collection: ("+collectionName+") in database: ("+dbName+")");
        }

        Optional<String> documentId = Optional.ofNullable(documentNode.get("_id")).map(JsonNode::asText);
        Document documentObj = documentService.createDocument(dbName, collectionName, documentNode,documentId);
        if (!isBroadcasted){
            Broadcaster.broadcast(
                    new Request()
                            .setMethod(HttpMethod.POST)
                            .addAuthHeaders(username, password)
                            .setBody(documentObj.getContent())
                            .setUrl("http://nodeNODE_ID:9000/api/databases/"+dbName+"/collections/"+collectionName+"/documents"));
        }
        log.info("Created document with Id : "+documentObj.getId() +" successfully");
        return new ResponseEntity<>("Created document with Id : "+documentObj.getId() +" successfully", HttpStatus.CREATED);
    }
    
    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @PutMapping("/{doc_id}")
    public ResponseEntity<String> updateDocument(@PathVariable("db_name") String dbName,
                                                 @PathVariable("collection_name") String collectionName,
                                                 @PathVariable("doc_id") String documentId,
                                                 @RequestBody ObjectNode updatedProperties,
                                                 @RequestHeader(value = "isBroadcast", required = false, defaultValue = "false") Boolean isBroadcasted,
                                                 @RequestHeader("username") String username,
                                                 @RequestHeader("password") String password) throws OperationFailedException, ResourceNotFoundException, VersionMismatchException {

        if(isBroadcasted){
            log.info("Received broadcast request to update document with Id : ("+documentId+") in collection: ("+collectionName+") in database: ("+dbName+")");
        }
        int nodeWithAffinity = AffinityBalancer.getInstance().getNodeWithAffinityId(documentId);
        int currentNode = NodeInfo.getInstance().getNodeId();
        boolean hasAffinity = nodeWithAffinity == currentNode;
        Request updateRequest = new Request()
                .setMethod(HttpMethod.PUT)
                .addAuthHeaders(username, password)
                .setBody(updatedProperties)
                .setUrl("http://nodeNODE_ID:9000/api/databases/" + dbName + "/collections/" + collectionName + "/documents/" + documentId);

        if (!hasAffinity && !isBroadcasted){
            log.info("Current node (node "+currentNode+") does not have affinity of the document, " +
                    "redirecting request to node with affinity (node "+nodeWithAffinity+")");
            return RedirectionService.redirect(updateRequest, nodeWithAffinity);
        }
        documentService.updateDocument(dbName, collectionName, documentId,updatedProperties);
        if(!isBroadcasted){
            Broadcaster.broadcast(updateRequest);
        }
        log.info("Updated document with id : "+documentId+" successfully");
        return new ResponseEntity<>("Updated document with id : "+documentId+" successfully", HttpStatus.OK);
    }
    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @DeleteMapping("/{doc_id}")
    public ResponseEntity<String> deleteDocument(@PathVariable("db_name") String dbName,
                                                 @PathVariable("collection_name") String collectionName,
                                                 @PathVariable("doc_id") String documentId,
                                                 @RequestHeader(value = "isBroadcast", required = false, defaultValue = "false") Boolean isBroadcasted,
                                                 @RequestHeader("username") String username,
                                                 @RequestHeader("password") String password) throws OperationFailedException, ResourceNotFoundException {

        if(isBroadcasted){
            log.info("Received broadcast request to delete document from collection: ("+collectionName+") in database: ("+dbName+")");
        }
        int nodeWithAffinity = AffinityBalancer.getInstance().getNodeWithAffinityId(documentId);
        int currentNode = NodeInfo.getInstance().getNodeId();
        boolean hasAffinity = nodeWithAffinity == currentNode;
        Request deleteRequest = new Request()
                .setMethod(HttpMethod.DELETE)
                .addAuthHeaders(username, password)
                .setUrl("http://nodeNODE_ID:9000/api/databases/"+dbName+"/collections/"+collectionName+"/documents/"+documentId);

        if (!hasAffinity && !isBroadcasted){
            log.info("Current node (node "+currentNode+") does not have affinity of the document, " +
                    "redirecting request to node with affinity (node "+nodeWithAffinity+")");
            return RedirectionService.redirect(deleteRequest, nodeWithAffinity);
        }
        documentService.deleteDocument(dbName, collectionName, documentId);
        if(!isBroadcasted){
            Broadcaster.broadcast(deleteRequest);
        }
        log.info("Deleted Document with id : "+documentId+" successfully");
        return new ResponseEntity<>("Deleted Document with id : "+documentId+" successfully", HttpStatus.OK);
    }
    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @GetMapping("/{doc_id}")
    public ResponseEntity<JsonNode> fetchDocumentById(@PathVariable("db_name") String dbName,
                                                      @PathVariable("collection_name") String collectionName,
                                                      @PathVariable("doc_id") String documentId,
                                                      @RequestHeader("username") String username,
                                                      @RequestHeader("password") String password) {

        Optional<Document> documentOptional = documentService.fetchDocument(dbName, collectionName, documentId);
        if (documentOptional.isEmpty()) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        JsonNode document = documentOptional.get().getContent();
        log.info("Fetched document with id: "+documentId+" from collection: "+collectionName+" in database: "+dbName+" successfully");
        return new ResponseEntity<>(document, HttpStatus.OK);
    }
    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @GetMapping
    public ResponseEntity<String> fetchCollectionDocuments(@PathVariable("db_name") String dbName,
                                                           @PathVariable("collection_name") String collectionName,
                                                           @RequestParam(value = "property_name", required = false) String propertyName,
                                                           @RequestParam(value = "property_value", required = false) String propertyValue,
                                                           @RequestHeader("username") String username,
                                                           @RequestHeader("password") String password) throws JsonProcessingException, ResourceNotFoundException {
        List<JsonNode> documents;
        if (propertyName != null) {
            if (propertyValue == null) {
                return new ResponseEntity<>("Property value is required when property name is provided", HttpStatus.BAD_REQUEST);
            }
            documents = documentService.fetchAllDocumentsByPropertyValue(dbName, collectionName, propertyName, propertyValue);
            log.info("Fetched all documents from collection: "+collectionName+" in database: "+dbName+" with property: "+propertyName+" having value: "+propertyValue+" successfully");
        } else {
            documents = documentService.fetchAllDocumentsFromCollection(dbName, collectionName);
            log.info("Fetched all documents from collection: "+collectionName+" in database: "+dbName+" successfully");
        }

        ObjectMapper mapper = new ObjectMapper();
        String prettyDocuments = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(documents);
        return new ResponseEntity<>(prettyDocuments, HttpStatus.OK);
    }
    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @GetMapping("/{doc_id}/{propertyName}")
    public ResponseEntity<String> readDocumentProperty(@PathVariable("db_name") String dbName,
                                                       @PathVariable("collection_name") String collectionName,
                                                       @PathVariable("doc_id") String documentId,
                                                       @PathVariable String propertyName,
                                                       @RequestHeader("username") String username,
                                                       @RequestHeader("password") String password) throws ResourceNotFoundException {
        String propertyValue = documentService.readDocumentProperty(dbName, collectionName, documentId, propertyName);
        log.info("Read property: "+propertyName+" from document with id: "+documentId+" in collection: "+collectionName+" in database: "+dbName+" successfully");
        return new ResponseEntity<>(propertyValue, HttpStatus.OK);
    }

}
