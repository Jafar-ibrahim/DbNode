package org.example.dbnode.Controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.example.dbnode.Affinity.AffinityBalancer;
import org.example.dbnode.Affinity.RedirectionService;
import org.example.dbnode.Broadcast.Broadcaster;
import org.example.dbnode.Broadcast.Request;
import org.example.dbnode.Exception.OperationFailedException;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Exception.SchemaMismatchException;
import org.example.dbnode.Exception.VersionMismatchException;
import org.example.dbnode.Model.Document;
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
                                                 @RequestParam Object document,
                                                 @RequestHeader("username") String username,
                                                 @RequestHeader("password") String password,
                                                 @RequestHeader(value = "X-Broadcast", required = false, defaultValue = "false") Boolean isBroadcasted) throws SchemaMismatchException, OperationFailedException, IOException, ResourceNotFoundException {
        ObjectNode documentNode;
        if (document instanceof String) {
            ObjectMapper mapper = new ObjectMapper();
            documentNode = mapper.readValue((String) document, ObjectNode.class);
        } else if (document instanceof ObjectNode) {
            documentNode = (ObjectNode) document;
        } else {
            throw new IllegalArgumentException("Invalid document type");
        }
        documentService.addDocumentToCollection(dbName, collectionName, documentNode);
        if (!isBroadcasted){
            Broadcaster.broadcast(
                    new Request()
                            .setMethod(HttpMethod.POST)
                            .addAuthHeaders(username, password)
                            .addParam("document", document)
                            .setUrl("http://nodeNODE_ID:9000/api/databases/"+dbName+"/collections/"+collectionName+"/documents"));
        }
        return new ResponseEntity<>("Document created successfully", HttpStatus.CREATED);
    }
    
    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @PutMapping("/{doc_id}/{property_name}")
    public ResponseEntity<String> updateDocument(@PathVariable("db_name") String dbName,
                                                 @PathVariable("collection_name") String collectionName,
                                                 @PathVariable("doc_id") String documentId,
                                                 @PathVariable("property_name") String propertyName,
                                                 @RequestParam("newPropertyValue") Object newPropertyValue,
                                                 @RequestParam("expectedVersion") Long expectedVersion,
                                                 @RequestHeader(value = "X-Broadcast", required = false, defaultValue = "false") Boolean isBroadcasted,
                                                 @RequestHeader("username") String username,
                                                 @RequestHeader("password") String password) throws OperationFailedException, ResourceNotFoundException, VersionMismatchException {


        int nodeWithAffinity = AffinityBalancer.getInstance().getNodeWithAffinityId(documentId);
        boolean hasAffinity = nodeWithAffinity == NodeInfo.getInstance().getNodeId();
        Request updateRequest = new Request()
                .setMethod(HttpMethod.PUT)
                .addAuthHeaders(username, password)
                .addParam("newPropertyValue", newPropertyValue)
                .addParam("expectedVersion", expectedVersion)
                .setUrl("http://nodeNODE_ID:9000/api/databases/" + dbName + "/collections/" + collectionName + "/documents/" + documentId + "/" + propertyName);

        if (!hasAffinity && !isBroadcasted){
            return RedirectionService.redirect(updateRequest, nodeWithAffinity);
        }
        documentService.updateDocumentProperty(dbName, collectionName, documentId,expectedVersion ,propertyName, newPropertyValue);
        if(!isBroadcasted){
            Broadcaster.broadcast(updateRequest);
        }

        return new ResponseEntity<>("Document updated successfully", HttpStatus.OK);
    }
    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @DeleteMapping("/{doc_id}")
    public ResponseEntity<String> deleteDocument(@PathVariable("db_name") String dbName,
                                                 @PathVariable("collection_name") String collectionName,
                                                 @PathVariable("doc_id") String documentId,
                                                 @RequestHeader(value = "X-Broadcast", required = false, defaultValue = "false") Boolean isBroadcasted,
                                                 @RequestHeader("username") String username,
                                                 @RequestHeader("password") String password) throws OperationFailedException, ResourceNotFoundException {

        int nodeWithAffinity = AffinityBalancer.getInstance().getNodeWithAffinityId(documentId);
        boolean hasAffinity = nodeWithAffinity == NodeInfo.getInstance().getNodeId();
        Request deleteRequest = new Request()
                .setMethod(HttpMethod.DELETE)
                .addAuthHeaders(username, password)
                .setUrl("http://nodeNODE_ID:9000/api/databases/"+dbName+"/collections/"+collectionName+"/documents/"+documentId);

        if (!hasAffinity && !isBroadcasted){
            return RedirectionService.redirect(deleteRequest, nodeWithAffinity);
        }
        documentService.deleteDocument(dbName, collectionName, documentId);
        if(!isBroadcasted){
            Broadcaster.broadcast(deleteRequest);
        }
        return new ResponseEntity<>("Document deleted successfully", HttpStatus.OK);
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
        return new ResponseEntity<>(document, HttpStatus.OK);
    }
    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @GetMapping
    public ResponseEntity<String> fetchCollectionDocuments(@PathVariable("db_name") String dbName,
                                                           @PathVariable("collection_name") String collectionName,
                                                           @RequestHeader("username") String username,
                                                           @RequestHeader("password") String password) {

        List<JsonNode> documents = documentService.fetchAllDocumentsFromCollection(dbName, collectionName);
        return new ResponseEntity<>(documents.toString(), HttpStatus.OK);

    }
    @PreAuthorize("@authenticationService.authenticateAdmin(#username, #password)")
    @GetMapping("/{doc_id}/{propertyName}")
    public ResponseEntity<String> readDocumentProperty(@PathVariable("db_name") String dbName,
                                                       @PathVariable("collection_name") String collectionName,
                                                       @PathVariable("doc_id") String documentId,
                                                       @PathVariable String propertyName){
        String propertyValue = documentService.readDocumentProperty(dbName, collectionName, documentId, propertyName);
        return new ResponseEntity<>(propertyValue, HttpStatus.OK);
    }
}
