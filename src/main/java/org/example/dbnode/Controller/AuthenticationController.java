package org.example.dbnode.Controller;

import org.example.dbnode.Service.AuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class AuthenticationController {

    private final AuthenticationService authenticationService;

    @Autowired
    public AuthenticationController(AuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    @GetMapping("/auth")
    public ResponseEntity<?> authenticate(@RequestParam("username") String username,
                                          @RequestParam("password") String password) {
        Map<String, String> response = new HashMap<>();
        response.put("username", username);
        if(authenticationService.authenticateAdmin(username, password)) {
            response.put("role","ADMIN");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }
        if (authenticationService.authenticateUser(username, password)) {
            response.put("role","USER");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        return new ResponseEntity<>("User authentication failed", HttpStatus.UNAUTHORIZED);
    }

    @GetMapping("/auth/user")
    public ResponseEntity<String> authenticateUser(@RequestParam("username") String username,
                                                   @RequestParam("password") String password) {
        if(!authenticationService.authenticateUser(username, password))
            return new ResponseEntity<>("User authentication failed", HttpStatus.UNAUTHORIZED);

        return new ResponseEntity<>("User authenticated successfully", HttpStatus.OK);
    }
}
