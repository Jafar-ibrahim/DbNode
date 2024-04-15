package org.example.dbnode.Controller;

import org.example.dbnode.Service.AuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class AuthenticationController {

    private final AuthenticationService authenticationService;

    @Autowired
    public AuthenticationController(AuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    @GetMapping("/auth/admin")
    public ResponseEntity<String> authenticateAdmin(@RequestParam("username") String username,
                                                    @RequestParam("password") String password) {
        if(!authenticationService.authenticateAdmin(username, password))
            return new ResponseEntity<>("Admin authentication failed", HttpStatus.UNAUTHORIZED);

        return new  ResponseEntity<>("Admin authenticated successfully", HttpStatus.OK);
    }

    @GetMapping("/auth/user")
    public ResponseEntity<String> authenticateUser(@RequestParam("username") String username,
                                                   @RequestParam("password") String password) {
        if(!authenticationService.authenticateAdmin(username, password))
            return new ResponseEntity<>("User authentication failed", HttpStatus.UNAUTHORIZED);

        return new  ResponseEntity<>("User authenticated successfully", HttpStatus.OK);
    }
}
