package org.example.dbnode.Service.Interfaces;

public interface AuthenticationService {
    boolean authenticateAdmin(String username, String password);
    boolean authenticateUser(String username, String password);
}
