package org.example.dbnode.Service.Interface;

public interface IAuthenticationService {
    boolean authenticateAdmin(String username, String password);
    boolean authenticateUser(String username, String password);
}
