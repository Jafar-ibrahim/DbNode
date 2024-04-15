package org.example.dbnode.Model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.Setter;
import org.example.dbnode.Enum.Role;


@Setter
@Getter
public class User {
    private final String username;
    private String password;
    private final Role role;

    public User(String username, String password, Role role) {
        this.username = username;
        setPassword(password);
        this.role = role;
    }


    @SuppressWarnings("unchecked")
    public ObjectNode toJson() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode userJson = mapper.createObjectNode();
        userJson.put("username", username);
        userJson.put("password", password);
        userJson.put("role", role.toString());
        return userJson;
    }
}