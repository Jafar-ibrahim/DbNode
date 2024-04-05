package org.example.dbnode.Model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.*;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class TestModel {
    @JsonProperty(namespace = "id", access = JsonProperty.Access.READ_ONLY, required = true)
    private String id;
    @JsonProperty(namespace = "name",access = JsonProperty.Access.READ_WRITE, required = true)
    private String name;
    @JsonIgnore
    private int number;
    public ObjectNode toJson(TestModel model) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.valueToTree(model);
        return objectNode;
    }
}
