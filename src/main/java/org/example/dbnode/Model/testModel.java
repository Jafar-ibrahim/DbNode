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
public class testModel {
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private String id;
    @JsonProperty(access = JsonProperty.Access.READ_WRITE, required = true)
    private String name;
    @JsonIgnore
    private int number;
    public ObjectNode toJson(testModel model) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.valueToTree(model);
        return objectNode;
    }
}
