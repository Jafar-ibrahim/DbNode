package org.example.dbnode.Model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.jsonSchema.jakarta.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.jakarta.JsonSchemaGenerator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Schema {
    private String type;
    private Map<String, String> properties;
    private String[] required;

    public ObjectNode toJson() {
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode schemaNode = mapper.createObjectNode();
        schemaNode.put("type", type);
        schemaNode.putPOJO("properties", properties);
        if (getRequired() != null) {
            ArrayNode requiredArray = mapper.createArrayNode();
            for (String requiredField : required) {
                requiredArray.add(requiredField);
            }
            schemaNode.set("required", requiredArray);
        }
        return schemaNode;
    }
    public static JsonNode of(Class<?> clazz) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
        JsonSchema schema = schemaGen.generateSchema(clazz);

        return mapper.convertValue(schema, JsonNode.class);
    }
}
