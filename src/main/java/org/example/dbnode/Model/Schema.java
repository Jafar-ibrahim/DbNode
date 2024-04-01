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
import java.util.*;

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

        ObjectNode propertiesNode = mapper.createObjectNode();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            ObjectNode propertyNode = mapper.createObjectNode();
            propertyNode.put("type", entry.getValue());
            propertyNode.put("required", Arrays.asList(required).contains(entry.getKey()));
            propertiesNode.set(entry.getKey(), propertyNode);
        }
        schemaNode.set("properties", propertiesNode);

        return schemaNode;
    }
    public static JsonNode of(Class<?> clazz) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
        JsonSchema schema = schemaGen.generateSchema(clazz);

        return mapper.convertValue(schema, JsonNode.class);
    }
    public static Schema convertJsonToSchema(String jsonSchema) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode schemaNode = mapper.readValue(jsonSchema, JsonNode.class);

        String type = schemaNode.get("type").asText();

        Map<String, String> properties = new HashMap<>();
        List<String> requiredList = new ArrayList<>();
        JsonNode propertiesNode = schemaNode.get("properties");
        propertiesNode.fieldNames().forEachRemaining(field -> {
            properties.put(field, propertiesNode.get(field).get("type").asText());
            if (propertiesNode.get(field).has("required") && propertiesNode.get(field).get("required").asBoolean()) {
                requiredList.add(field);
            }
        });

        String[] required = requiredList.toArray(new String[0]);

        return new Schema(type, properties, required);
    }
    /*
    // Example JSON Schema
    {
      "type" : "object",
      "id" : "urn:jsonschema:org:example:dbnode:Model:testModel",
      "properties" : {
        "id" : {
          "type" : "string"
        },
        "name" : {
          "type" : "string",
          "required" : true
        },
        "version" : {
          "type" : "integer",
          "required" : true
        },
        "replicated" : {
          "type" : "boolean",
          "required" : true
        }
      }
   }*/
}
