package org.example.dbnode.Model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.jsonSchema.jakarta.*;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.ValidationMessage;
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
    public static JsonNode fromClass(Class<?> clazz) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
        JsonSchema schema = schemaGen.generateSchema(clazz);

        return mapper.convertValue(schema, JsonNode.class);
    }

    public static Schema of(String jsonSchema) throws IOException {
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

    public static void main(String[] args) throws IOException {
        String schema = Schema.fromClass(TestModel.class).toString();
        System.out.println(schema);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode schemaNode = mapper.readValue(schema, JsonNode.class);
        System.out.println(schemaNode.get("type"));
    }

    /*public boolean validateDocument(ObjectNode jsonNode) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
            JsonSchema schema = schemaGen.generateSchema(Schema.class);
            JsonNode schemaNode = mapper.valueToTree(schema);

            JsonNode jsonNodeSchema = mapper.readTree(schemaNode.toString());
            JsonNode jsonNodeDocument = mapper.readTree(jsonNode.toString());

            // validate the document
            if (jsonNodeSchema.equals(jsonNodeDocument)) {
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new RuntimeException("Validation failed: " + e.getMessage());
        }
    }*/
    public boolean validateDocument(ObjectNode jsonToValidate) {
        JsonNode schemaNode = this.toJson();
        Set<ValidationMessage> validationMessages = JsonSchemaFactory.getInstance().getSchema(schemaNode).validate(jsonToValidate);
        return validationMessages.isEmpty();
    }
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

