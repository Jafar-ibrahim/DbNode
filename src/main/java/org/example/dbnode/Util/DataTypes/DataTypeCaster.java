package org.example.dbnode.Util.DataTypes;

import org.example.dbnode.DatabaseDiskCRUD;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Model.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Objects;
@Component
public class DataTypeCaster {

    private final DatabaseDiskCRUD databaseDiskCRUD;
    @Autowired
    public DataTypeCaster(@Lazy DatabaseDiskCRUD databaseDiskCRUD) {
        this.databaseDiskCRUD = databaseDiskCRUD;
    }

    public Object castToDataType(String value, String databaseName,String collectionName, String propertyName) throws ResourceNotFoundException {
        String dataType = getDataType(databaseName,collectionName, propertyName);
        return switch (Objects.requireNonNull(dataType).toUpperCase()) {
            case "STRING" -> value;
            case "INTEGER" -> Integer.parseInt(value);
            case "NUMBER" -> Double.parseDouble(value);
            case "BOOLEAN" -> Boolean.parseBoolean(value);
            default -> null;
        };
    }

    public String getDataType(String databaseName, String collectionName, String property) throws ResourceNotFoundException {
        Schema schema = databaseDiskCRUD.getCollectionSchema(databaseName, collectionName);
        String dataType = schema.getProperties().get(property);
        if (dataType == null) {
            throw new IllegalArgumentException("Property not found in schema");
        }
        return dataType;
    }
}
