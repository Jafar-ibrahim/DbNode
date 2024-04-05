package org.example.dbnode.Util;

import org.example.dbnode.DatabaseDiskCRUD;
import org.example.dbnode.Exception.ResourceNotFoundException;
import org.example.dbnode.Model.Schema;

import java.util.Objects;

public class DataTypeCaster {

    private final DatabaseDiskCRUD databaseDiskCRUD;
    private static DataTypeCaster instance;
    private DataTypeCaster() {
        this.databaseDiskCRUD = DatabaseDiskCRUD.getInstance();
    }

    public static DataTypeCaster getInstance() {
        if (instance == null) {
            instance = new DataTypeCaster();
        }
        return instance;
    }

    public Object castToDataType(String value, String databaseName,String collectionName, String propertyName) throws ResourceNotFoundException {
        String dataType = getDataType(databaseName,collectionName, propertyName);
        return switch (Objects.requireNonNull(dataType).toUpperCase()) {
            case "STRING" -> value;
            case "LONG" -> Long.parseLong(value);
            case "DOUBLE" -> Double.parseDouble(value);
            case "BOOLEAN" -> Boolean.parseBoolean(value);
            default -> null;
        };
    }

    public String getDataType(String databaseName, String collectionName, String property) throws ResourceNotFoundException {
        Schema schema = databaseDiskCRUD.getCollectionSchema(databaseName, collectionName);
        String dataType = schema.getProperties().get(property);
        if (dataType == null) {
            throw new ResourceNotFoundException("Property " + property + " in collection " + collectionName);
        }
        return dataType;
    }
}
