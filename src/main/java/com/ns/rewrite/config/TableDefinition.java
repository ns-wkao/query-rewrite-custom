package com.ns.rewrite.config;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableDefinition {
    private List<Map<String, String>> schema; // [{colName: type}, ...]
    private transient List<ColumnDefinition> parsedSchema;


    // Getters and Setters
    public List<Map<String, String>> getSchema() {
        return schema;
    }

    public void setSchema(List<Map<String, String>> schema) {
        this.schema = schema;
        this.parsedSchema = schema.stream().map(colMap -> {
            ColumnDefinition cd = new ColumnDefinition();
            Map.Entry<String, String> entry = colMap.entrySet().iterator().next();
            cd.setName(entry.getKey());
            cd.setType(entry.getValue());
            return cd;
        }).collect(Collectors.toList());
    }

    public List<ColumnDefinition> getParsedSchema() {
        if (parsedSchema == null && schema != null) { // Handle direct instantiation if needed
             this.parsedSchema = schema.stream().map(colMap -> {
                ColumnDefinition cd = new ColumnDefinition();
                Map.Entry<String, String> entry = colMap.entrySet().iterator().next();
                cd.setName(entry.getKey());
                cd.setType(entry.getValue());
                return cd;
            }).collect(Collectors.toList());
        }
        return parsedSchema;
    }


    @Override
    public String toString() {
        return "Schema: " + (parsedSchema != null ? parsedSchema.toString() : "null");
    }
}