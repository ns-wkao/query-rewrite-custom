package com.poc.rewrite.config;

public class MaterializedViewDefinition {
    private String definition;
    private String targetTable;

    // Getters and Setters
    public String getDefinition() { return definition; }
    public void setDefinition(String definition) { this.definition = definition; }
    public String getTargetTable() { return targetTable; }
    public void setTargetTable(String targetTable) { this.targetTable = targetTable; }

    @Override
    public String toString() {
        return "MV Definition: " + definition + " -> " + targetTable;
    }
}