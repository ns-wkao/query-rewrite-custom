package com.ns.rewrite.config;

public class ColumnDefinition {
    private String name;
    private String type;

    // Getters and Setters
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    @Override
    public String toString() {
        return name + ": " + type;
    }
}