package com.ns.rewrite.config;

import java.util.Map;

public class TableConfig {
    private Map<String, TableDefinition> tables;
    private Map<String, MaterializedViewDefinition> materializedViews;

    // Getters and Setters
    public Map<String, TableDefinition> getTables() { return tables; }
    public void setTables(Map<String, TableDefinition> tables) { this.tables = tables; }
    public Map<String, MaterializedViewDefinition> getMaterializedViews() { return materializedViews; }
    public void setMaterializedViews(Map<String, MaterializedViewDefinition> materializedViews) { this.materializedViews = materializedViews; }
}