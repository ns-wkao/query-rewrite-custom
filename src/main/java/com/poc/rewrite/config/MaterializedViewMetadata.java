package com.poc.rewrite.config;

import java.util.List;
import java.util.Map;
import java.util.Optional; // Ensure this is imported

public class MaterializedViewMetadata {
    private String baseTable;
    private Optional<String> tableAlias = Optional.empty(); // New field
    private List<String> projectionColumns;
    private Map<String, String> aggregations;
    private List<String> groupByColumns;

    // Getters and setters
    public String getBaseTable() { return baseTable; }
    public void setBaseTable(String baseTable) { this.baseTable = baseTable; }

    public Optional<String> getTableAlias() { return tableAlias; }
    public void setTableAlias(Optional<String> tableAlias) { this.tableAlias = tableAlias; }

    public List<String> getProjectionColumns() { return projectionColumns; }
    public void setProjectionColumns(List<String> projectionColumns) { this.projectionColumns = projectionColumns; }

    public Map<String, String> getAggregations() { return aggregations; }
    public void setAggregations(Map<String, String> aggregations) { this.aggregations = aggregations; }

    public List<String> getGroupByColumns() { return groupByColumns; }
    public void setGroupByColumns(List<String> groupByColumns) { this.groupByColumns = groupByColumns; }
}