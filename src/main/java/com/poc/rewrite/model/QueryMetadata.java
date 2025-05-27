package com.poc.rewrite.model;

import java.util.List;
import java.util.Optional;
import java.util.ArrayList; // Import ArrayList

public class QueryMetadata {
    private String baseTable;
    private Optional<String> tableAlias = Optional.empty();
    private List<String> projectionColumns = new ArrayList<>();
    private List<AggregationInfo> aggregations = new ArrayList<>();
    private List<String> groupByColumns = new ArrayList<>();
    private List<String> filterColumns = new ArrayList<>();


    // Getters and setters
    public String getBaseTable() { return baseTable; }
    public void setBaseTable(String baseTable) { this.baseTable = baseTable; }

    public Optional<String> getTableAlias() { return tableAlias; }
    public void setTableAlias(Optional<String> tableAlias) { this.tableAlias = tableAlias; }

    public List<String> getProjectionColumns() { return projectionColumns; }
    public void setProjectionColumns(List<String> projectionColumns) { this.projectionColumns = projectionColumns; }

    public List<AggregationInfo> getAggregations() { return aggregations; }
    public void setAggregations(List<AggregationInfo> aggregations) { this.aggregations = aggregations; }

    public List<String> getGroupByColumns() { return groupByColumns; }
    public void setGroupByColumns(List<String> groupByColumns) { this.groupByColumns = groupByColumns; }

    public List<String> getFilterColumns() { return filterColumns; }
    public void setFilterColumns(List<String> filterColumns) { this.filterColumns = filterColumns; }
}