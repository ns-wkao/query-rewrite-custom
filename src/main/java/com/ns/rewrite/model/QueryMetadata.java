// File: src/main/java/com/ns/rewrite/model/QueryMetadata.java
package com.ns.rewrite.model;

import com.ns.rewrite.analysis.TemporalGranularityAnalyzer.TimeGranularity;
import java.util.List;
import java.util.Optional;
import java.util.ArrayList;

public class QueryMetadata {
    private String baseTable; // Fully qualified name of the primary base table
    private Optional<String> tableAlias = Optional.empty();
    private List<String> projectionColumns = new ArrayList<>();
    private List<AggregationInfo> aggregations = new ArrayList<>();
    private List<String> groupByColumns = new ArrayList<>();
    private List<String> filterColumns = new ArrayList<>();
    private List<String> joinColumns = new ArrayList<>();
    private List<String> allBaseTables = new ArrayList<>();
    
    // Temporal granularity information
    private TimeGranularity temporalGranularity = TimeGranularity.UNKNOWN;
    private List<String> temporalGroupByColumns = new ArrayList<>();
    
    // Filter temporal requirements
    private TimeGranularity filterGranularity = TimeGranularity.NONE;
    private TimeGranularity minimumRequiredGranularity = TimeGranularity.NONE;
    private List<String> temporalFilterColumns = new ArrayList<>();

    // Getters and setters
    public String getBaseTable() {
        return baseTable;
    }

    public void setBaseTable(String baseTable) {
        this.baseTable = baseTable;
    }

    public Optional<String> getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(Optional<String> tableAlias) {
        this.tableAlias = tableAlias;
    }

    public List<String> getProjectionColumns() {
        return projectionColumns;
    }

    public void setProjectionColumns(List<String> projectionColumns) {
        this.projectionColumns = projectionColumns;
    }

    public List<AggregationInfo> getAggregations() {
        return aggregations;
    }

    public void setAggregations(List<AggregationInfo> aggregations) {
        this.aggregations = aggregations;
    }

    public List<String> getGroupByColumns() {
        return groupByColumns;
    }

    public void setGroupByColumns(List<String> groupByColumns) {
        this.groupByColumns = groupByColumns;
    }

    public List<String> getFilterColumns() {
        return filterColumns;
    }

    public void setFilterColumns(List<String> filterColumns) {
        this.filterColumns = filterColumns;
    }

    public List<String> getJoinColumns() {
        return joinColumns;
    }

    public void setJoinColumns(List<String> joinColumns) {
        this.joinColumns = joinColumns;
    }

    public List<String> getAllBaseTables() {
        return allBaseTables;
    }

    public void setAllBaseTables(List<String> allBaseTables) {
        this.allBaseTables = allBaseTables;
    }

    public TimeGranularity getTemporalGranularity() {
        return temporalGranularity;
    }

    public void setTemporalGranularity(TimeGranularity temporalGranularity) {
        this.temporalGranularity = temporalGranularity != null ? temporalGranularity : TimeGranularity.UNKNOWN;
    }

    public List<String> getTemporalGroupByColumns() {
        return temporalGroupByColumns;
    }

    public void setTemporalGroupByColumns(List<String> temporalGroupByColumns) {
        this.temporalGroupByColumns = temporalGroupByColumns != null ? temporalGroupByColumns : new ArrayList<>();
    }

    public TimeGranularity getFilterGranularity() {
        return filterGranularity;
    }

    public void setFilterGranularity(TimeGranularity filterGranularity) {
        this.filterGranularity = filterGranularity != null ? filterGranularity : TimeGranularity.NONE;
    }

    public TimeGranularity getMinimumRequiredGranularity() {
        return minimumRequiredGranularity;
    }

    public void setMinimumRequiredGranularity(TimeGranularity minimumRequiredGranularity) {
        this.minimumRequiredGranularity = minimumRequiredGranularity != null ? minimumRequiredGranularity : TimeGranularity.NONE;
    }

    public List<String> getTemporalFilterColumns() {
        return temporalFilterColumns;
    }

    public void setTemporalFilterColumns(List<String> temporalFilterColumns) {
        this.temporalFilterColumns = temporalFilterColumns != null ? temporalFilterColumns : new ArrayList<>();
    }
}
