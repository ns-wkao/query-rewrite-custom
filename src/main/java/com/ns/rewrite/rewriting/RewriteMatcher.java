package com.ns.rewrite.rewriting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ns.rewrite.config.TableDefinition;
import com.ns.rewrite.model.AggregationInfo;
import com.ns.rewrite.model.QueryMetadata;
import com.ns.rewrite.config.ColumnDefinition;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Determines if a Materialized View can satisfy a given user query.
 */
public class RewriteMatcher {
    private static final Logger logger = LoggerFactory.getLogger(RewriteMatcher.class);

    public static class MatchResult {
        public final boolean canSatisfy;
        public final List<String> mismatchReasons;

        private MatchResult(boolean canSatisfy, List<String> mismatchReasons) {
            this.canSatisfy = canSatisfy;
            this.mismatchReasons = Collections.unmodifiableList(mismatchReasons);
        }

        public static MatchResult success() {
            return new MatchResult(true, new ArrayList<>());
        }

        public static MatchResult failure(List<String> reasons) {
            return new MatchResult(false, reasons);
        }
    }

    /**
     * Determines if a materialized view can satisfy the user query.
     */
    public static MatchResult canSatisfy(QueryMetadata userMetadata, 
                                         QueryMetadata mvMetadata,
                                         String targetTableName, 
                                         TableDefinition originalTableSchema) {
        
        MatchValidator validator = new MatchValidator(userMetadata, mvMetadata, targetTableName, originalTableSchema);
        return validator.validate();
    }

    /**
     * Internal class to handle the validation logic with cleaner separation of concerns.
     */
    private static class MatchValidator {
        private final QueryMetadata userMetadata;
        private final QueryMetadata mvMetadata;
        private final String targetTableName;
        private final TableDefinition originalTableSchema;
        private final List<String> failures = new ArrayList<>();
        
        // Normalized sets for efficient comparison
        private final Set<String> userGroupBys;
        private final Set<String> mvGroupBys;
        private final Set<AggregationInfo> userAggs;
        private final Set<AggregationInfo> mvAggs;
        private final Set<String> userNeededColumns;
        private final Set<String> mvAvailableColumns;
        private final Set<String> originalTableColumns;

        MatchValidator(QueryMetadata userMetadata, QueryMetadata mvMetadata, 
                      String targetTableName, TableDefinition originalTableSchema) {
            this.userMetadata = userMetadata;
            this.mvMetadata = mvMetadata;
            this.targetTableName = targetTableName;
            this.originalTableSchema = originalTableSchema;
            
            // Pre-compute normalized sets
            this.userGroupBys = normalizeColumns(userMetadata.getGroupByColumns());
            this.mvGroupBys = normalizeColumns(mvMetadata.getGroupByColumns());
            this.userAggs = new HashSet<>(userMetadata.getAggregations());
            this.mvAggs = new HashSet<>(mvMetadata.getAggregations());
            
            this.userNeededColumns = combineColumns(
                userMetadata.getFilterColumns(), 
                userMetadata.getProjectionColumns()
            );
            this.mvAvailableColumns = combineColumns(
                mvMetadata.getProjectionColumns(), 
                mvMetadata.getGroupByColumns()
            );
            this.originalTableColumns = extractOriginalTableColumns();
        }

        MatchResult validate() {
            if (!validateBaseTable()) {
                return MatchResult.failure(failures);
            }
            
            validateGroupByColumns();
            validateAggregations();
            validateColumnAvailability();
            
            if (failures.isEmpty()) {
                logger.info("MV '{}' can satisfy user query for table '{}'", 
                    mvMetadata.getBaseTable(), targetTableName);
                return MatchResult.success();
            } else {
                logger.info("MV '{}' cannot satisfy query for table '{}': {}", 
                    mvMetadata.getBaseTable(), targetTableName, String.join("; ", failures));
                return MatchResult.failure(failures);
            }
        }

        private boolean validateBaseTable() {
            if (!mvMetadata.getBaseTable().equalsIgnoreCase(targetTableName)) {
                failures.add(String.format("MV base table '%s' does not match target table '%s'",
                    mvMetadata.getBaseTable(), targetTableName));
                return false;
            }
            return true;
        }

        private void validateGroupByColumns() {
            Set<String> missingGroupBys = findMissingColumns(userGroupBys, mvGroupBys, originalTableColumns);
            if (!missingGroupBys.isEmpty()) {
                failures.add(String.format("Missing GROUP BY columns: %s", missingGroupBys));
            }
        }

        private void validateAggregations() {
            Set<AggregationInfo> missingAggs = userAggs.stream()
                .filter(agg -> !mvAggs.contains(agg))
                .collect(Collectors.toSet());
            
            if (!missingAggs.isEmpty()) {
                failures.add(String.format("Missing aggregations: %s", missingAggs));
            }
        }

        private void validateColumnAvailability() {
            if (originalTableSchema == null || originalTableSchema.getParsedSchema() == null) {
                failures.add("Original table schema not available for column validation");
                return;
            }

            Set<String> cleanedUserColumns = cleanUserColumns(userNeededColumns);
            Set<String> missingColumns = findMissingColumns(cleanedUserColumns, mvAvailableColumns, originalTableColumns);
            
            if (!missingColumns.isEmpty()) {
                failures.add(String.format("Missing essential columns: %s", missingColumns));
            }
        }

        private Set<String> findMissingColumns(Set<String> needed, Set<String> available, Set<String> originalColumns) {
            return needed.stream()
                .filter(col -> !available.contains(col) && originalColumns.contains(col))
                .collect(Collectors.toSet());
        }

        private Set<String> cleanUserColumns(Set<String> columns) {
            return columns.stream()
                .filter(col -> !col.equals("*") && !col.endsWith(".*"))
                .collect(Collectors.toSet());
        }

        private Set<String> combineColumns(Collection<String> set1, Collection<String> set2) {
            Set<String> combined = new HashSet<>();
            combined.addAll(normalizeColumns(set1));
            combined.addAll(normalizeColumns(set2));
            return combined;
        }

        private Set<String> extractOriginalTableColumns() {
            if (originalTableSchema == null || originalTableSchema.getParsedSchema() == null) {
                return new HashSet<>();
            }
            
            return originalTableSchema.getParsedSchema().stream()
                .map(colDef -> normalizeColumns(Collections.singletonList(colDef.getName())).iterator().next())
                .collect(Collectors.toSet());
        }
    }

    /**
     * Normalizes column names by removing qualifiers and quotes, and converting to lowercase.
     */
    private static Set<String> normalizeColumns(Collection<String> columns) {
        if (columns == null) {
            return new HashSet<>();
        }
        
        return columns.stream()
                .map(RewriteMatcher::normalizeColumnName)
                .collect(Collectors.toSet());
    }

    private static String normalizeColumnName(String column) {
        // Strip qualifier if present (but not for function calls)
        int dotIndex = column.lastIndexOf('.');
        if (dotIndex >= 0 && !column.contains("(")) {
            column = column.substring(dotIndex + 1);
        }
        
        // Remove quotes and convert to lowercase
        return column.replace("\"", "").toLowerCase();
    }
}