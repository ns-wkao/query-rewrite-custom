package com.ns.rewrite.rewriting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ns.rewrite.analysis.TemporalGranularityAnalyzer;
import com.ns.rewrite.config.TableDefinition;
import com.ns.rewrite.model.AggregationInfo;
import com.ns.rewrite.model.QueryMetadata;

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
        
        logger.info("Starting MV validation for table '{}' against user query", targetTableName);
        MatchValidator validator = new MatchValidator(userMetadata, mvMetadata, targetTableName, originalTableSchema);
        MatchResult result = validator.validate();
        
        if (result.canSatisfy) {
            logger.info("MV validation completed successfully for table '{}'", targetTableName);
        } else {
            logger.info("MV validation failed for table '{}': {}", targetTableName, String.join("; ", result.mismatchReasons));
        }
        
        return result;
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
                userMetadata.getProjectionColumns(),
                userMetadata.getJoinColumns()
            );
            this.mvAvailableColumns = combineColumns(
                mvMetadata.getProjectionColumns(), 
                mvMetadata.getGroupByColumns()
            );
        }

        MatchResult validate() {
            logger.debug("Validating base table compatibility");
            if (!validateBaseTable()) {
                return MatchResult.failure(failures);
            }
            
            logger.debug("Validating GROUP BY column compatibility");
            validateGroupByColumns();
            
            logger.debug("Validating aggregation function compatibility");
            validateAggregations();
            
            logger.debug("Validating column availability");
            validateColumnAvailability();
            
            logger.debug("Validating temporal granularity compatibility");
            validateTemporalGranularity();
            
            if (failures.isEmpty()) {
                logger.debug("All validation checks passed for table '{}'", targetTableName);
                return MatchResult.success();
            } else {
                logger.debug("Validation failed with {} issues: {}", failures.size(), String.join("; ", failures));
                return MatchResult.failure(failures);
            }
        }

        private boolean validateBaseTable() {
            logger.debug("Checking base table compatibility: MV='{}' vs Target='{}'", mvMetadata.getBaseTable(), targetTableName);
            if (!mvMetadata.getBaseTable().equalsIgnoreCase(targetTableName)) {
                String reason = String.format("MV base table '%s' does not match target table '%s'",
                    mvMetadata.getBaseTable(), targetTableName);
                failures.add(reason);
                logger.debug("Base table validation failed: {}", reason);
                return false;
            }
            logger.debug("Base table validation passed");
            return true;
        }

        private void validateGroupByColumns() {
            logger.debug("Checking GROUP BY compatibility: User={} vs MV={}", userGroupBys, mvGroupBys);
            Set<String> missingGroupBys = findMissingColumns(userGroupBys, mvGroupBys);
            if (!missingGroupBys.isEmpty()) {
                String reason = String.format("Missing GROUP BY columns: %s", missingGroupBys);
                failures.add(reason);
                logger.debug("GROUP BY validation failed: {}", reason);
            } else {
                logger.debug("GROUP BY validation passed - all required columns available");
            }
        }

        private void validateAggregations() {
            logger.debug("Checking aggregation compatibility: User={} vs MV={}", userAggs, mvAggs);
            
            // Filter aggregations to only consider those relevant to the target table
            Set<AggregationInfo> relevantUserAggs = userAggs.stream()
                .map(this::filterAggregationToTargetTable)
                .filter(agg -> agg != null) // Only keep non-null aggregations (null means no relevant args)
                .collect(Collectors.toSet());
            
            logger.debug("Relevant user aggregations after filtering: {}", relevantUserAggs);
            
            Set<AggregationInfo> exactlyMissingAggs = relevantUserAggs.stream()
                .filter(agg -> !mvAggs.contains(agg))
                .collect(Collectors.toSet());
            
            if (exactlyMissingAggs.isEmpty()) {
                logger.debug("Aggregation validation passed - all aggregations directly available");
                return; // All aggregations are directly available
            }
            
            logger.debug("Missing aggregations: {}, checking if they can be computed", exactlyMissingAggs);
            
            // Check if missing aggregations can be computed from available MV columns
            Set<AggregationInfo> uncomputableAggs = exactlyMissingAggs.stream()
                .filter(agg -> !canComputeAggregation(agg))
                .collect(Collectors.toSet());
            
            if (!uncomputableAggs.isEmpty()) {
                String reason = String.format("Missing aggregations that cannot be computed: %s", uncomputableAggs);
                failures.add(reason);
                logger.debug("Aggregation validation failed: {}", reason);
            } else if (!exactlyMissingAggs.isEmpty()) {
                logger.debug("Aggregation validation passed - missing aggregations can be computed from MV: {}", exactlyMissingAggs);
            }
        }
        
        private AggregationInfo filterAggregationToTargetTable(AggregationInfo agg) {
            String tablePrefix = targetTableName.toLowerCase() + ".";
            
            // Filter arguments to only include those from the target table and are real columns
            List<String> filteredArgs = agg.getArguments().stream()
                .filter(arg -> {
                    // Allow * and empty arguments (for COUNT(*))
                    if (arg.equals("*") || arg.isEmpty()) {
                        return true;
                    }
                    // Only include arguments that belong to target table and are real columns
                    return arg.startsWith(tablePrefix) && isRealColumnInBaseTable(arg);
                })
                .collect(Collectors.toList());
            
            // If the original aggregation had arguments but none remain after filtering, filter it out
            // Exception: COUNT(*) and similar have empty/star arguments and should be kept
            boolean originalHadRealArgs = !agg.getArguments().isEmpty() && 
                                        !agg.getArguments().contains("*");
            if (filteredArgs.isEmpty() && originalHadRealArgs) {
                return null;
            }
            
            // Return new AggregationInfo with filtered arguments
            return new AggregationInfo(agg.getFunctionName(), filteredArgs, agg.isDistinct());
        }

        /**
         * Determines if an aggregation can be computed from the materialized view's available columns.
         */
        private boolean canComputeAggregation(AggregationInfo agg) {
            String function = agg.getFunctionName().toLowerCase();
            List<String> args = agg.getArguments();
            
            switch (function) {
                case "count":
                    return canComputeCount(agg);
                case "sum":
                    return canComputeSum(agg);
                default:
                    // For unknown functions, be conservative and return false
                    return false;
            }
        }

        private boolean canComputeCount(AggregationInfo agg) {
            if (agg.isDistinct()) {
                // COUNT(DISTINCT col) can be computed ONLY if 'col' is a GROUP BY key in the MV
                // This works because if col is a grouping key, each row in MV represents 
                // a distinct value of col (among other grouping dimensions)
                return agg.getArguments().stream()
                    .allMatch(arg -> mvGroupBys.contains(arg.toLowerCase()));
            } else {
                // COUNT(*) or COUNT(col) - if we have any pre-computed count or if all referenced columns are available
                if (agg.getArguments().isEmpty() || agg.getArguments().contains("*")) {
                    // COUNT(*) can be computed if we have any count aggregation in MV or if we can count groups
                    return hasAnyCountAggregation() || !mvGroupBys.isEmpty();
                } else {
                    // COUNT(col) can be computed if col is available
                    return agg.getArguments().stream()
                        .allMatch(arg -> mvAvailableColumns.contains(arg.toLowerCase()));
                }
            }
        }

        private boolean canComputeSum(AggregationInfo agg) {
            // SUM(col) can be computed if:
            // 1. We have the exact SUM(col) in MV, OR
            // 2. We have 'col' available and can sum it up from MV groups
            return agg.getArguments().stream()
                .allMatch(arg -> mvAvailableColumns.contains(arg.toLowerCase()));
        }

        private boolean hasAnyCountAggregation() {
            return mvAggs.stream()
                .anyMatch(agg -> agg.getFunctionName().equals("count"));
        }



        private void validateColumnAvailability() {
            if (originalTableSchema == null || originalTableSchema.getParsedSchema() == null) {
                String reason = "Original table schema not available for column validation";
                failures.add(reason);
                logger.debug("Column validation failed: {}", reason);
                return;
            }

            logger.debug("Checking column availability: User needs={} (filters + projections + joins) vs MV provides={}", userNeededColumns, mvAvailableColumns);
            Set<String> cleanedUserColumns = cleanUserColumns(userNeededColumns);
            Set<String> missingColumns = findMissingColumns(cleanedUserColumns, mvAvailableColumns);
            
            logger.debug("Cleaned user columns: {}", cleanedUserColumns);
            logger.debug("MV available columns: {}", mvAvailableColumns);
            logger.debug("Missing columns after analysis: {}", missingColumns);

            if (!missingColumns.isEmpty()) {
                String reason = String.format("Missing essential columns: %s", missingColumns);
                failures.add(reason);
                logger.debug("Column validation failed: {}", reason);
            } else {
                logger.debug("Column validation passed - all required columns available");
            }
        }

        private Set<String> findMissingColumns(Set<String> needed, Set<String> available) {
            // Conservative approach: Only consider qualified columns that clearly belong to the target table
            // AND actually exist in the base table schema (not aliases/computed columns)
            String tablePrefix = targetTableName.toLowerCase() + ".";
            
            return needed.stream()
                .filter(col -> col.startsWith(tablePrefix))           // Only qualified columns from the target table
                .filter(col -> isRealColumnInBaseTable(col))          // That actually exist in the base table schema
                .filter(col -> !available.contains(col))             // That are missing from MV
                .collect(Collectors.toSet());
        }
        
        private boolean isRealColumnInBaseTable(String qualifiedColumnName) {
            if (originalTableSchema == null || originalTableSchema.getParsedSchema() == null) {
                return true; // If no schema available, assume it's real to be safe
            }
            
            String tablePrefix = targetTableName.toLowerCase() + ".";
            if (!qualifiedColumnName.startsWith(tablePrefix)) {
                return false;
            }
            
            // Extract the column name without the table prefix
            String columnName = qualifiedColumnName.substring(tablePrefix.length());
            
            // Check if this column exists in the base table schema
            return originalTableSchema.getParsedSchema().stream()
                .anyMatch(colDef -> colDef.getName().toLowerCase().equals(columnName));
        }

        private void validateTemporalGranularity() {
            // Use the combined minimum required granularity instead of just GROUP BY granularity
            TemporalGranularityAnalyzer.TimeGranularity userRequiredGranularity = userMetadata.getMinimumRequiredGranularity();
            TemporalGranularityAnalyzer.TimeGranularity mvRequiredGranularity = mvMetadata.getMinimumRequiredGranularity();
            
            // If neither query has temporal requirements, validation passes
            if (userRequiredGranularity == TemporalGranularityAnalyzer.TimeGranularity.UNKNOWN &&
                mvRequiredGranularity == TemporalGranularityAnalyzer.TimeGranularity.UNKNOWN) {
                logger.debug("No temporal requirements found in either user query or MV - validation passed");
                return;
            }
            
            // If only user query has temporal requirements, MV cannot satisfy
            if (userRequiredGranularity != TemporalGranularityAnalyzer.TimeGranularity.UNKNOWN &&
                mvRequiredGranularity == TemporalGranularityAnalyzer.TimeGranularity.UNKNOWN) {
                String reason = String.format("User query requires temporal granularity (%s) but MV has no temporal requirements", 
                                             userRequiredGranularity);
                failures.add(reason);
                logger.debug("Temporal validation failed: {}", reason);
                return;
            }
            
            // If only MV has temporal requirements, it can still satisfy non-temporal queries
            if (userRequiredGranularity == TemporalGranularityAnalyzer.TimeGranularity.UNKNOWN &&
                mvRequiredGranularity != TemporalGranularityAnalyzer.TimeGranularity.UNKNOWN) {
                logger.debug("MV has temporal requirements ({}) but user query doesn't require any - validation passed", mvRequiredGranularity);
                return;
            }
            
            // Both have temporal requirements - check compatibility
            TemporalGranularityAnalyzer analyzer = new TemporalGranularityAnalyzer();
            if (!analyzer.canMvSatisfyQuery(mvRequiredGranularity, userRequiredGranularity)) {
                String reason = String.format("MV temporal granularity (%s) is too coarse for user query requirements (%s)", 
                                             mvRequiredGranularity, userRequiredGranularity);
                failures.add(reason);
                logger.debug("Temporal validation failed: {}", reason);
            } else {
                logger.debug("Temporal validation passed: MV granularity ({}) can satisfy user query requirements ({})", 
                           mvRequiredGranularity, userRequiredGranularity);
            }
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

        private Set<String> combineColumns(Collection<String> set1, Collection<String> set2, Collection<String> set3) {
            Set<String> combined = new HashSet<>();
            combined.addAll(normalizeColumns(set1));
            combined.addAll(normalizeColumns(set2));
            combined.addAll(normalizeColumns(set3));
            return combined;
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
        return column.toLowerCase();
    }
}