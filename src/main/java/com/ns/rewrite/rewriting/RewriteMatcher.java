package com.ns.rewrite.rewriting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ns.rewrite.config.TableDefinition;
import com.ns.rewrite.model.AggregationInfo;
import com.ns.rewrite.model.QueryMetadata;
import com.ns.rewrite.config.TableDefinition;
import com.ns.rewrite.config.ColumnDefinition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides logic to determine if a Materialized View can satisfy a given user query.
 * Uses structured AggregationInfo for matching and assumes explicit column lists.
 */
public class RewriteMatcher {

    private static final Logger logger = LoggerFactory.getLogger(RewriteMatcher.class);

    /**
     * Helper class to hold the result of a match attempt.
     */
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
     * Strips qualifiers (optional) and lowercases column names for comparison.
     * Keeps it simple for now, mainly lowercasing and basic quote removal.
     *
     * @param cols List of column/expression strings.
     * @return A Set of normalized strings.
     */
    private static Set<String> normalizeColumns(Collection<String> cols) {
        if (cols == null) {
            return new HashSet<>();
        }
        return cols.stream()
                .map(c -> {
                    // Only strip qualifier if a dot exists AND it's not part of a function call
                    int dot = c.lastIndexOf('.');
                    return (dot >= 0 && !c.contains("(")) ? c.substring(dot + 1) : c;
                })
                .map(c -> c.replace("\"", "")) // Remove quotes
                .map(String::toLowerCase)       // Convert to lowercase
                .collect(Collectors.toSet());
    }

    /**
     * Determines if a materialized view can satisfy the user query.
     *
     * @param userMetadata Metadata from the user query (refined, explicit columns).
     * @param mvMetadata   Metadata from the materialized view (explicit columns).
     * @return A MatchResult object indicating success or failure with reasons.
     */
    public static MatchResult canSatisfy(QueryMetadata userMetadata, 
                                         QueryMetadata mvMetadata,
                                         String actualUserQueryTableToReplace, 
                                         TableDefinition originalTableSchema) {
        List<String> reasons = new ArrayList<>();

        // 1. Base table consistency check: MV's base must be the table we're trying to replace.
        // Should be already filtered by SQLRewriter
        if (!mvMetadata.getBaseTable().equalsIgnoreCase(actualUserQueryTableToReplace)) {
            reasons.add(String.format("MV base table '%s' does not match the target user query table '%s' for replacement.",
                    mvMetadata.getBaseTable(), actualUserQueryTableToReplace));
            logger.debug("MV base table mismatch: mv='{}', target_user_table='{}'",
                    mvMetadata.getBaseTable(), actualUserQueryTableToReplace);
            return MatchResult.failure(reasons); // Fail fast
        }
        logger.debug("MV '{}' is based on table '{}', which matches user query table being considered for replacement.", mvMetadata.getBaseTable(), actualUserQueryTableToReplace);


        // Use Sets for efficient comparison (relies on AggregationInfo.equals/hashCode)
        Set<AggregationInfo> userAggs = new HashSet<>(userMetadata.getAggregations());
        Set<AggregationInfo> mvAggs = new HashSet<>(mvMetadata.getAggregations());

        Set<String> userGbs = normalizeColumns(userMetadata.getGroupByColumns());
        Set<String> mvGbs = normalizeColumns(mvMetadata.getGroupByColumns());

        // User query's filters and projections are global for the entire query
        Set<String> userNeededFilterColumns = normalizeColumns(userMetadata.getFilterColumns());
        Set<String> userNeededProjectionColumns = normalizeColumns(userMetadata.getProjectionColumns());
        
        logger.debug("User query needs filter columns (normalized): {}", userNeededFilterColumns);
        logger.debug("User query needs projection columns (normalized): {}", userNeededProjectionColumns);

        // Get normalized columns from the materialize view
        Set<String> mvAvailableNormalizedColumns = new HashSet<>();
        mvAvailableNormalizedColumns.addAll(normalizeColumns(mvMetadata.getProjectionColumns()));
        mvAvailableNormalizedColumns.addAll(normalizeColumns(mvMetadata.getGroupByColumns()));
        logger.debug("MV '{}' offers available columns (normalized projections + groupBys): {}", mvMetadata.getBaseTable(), mvAvailableNormalizedColumns);


        // 2. GROUP BY: MV must contain *at least* all user GROUP BY columns that orignated from the table being replaced.
        //    Simplification: if a GB column isn't in MV, check if it belonged to the original table.
        //    If it belonged elsewhere, it's not MV's fault. If it belonged to original table, it's a miss.
        Set<String> missingGbs = new HashSet<>();
        for (String userGbCol : userGbs) {
            if (!mvGbs.contains(userGbCol)) {
                // Check if this group-by column was part of the originalTableSchema
                boolean wasInOriginalTable = false;
                if (originalTableSchema != null && originalTableSchema.getParsedSchema() != null) {
                    for (ColumnDefinition colDef : originalTableSchema.getParsedSchema()) {
                        if (normalizeColumns(Collections.singletonList(colDef.getName())).contains(userGbCol)) {
                            wasInOriginalTable = true;
                            break;
                        }
                    }
                }
                if (wasInOriginalTable) { // Only a problem if the original table was supposed to provide it
                    missingGbs.add(userGbCol);
                }
            }
        }
        if (!missingGbs.isEmpty()) {
            reasons.add(String.format("Group-by mismatch for table '%s'. MV needs to provide these missing group-by columns: %s. (MV has: %s, User query needs: %s)",
                    actualUserQueryTableToReplace, missingGbs, mvGbs, userGbs));
        }

        // 3. Aggregations: MV must provide *at least* all user aggregation expressions
        //    that involve columns from the table being replaced.
        //    Simplification: The AggregationInfo normalizes column names. If an agg uses a column
        //    from originalTableSchema and that agg isn't in mvAggs, it's a problem.
        //    This is hard to check precisely without knowing source of each arg in AggInfo.
        //    Current AggInfo normalizes to base column name. Assume if user agg is not in MV, it's missing.
        //    This part might need refinement if an aggregation is purely on other tables.
        //    For now, keeping original logic: MV must have all user aggs.
        Set<AggregationInfo> missingAggs = userAggs.stream()
                .filter(uAgg -> !mvAggs.contains(uAgg))
                .collect(Collectors.toSet());
        if (!missingAggs.isEmpty()) {
            reasons.add(String.format("Aggregation mismatch for table '%s'. Query needs %s, MV has %s. Missing: %s",
                    actualUserQueryTableToReplace, userAggs, mvAggs, missingAggs));
        }



        // 4. Column Availability: MV must provide all *base columns* needed for filters and projections.
        Set<String> allUserNeededNormalizedColumns = new HashSet<>();
        allUserNeededNormalizedColumns.addAll(userNeededFilterColumns);
        allUserNeededNormalizedColumns.addAll(userNeededProjectionColumns);
        allUserNeededNormalizedColumns.remove("*"); // Don't check for global "*" presence in MV
        // Also remove "alias.*" patterns from checking, as MV stores specific columns
        allUserNeededNormalizedColumns.removeIf(col -> col.endsWith(".*"));

        Set<String> missingEssentialCols = new HashSet<>();
        
        // Declare originalTableNormalizedSchemaCols outside the if block to ensure it's accessible later
        Set<String> originalTableNormalizedSchemaCols = new HashSet<>();
        
        if (originalTableSchema == null || originalTableSchema.getParsedSchema() == null) {
             reasons.add(String.format("Schema for original table '%s' not available, cannot reliably check column availability.", actualUserQueryTableToReplace));
        } else {
            originalTableNormalizedSchemaCols = originalTableSchema.getParsedSchema().stream()
                .map(colDef -> normalizeColumns(Collections.singletonList(colDef.getName())).iterator().next())
                .collect(Collectors.toSet());
            logger.debug("Normalized columns from schema of '{}': {}", actualUserQueryTableToReplace, originalTableNormalizedSchemaCols);

            for (String neededCol : allUserNeededNormalizedColumns) {
                if (!mvAvailableNormalizedColumns.contains(neededCol)) { // MV doesn't have this needed column
                    // Check if this needed column *should* have come from the table MV is replacing
                    if (originalTableNormalizedSchemaCols.contains(neededCol)) {
                        // Yes, it was part of the original table's schema, so MV should provide it.
                        missingEssentialCols.add(neededCol);
                    } else {
                        // No, it wasn't part of the original table's schema.
                        // So, it must be from another table in the user's query. Not MV's fault.
                        logger.trace("Needed column '{}' is not in MV, but also not in schema of original table '{}'. Assuming it's from another table.", neededCol, actualUserQueryTableToReplace);
                    }
                }
            }
        }
        // --- Result ---
        if (!missingEssentialCols.isEmpty()) {
            reasons.add(String.format("Column availability mismatch for table '%s'. MV is missing essential columns: %s. (MV has: %s, User query needs these from original table: %s)",
                    actualUserQueryTableToReplace, missingEssentialCols, mvAvailableNormalizedColumns, allUserNeededNormalizedColumns.stream().filter(originalTableNormalizedSchemaCols::contains).collect(Collectors.toSet())));
        }

        // --- Result ---
        if (reasons.isEmpty()) {
            logger.info("Potential match! MV for base table '{}' can satisfy relevant parts of user query for replacing table '{}'.",
                    mvMetadata.getBaseTable(), actualUserQueryTableToReplace);
            return MatchResult.success();
        } else {
            logger.info("MV for base '{}' cannot satisfy query for replacing table '{}'. Reasons: {}",
                    mvMetadata.getBaseTable(), actualUserQueryTableToReplace, reasons.stream().collect(Collectors.joining("; ")));
            return MatchResult.failure(reasons);
        }
    }
}