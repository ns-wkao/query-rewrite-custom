package com.poc.rewrite.rewriting;

import com.poc.rewrite.model.AggregationInfo;
import com.poc.rewrite.model.QueryMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
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
     * @return True if the materialized view can satisfy the query, false otherwise.
     */
    public static boolean canSatisfy(QueryMetadata userMetadata, QueryMetadata mvMetadata) {
        // 1. Base table must match (case-insensitive)
        if (!userMetadata.getBaseTable().equalsIgnoreCase(mvMetadata.getBaseTable())) {
            logger.debug("Base table mismatch: query='{}', mv='{}'",
                    userMetadata.getBaseTable(), mvMetadata.getBaseTable());
            return false;
        }

        // Use Sets for efficient comparison (relies on AggregationInfo.equals/hashCode)
        Set<AggregationInfo> userAggs = new HashSet<>(userMetadata.getAggregations());
        Set<AggregationInfo> mvAggs = new HashSet<>(mvMetadata.getAggregations());

        Set<String> userGbs = normalizeColumns(userMetadata.getGroupByColumns());
        Set<String> mvGbs = normalizeColumns(mvMetadata.getGroupByColumns());

        Set<String> userFilters = normalizeColumns(userMetadata.getFilterColumns());
        // userProjections now contains the *actual base columns* needed.
        Set<String> userProjections = normalizeColumns(userMetadata.getProjectionColumns());

        // Available *columns* in MV (Projections + Group-bys)
        Set<String> mvAvailableColumns = new HashSet<>();
        mvAvailableColumns.addAll(normalizeColumns(mvMetadata.getProjectionColumns()));
        mvAvailableColumns.addAll(mvGbs);


        // --- Core Matching Logic ---

        // 2. GROUP BY: MV must contain *at least* all user GROUP BY columns.
        if (!mvGbs.containsAll(userGbs)) {
            logger.info("Group-by mismatch. Query needs {}, MV has {}. Missing: {}",
                    userGbs, mvGbs, userGbs.stream().filter(u -> !mvGbs.contains(u)).collect(Collectors.toSet()));
            return false;
        }

        // 3. Aggregations: MV must provide *at least* all user aggregation expressions.
        if (!mvAggs.containsAll(userAggs)) {
            logger.info("Aggregation mismatch. Query needs {}, MV has {}. Missing: {}",
                    userAggs, mvAggs, userAggs.stream().filter(u -> !mvAggs.contains(u)).collect(Collectors.toSet()));
            return false;
        }

        // 4. Aggregation Compatibility
        if (!userAggs.isEmpty() && !mvGbs.containsAll(userGbs)) {
             logger.info("Group-by set mismatch for aggregated queries (User: {}, MV: {}). Requires exact match for POC.",
                          userGbs, mvGbs);
             return false;
        }

        // 5. Column Availability: MV must provide all *base columns* needed for filters and projections.
        Set<String> neededBaseColumns = new HashSet<>();
        neededBaseColumns.addAll(userFilters);
        neededBaseColumns.addAll(userProjections);

        if (!mvAvailableColumns.containsAll(neededBaseColumns)) {
            logger.info("Column availability mismatch. Query needs {}, MV has {}. Missing: {}",
                    neededBaseColumns, mvAvailableColumns, neededBaseColumns.stream().filter(n -> !mvAvailableColumns.contains(n)).collect(Collectors.toSet()));
            return false;
        }

        logger.info("Potential match found! UserQuery ~ MV (Base: {}, GB: {}, Aggs: {}, Cols: OK)",
                mvMetadata.getBaseTable(), userGbs.size(), userAggs.size());
        return true;
    }
}