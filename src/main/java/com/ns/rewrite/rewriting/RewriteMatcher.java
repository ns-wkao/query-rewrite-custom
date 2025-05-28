package com.ns.rewrite.rewriting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ns.rewrite.model.AggregationInfo;
import com.ns.rewrite.model.QueryMetadata;

import java.util.ArrayList; // Added
import java.util.Collection;
import java.util.Collections; // Added
import java.util.HashSet;
import java.util.List; // Added
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
    public static MatchResult canSatisfy(QueryMetadata userMetadata, QueryMetadata mvMetadata) {
        List<String> reasons = new ArrayList<>();

        // 1. Base table must match (case-insensitive)
        if (!userMetadata.getBaseTable().equalsIgnoreCase(mvMetadata.getBaseTable())) {
            reasons.add(String.format("Base table mismatch: query='%s', mv='%s'",
                    userMetadata.getBaseTable(), mvMetadata.getBaseTable()));
            logger.debug("Base table mismatch: query='{}', mv='{}'",
                    userMetadata.getBaseTable(), mvMetadata.getBaseTable());
            return MatchResult.failure(reasons); // Fail fast on base table
        }

        // Use Sets for efficient comparison (relies on AggregationInfo.equals/hashCode)
        Set<AggregationInfo> userAggs = new HashSet<>(userMetadata.getAggregations());
        Set<AggregationInfo> mvAggs = new HashSet<>(mvMetadata.getAggregations());

        Set<String> userGbs = normalizeColumns(userMetadata.getGroupByColumns());
        Set<String> mvGbs = normalizeColumns(mvMetadata.getGroupByColumns());

        Set<String> userFilters = normalizeColumns(userMetadata.getFilterColumns());
        Set<String> userProjections = normalizeColumns(userMetadata.getProjectionColumns());

        logger.info("userFilters: {}", userFilters);
        logger.info("userProjections: {}", userProjections);

        // Available *columns* in MV (Projections + Group-bys)
        Set<String> mvAvailableColumns = new HashSet<>();
        mvAvailableColumns.addAll(normalizeColumns(mvMetadata.getProjectionColumns()));
        mvAvailableColumns.addAll(mvGbs);


        // --- Core Matching Logic ---
        Set<String> neededBaseColumns = new HashSet<>();
        neededBaseColumns.addAll(userFilters);
        neededBaseColumns.addAll(userProjections);

        // 2. GROUP BY: MV must contain *at least* all user GROUP BY columns.
        Set<String> missingGbs = userGbs.stream()
                .filter(u -> !mvGbs.contains(u))
                .collect(Collectors.toSet());
        if (!missingGbs.isEmpty()) {
            reasons.add(String.format("Group-by mismatch. Query needs %s, MV has %s. Missing: %s",
                    userGbs, mvGbs, missingGbs));
        }

        // 3. Aggregations: MV must provide *at least* all user aggregation expressions.
        Set<AggregationInfo> missingAggs = userAggs.stream()
                .filter(u -> !mvAggs.contains(u))
                .collect(Collectors.toSet());
        if (!missingAggs.isEmpty()) {
            reasons.add(String.format("Aggregation mismatch. Query needs %s, MV has %s. Missing: %s",
                    userAggs, mvAggs, missingAggs));
        }

        // 4. Column Availability: MV must provide all *base columns* needed for filters and projections.
        Set<String> missingCols = neededBaseColumns.stream()
                .filter(n -> !mvAvailableColumns.contains(n))
                .filter(n -> !n.equals("*")) // Don't report '*' as missing
                .collect(Collectors.toSet());
        if (!missingCols.isEmpty()) {
            reasons.add(String.format("Column availability mismatch. Query needs %s, MV has %s. Missing: %s",
                    neededBaseColumns, mvAvailableColumns, missingCols));
        }

        // --- Result ---
        if (reasons.isEmpty()) {
            logger.info("Potential match found! UserQuery ~ MV (Base: {}, GB: {}, Aggs: {}, Cols: OK)",
                    mvMetadata.getBaseTable(), userGbs.size(), userAggs.size());
            return MatchResult.success();
        } else {
            // Log here as well for immediate feedback during processing
            logger.info("MV with base '{}' cannot satisfy query. Reasons: {}",
                    mvMetadata.getBaseTable(), reasons);
            return MatchResult.failure(reasons);
        }
    }
}