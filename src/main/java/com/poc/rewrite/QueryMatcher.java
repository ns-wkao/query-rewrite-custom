package com.poc.rewrite;

import com.poc.rewrite.config.MaterializedViewMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryMatcher {

    private static final Logger logger = LoggerFactory.getLogger(QueryMatcher.class);

    /**
     * Strip any table/alias qualifier and lowercase.
     */
    private static List<String> unqualify(List<String> cols) {
        return cols.stream()
                   .map(c -> {
                       int dot = c.lastIndexOf('.');
                       return (dot >= 0 ? c.substring(dot + 1) : c);
                   })
                   .map(String::toLowerCase)
                   .collect(Collectors.toList());
    }

    /**
     * Determines if a materialized view can satisfy the user query.
     *
     * @param userMetadata Metadata from the user query.
     * @param mvMetadata   Metadata from the materialized view.
     * @return True if the materialized view matches the query, false otherwise.
     */
    public static boolean isMatch(MaterializedViewMetadata userMetadata, MaterializedViewMetadata mvMetadata) {
        // 1. Base table must match exactly
        if (!userMetadata.getBaseTable().equals(mvMetadata.getBaseTable())) {
            logger.info("Base table mismatch: query uses '{}', MV uses '{}'.",
                         userMetadata.getBaseTable(), mvMetadata.getBaseTable());
            return false;
        }

        // 2. GROUP BY: MV must contain all user GROUP BY columns (after stripping qualifiers)
        List<String> userGbs = unqualify(userMetadata.getGroupByColumns());
        List<String> mvGbs   = unqualify(mvMetadata.getGroupByColumns());
        if (!mvGbs.containsAll(userGbs)) {
            logger.info("Group-by mismatch. Query needs {}, MV has {}.", userGbs, mvGbs);
            return false;
        }

        // 3. Aggregations: MV must provide all user aggregation expressions
        List<String> userAggs = unqualify(userMetadata.getAggregations());
        List<String> mvAggs   = unqualify(mvMetadata.getAggregations());
        if (!mvAggs.containsAll(userAggs)) {
            List<String> missingAggs = userAggs.stream()
                                               .filter(a -> !mvAggs.contains(a))
                                               .collect(Collectors.toList());
            logger.info("Aggregation mismatch. Query requires {}, MV provides {}. Missing {}",
                         userAggs, mvAggs, missingAggs);
            return false;
        }

        // 4. Filters: MV projections + group-by columns must cover user WHERE columns
        List<String> userFilters = unqualify(userMetadata.getFilterColumns());

        // Combine MV projections and groupBy as available
        Set<String> mvAvailable = new HashSet<>();
        mvAvailable.addAll(unqualify(mvMetadata.getProjectionColumns()));
        mvAvailable.addAll(mvGbs);
        List<String> mvAvailList = new ArrayList<>(mvAvailable);

        if (!mvAvailable.containsAll(userFilters)) {
            List<String> missing = userFilters.stream()
                                              .filter(f -> !mvAvailable.contains(f))
                                              .collect(Collectors.toList());
            logger.info("Filter mismatch. Query needs {}, MV has {}. Missing {}",
                         userFilters, mvAvailList, missing);
            return false;
        }

        // All checks passed
        return true;
    }
}
