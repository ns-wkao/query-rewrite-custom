package com.poc.rewrite;

import com.poc.rewrite.config.MaterializedViewMetadata;
// import com.poc.rewrite.QueryMetadataExtractor;
// import io.trino.sql.tree.Statement; 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
// List, Map, Set, Collectors imports are used by the remaining isMatch method
import java.util.Set; 
import java.util.stream.Collectors;

public class QueryMatcher {

    private static final Logger logger = LoggerFactory.getLogger(QueryMatcher.class);

    /**
     * Determines if a materialized view can satisfy the user query.
     *
     * @param userMetadata Metadata from the user query.
     * @param mvMetadata Metadata from the materialized view.
     * @return True if the materialized view matches the query, false otherwise.
     */
    public static boolean isMatch(MaterializedViewMetadata userMetadata, MaterializedViewMetadata mvMetadata) {
        // Check base table match
        if (!userMetadata.getBaseTable().equals(mvMetadata.getBaseTable())) {
            logger.debug("Base table mismatch: query uses '{}', MV uses '{}'.",
                    userMetadata.getBaseTable(), mvMetadata.getBaseTable());
            return false;
        }

        // Check non-aggregated (group-by) columns
        // The MV must contain all group-by columns required by the user query.
        if (!mvMetadata.getGroupByColumns().containsAll(userMetadata.getGroupByColumns())) {
            logger.debug("Group-by columns mismatch. Query requires: {}, MV provides: {}.",
                    userMetadata.getGroupByColumns(), mvMetadata.getGroupByColumns());
            return false;
        }

        // Check aggregated columns by function name
        // The MV must provide all types of aggregations (e.g., SUM, COUNT) required by the user query.
        List<String> userAggs = userMetadata.getAggregations();
        List<String> mvAggs = mvMetadata.getAggregations();

        if (!mvAggs.containsAll(userAggs)) {
            logger.debug("Aggregation mismatch. Query requires aggregations: {}, MV provides aggregations: {}. User aggs not found in MV: {}",
                    userAggs, mvAggs,
                    userAggs.stream().filter(s -> !mvAggs.contains(s)).collect(Collectors.toList()));
            return false;
        }
        
        Set<String> userProjectionStrings = userMetadata.getProjectionColumns().stream()
                .collect(Collectors.toSet());
        Set<String> mvProjectionStrings = mvMetadata.getProjectionColumns().stream()
                .collect(Collectors.toSet());

        if (!mvProjectionStrings.containsAll(userProjectionStrings)) {
            logger.debug("Projection columns mismatch. Query requires projection items: {}, MV provides projection items: {}. User query items not found in MV: {}",
                    userProjectionStrings, mvProjectionStrings,
                    userProjectionStrings.stream().filter(s -> !mvProjectionStrings.contains(s)).collect(Collectors.toSet()));
            return false;
        }

        

        // Check if MV provides all columns needed by user's WHERE clause
        List<String> userFilterCols = userMetadata.getFilterColumns();
        Set<String> mvAvailableCols = new HashSet<>(mvMetadata.getProjectionColumns());
        mvAvailableCols.addAll(mvMetadata.getGroupByColumns()); // Group-by cols can also be used

        logger.info("QueryMatcher DEBUG - MV Key (if known, otherwise N/A)"); // You might not have MV key here easily
        logger.info("QueryMatcher DEBUG - User Filter Cols: {}", userFilterCols);
        logger.info("QueryMatcher DEBUG - MV Available Cols (for filter check): {}", mvAvailableCols);

        if (!mvAvailableCols.containsAll(userFilterCols)) {
            logger.debug("Filter columns mismatch. Query WHERE needs: {}, MV provides (Projections + GroupBy): {}. Missing: {}",
                    userFilterCols, mvAvailableCols,
                    userFilterCols.stream().filter(s -> !mvAvailableCols.contains(s)).collect(Collectors.toList()) );
            return false;
        }

        return true;
    }
}