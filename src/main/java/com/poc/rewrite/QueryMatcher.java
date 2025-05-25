package com.poc.rewrite;

import com.poc.rewrite.config.MaterializedViewMetadata;
// import com.poc.rewrite.QueryMetadataExtractor;
// import io.trino.sql.tree.Statement; 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        Set<String> userAggFunctionNames = userMetadata.getAggregations().keySet();
        Set<String> mvAggFunctionNames = mvMetadata.getAggregations().keySet();

        if (!mvAggFunctionNames.containsAll(userAggFunctionNames)) {
            logger.debug("Aggregation function types mismatch. Query requires functions: {}, MV provides functions: {}.",
                    userAggFunctionNames, mvAggFunctionNames);
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

        return true;
    }
}