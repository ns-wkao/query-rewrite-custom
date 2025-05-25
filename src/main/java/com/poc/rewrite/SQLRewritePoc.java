package com.poc.rewrite;

import com.poc.rewrite.config.MaterializedViewDefinition;
import com.poc.rewrite.config.MaterializedViewMetadata;
import com.poc.rewrite.config.PocConfig;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.*;
import io.trino.sql.SqlFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class SQLRewritePoc {

    private static final Logger logger = LoggerFactory.getLogger(SQLRewritePoc.class);
    private final PocConfig pocConfig;
    private final SqlParser sqlParser;
    private final Map<String, MaterializedViewMetadata> mvMetadataMap = new HashMap<>();

    public SQLRewritePoc(PocConfig config) {
        this.pocConfig = Objects.requireNonNull(config, "PocConfig cannot be null");
        this.sqlParser = new SqlParser();

        if (config.getMaterializedViews() != null) {
            for (Map.Entry<String, MaterializedViewDefinition> entry : config.getMaterializedViews().entrySet()) {
                String mvName = entry.getKey();
                String mvSql = entry.getValue().getDefinition();
                try {
                    Statement mvStmt = sqlParser.createStatement(mvSql);
                    MaterializedViewMetadata metadata = QueryMetadataExtractor.extractMetadataFromQuery(mvStmt);
                    mvMetadataMap.put(mvName, metadata);
                    logger.info("Extracted metadata for MV '{}': baseTable={}, projections={}, aggregations={}, groupBy={}.",
                            mvName, metadata.getBaseTable(), metadata.getProjectionColumns(),
                            metadata.getAggregations().keySet(), metadata.getGroupByColumns());
                } catch (Exception e) {
                    logger.error("Failed to parse or extract metadata for MV '{}': {}", mvName, e.getMessage());
                }
            }
        }
        logger.info("MaterializedViewPoc initialized. {} MVs loaded for potential rewrite.", mvMetadataMap.size());
    }

    public Statement parseSql(String sql) {
        return sqlParser.createStatement(sql);
    }

    public String processUserQuery(String userQuerySql) {
        logger.info("Processing user query: {}", userQuerySql);
        Statement userQueryStatement = parseSql(userQuerySql); // This is the top-level statement

        try {
            // Attempt to rewrite within CTEs first
            if (userQueryStatement instanceof Query) {
                Query query = (Query) userQueryStatement;
                Optional<With> withClauseOpt = query.getWith();

                if (withClauseOpt.isPresent()) {
                    With withClause = withClauseOpt.get();
                    logger.info("Query contains WITH clause. Analyzing {} CTE(s).", withClause.getQueries().size());

                    for (WithQuery cte : withClause.getQueries()) {
                        logger.info("Analyzing CTE: '{}'", cte.getName().getValue());
                        Query cteDefiningQuery = cte.getQuery(); // This is the Query node that defines the CTE

                        // Extract metadata specifically for this CTE's defining query
                        // Look into subqueries within CTE
                        MaterializedViewMetadata cteMetadata = QueryMetadataExtractor.extractMetadataFromQuery(cteDefiningQuery);
                        logger.info("Extracted metadata for CTE '{}': baseTable={}, alias={}, projections={}",
                                cte.getName().getValue(),
                                cteMetadata.getBaseTable(),
                                cteMetadata.getTableAlias().orElse("N/A"),
                                cteMetadata.getProjectionColumns());


                        // Try to match this CTE's metadata with available MVs
                        for (Map.Entry<String, MaterializedViewMetadata> mvEntry : mvMetadataMap.entrySet()) {
                            String mvConfigKey = mvEntry.getKey();
                            MaterializedViewMetadata mvMetadata = mvEntry.getValue();

                            if (QueryMatcher.isMatch(cteMetadata, mvMetadata)) {
                                logger.info("CTE '{}' (defining query) matches MV configuration with key '{}'.",
                                        cte.getName().getValue(), mvConfigKey);

                                MaterializedViewDefinition mvDefinition = pocConfig.getMaterializedViews().get(mvConfigKey);
                                if (mvDefinition == null || mvDefinition.getTargetTable() == null || mvDefinition.getTargetTable().trim().isEmpty()) {
                                    logger.error("Matched MV (config key: '{}') for CTE '{}' is missing a valid 'targetTable'. Cannot rewrite using this MV.",
                                            mvConfigKey, cte.getName().getValue());
                                    continue;
                                }
                                String mvTargetTableNameInSql = mvDefinition.getTargetTable();

                                logger.info("Attempting to rewrite original query by replacing table '{}' (found in CTE '{}') with actual MV name '{}'.",
                                        cteMetadata.getBaseTable(), // This baseTable is from inside the CTE
                                        cte.getName().getValue(),
                                        mvTargetTableNameInSql);

                                String rewrittenSql = rewriteQueryUsingAst(userQueryStatement, cteMetadata, mvTargetTableNameInSql);
                                if (!rewrittenSql.equals(SqlFormatter.formatSql(userQueryStatement))) { // Basic check if SQL changed
                                    logger.info("Successfully rewrote query based on CTE '{}' match.", cte.getName().getValue());
                                    return rewrittenSql;
                                } else {
                                    logger.warn("Rewrite for CTE '{}' was attempted with MV '{}' but the resulting SQL was unchanged. " +
                                                "This might indicate an issue in the TableReplacingVisitor or metadata consistency.",
                                                cte.getName().getValue(), mvTargetTableNameInSql);
                                }
                            }
                        }
                    }
                    logger.info("Finished analyzing CTEs. No successful rewrites performed within CTE definitions.");
                } else {
                    logger.info("Query does not contain a WITH clause.");
                }

                // --- Fallback/Original Logic: Analyze the main query body ---
                // This will only be reached if no CTEs were rewritten and returned.
                logger.info("Proceeding to analyze main query body.");
                MaterializedViewMetadata mainQueryMetadata = QueryMetadataExtractor.extractMetadataFromQuery(userQueryStatement);
                logger.info("Extracted metadata for Main Query: baseTable={}, alias={}, projections={}",
                        mainQueryMetadata.getBaseTable(),
                        mainQueryMetadata.getTableAlias().orElse("N/A"),
                        mainQueryMetadata.getProjectionColumns());


                for (Map.Entry<String, MaterializedViewMetadata> mvEntry : mvMetadataMap.entrySet()) {
                    String mvConfigKey = mvEntry.getKey();
                    MaterializedViewMetadata mvMetadata = mvEntry.getValue();

                    if (QueryMatcher.isMatch(mainQueryMetadata, mvMetadata)) {
                        logger.info("Main query body matches MV configuration with key '{}'.", mvConfigKey);
                        MaterializedViewDefinition mvDefinition = pocConfig.getMaterializedViews().get(mvConfigKey);
                        if (mvDefinition == null || mvDefinition.getTargetTable() == null || mvDefinition.getTargetTable().trim().isEmpty()) {
                            logger.error("Matched MV (config key: '{}') for main query is missing 'targetTable'. Cannot rewrite.", mvConfigKey);
                            continue;
                        }
                        String mvTargetTableNameInSql = mvDefinition.getTargetTable();
                        logger.info("Rewriting main query AST to use actual MV name '{}'.", mvTargetTableNameInSql);
                        return rewriteQueryUsingAst(userQueryStatement, mainQueryMetadata, mvTargetTableNameInSql);
                    }
                }
            } else {
                logger.warn("Input statement is not a Query. Skipping rewrite attempt. Statement type: {}", userQueryStatement.getClass().getSimpleName());
            }

        } catch (IllegalArgumentException e) {
            // Catch exceptions from QueryMetadataExtractor (e.g., unsupported structures)
            logger.warn("Could not extract metadata for a query part, skipping rewrite for that part: {}", e.getMessage());
        } catch (Exception e) {
            // Catch other potential errors during processing
            logger.error("Error during query processing pipeline: {}", e.getMessage(), e);
        }

        logger.warn("No suitable materialized view found for rewrite in any part of the query: {}", userQuerySql);
        return userQuerySql; // Return original query if no match or an error occurred
    }

    private String rewriteQueryUsingAst(Statement userQueryStatement, MaterializedViewMetadata userQueryMetadata, String mvTargetTableNameInSql) {
        String originalTableIdentifierString = userQueryMetadata.getBaseTable();
        logger.info("Rewriting query AST. Replacing original table '{}' with MV '{}'.", 
                    originalTableIdentifierString, mvTargetTableNameInSql);
                    
        // Construct QualifiedName for the MV, ensuring each part is treated as a delimited (quoted) identifier.
        List<Identifier> delimitedNameParts = Arrays.stream(mvTargetTableNameInSql.split("\\."))
                .map(part -> new Identifier(part, true)) // true sets the 'delimited' flag
                .collect(Collectors.toList());
        QualifiedName mvQualifiedName = QualifiedName.of(delimitedNameParts);

        TableReplacingVisitor visitor = new TableReplacingVisitor(originalTableIdentifierString, mvQualifiedName);
        Statement rewrittenStatementNode = (Statement) visitor.process(userQueryStatement, null);

        if (rewrittenStatementNode == null) {
            logger.error("AST rewrite resulted in a null statement when trying to use MV '{}'.", mvTargetTableNameInSql);
            throw new IllegalStateException("AST rewrite resulted in a null statement.");
        }

        if (!visitor.didChange()) {
            logger.warn("AST rewrite using MV '{}' did not alter the statement object, according to visitor's tracking. " +
                        "This implies the table to replace ('{}') was not found as expected. " +
                        "QueryMatcher indicated a match, so this is unexpected. Formatting original/unaltered statement.",
                        mvTargetTableNameInSql, originalTableIdentifierString);
        }
        
        return SqlFormatter.formatSql(rewrittenStatementNode);
    }


    public static PocConfig loadConfig(String configFileName) {
        LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setAllowDuplicateKeys(false);
        Yaml yaml = new Yaml(new Constructor(PocConfig.class, loaderOptions));
        try (InputStream inputStream = SQLRewritePoc.class
                .getClassLoader()
                .getResourceAsStream(configFileName)) {
            if (inputStream == null) {
                logger.error("Configuration file '{}' not found in classpath.", configFileName);
                throw new RuntimeException("Configuration file '" + configFileName + "' not found.");
            }
            return yaml.load(inputStream);
        } catch (Exception e) {
            logger.error("Error loading configuration from '{}': {}", configFileName, e.getMessage(), e);
            throw new RuntimeException("Error loading configuration.", e);
        }
    }
}