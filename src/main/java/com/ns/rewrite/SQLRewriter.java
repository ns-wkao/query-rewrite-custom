package com.ns.rewrite;

import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.*;
import io.trino.sql.SqlFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.ns.rewrite.analysis.QueryMetadataExtractor;
import com.ns.rewrite.config.MaterializedViewDefinition;
import com.ns.rewrite.config.TableConfig;
import com.ns.rewrite.config.TableDefinition;
import com.ns.rewrite.model.QueryMetadata;
import com.ns.rewrite.rewriting.RewriteMatcher;
import com.ns.rewrite.rewriting.TableReplacerVisitor;

import java.io.InputStream;
import java.util.*;

public class SQLRewriter {

    private static final Logger logger = LoggerFactory.getLogger(SQLRewriter.class);
    private final TableConfig tableConfig;
    private final SqlParser sqlParser;
    private final Map<String, QueryMetadata> mvMetadataMap = new LinkedHashMap<>();
    private final Map<String, TableDefinition> tableDefinitions = new HashMap<>();


    private static class RewriteCandidate {
        final String mvName;
        final String mvTargetTable;
        final String baseTableInUserQueryToReplace;;

        RewriteCandidate(String mvName, String mvTargetTable, String baseTableInUserQueryToReplace) {
            this.mvName = mvName;
            this.mvTargetTable = mvTargetTable;
            this.baseTableInUserQueryToReplace = baseTableInUserQueryToReplace;
        }
    }

    public SQLRewriter(TableConfig config) {
        this.tableConfig = Objects.requireNonNull(config, "TableConfig cannot be null");
        this.sqlParser = new SqlParser();
        if (config.getTables() != null) {
            this.tableDefinitions.putAll(config.getTables());
        }

        if (config.getMaterializedViews() != null) {
            config.getMaterializedViews().forEach((mvName, def) -> {
                try {
                    logger.info("Loading MV definition for '{}'", mvName);
                    Statement mvStmt = sqlParser.createStatement(def.getDefinition());
                    QueryMetadata metadata = QueryMetadataExtractor.extractMetadataFromQuery(mvStmt); 
                    if (metadata.getProjectionColumns().contains("*")) {
                        logger.error("MV '{}' uses '*' which is not allowed. Skipping.", mvName);
                        return;
                    }
                    mvMetadataMap.put(mvName, metadata);
                    logger.info("Finished loading MV '{}': baseTable={}", mvName, metadata.getBaseTable());
                } catch (ParsingException | IllegalArgumentException e) {
                    logger.error("Failed to load or parse MV '{}': {}", mvName, e.getMessage(), e);
                } catch (Exception e) {
                    logger.error("Unexpected error loading MV '{}': {}", mvName, e.getMessage(), e);
                }
            });
        }
        logger.info("Initialized with {} materialized views. Order: {}", mvMetadataMap.size(), mvMetadataMap.keySet());
    }

    public String processUserQuery(String sql) {
        logger.info("--- Processing Query ---");
        logger.debug("Original SQL: {}", sql.replace('\n', ' '));

        try {
            Statement stmt = sqlParser.createStatement(sql);

            if (!(stmt instanceof Query)) {
                logger.warn("Not a SELECT query, skipping rewrite.");
                return sql;
            }
            Query query = (Query) stmt;

            QueryMetadata userMeta = QueryMetadataExtractor.extractMetadataFromQuery(query);
            List<String> userBaseTables = userMeta.getAllBaseTables();

            if (userBaseTables == null || userBaseTables.isEmpty()) {
                logger.info("No base tables found in user query. Skipping rewrite search.");
                return sql;
            }
            
            logger.info("User query metadata extracted. Identified base tables for potential replacement: {}", userBaseTables);

            RewriteCandidate candidate = null;
            for (String tableToReplace : userBaseTables) {
                logger.info("Attempting to find MV to replace user query table: '{}'", tableToReplace);
                candidate = findRewriteCandidate(userMeta, tableToReplace);
                if (candidate != null) {
                    logger.info("Found suitable MV '{}' to replace '{}'. Rewriting query.", candidate.mvName, tableToReplace);
                    return rewriteAst(stmt, candidate.baseTableInUserQueryToReplace, candidate.mvTargetTable);
                }
            }

            logger.info("No suitable MV found for any base table. Returning original query.");
            return sql;

        } catch (UnsupportedOperationException e) {
            logger.warn("Query not supported for rewrite (likely needs '*' from base table): {}", e.getMessage());
            return sql;
        }
        catch (Exception ex) {
            logger.error("Error during query processing or rewrite: {}", ex.getMessage(), ex);
            return sql;
        } finally {
            logger.info("--- Finished Processing ---");
        }
    }


    /**
     * Finds a candidate MV to replace a specific table in the user query.
     * @param userMeta The metadata of the entire user query.
     * @param actualUserQueryTableToReplace The specific base table from the user query we are trying to replace.
     * @return A RewriteCandidate if a suitable MV is found, otherwise null.
     */
    private RewriteCandidate findRewriteCandidate(QueryMetadata userMeta, String actualUserQueryTableToReplace) {
        for (Map.Entry<String, QueryMetadata> entry : mvMetadataMap.entrySet()) {
            String mvName = entry.getKey();
            QueryMetadata mvMeta = entry.getValue();

            logger.info("Checking MV '{}' (based on '{}') against user query table '{}'...", 
                        mvName, mvMeta.getBaseTable(), actualUserQueryTableToReplace);

            if (!mvMeta.getBaseTable().equalsIgnoreCase(actualUserQueryTableToReplace)) {
                logger.debug("--> MV '{}' is for base table '{}', not a match for user query table '{}'. Skipping.", 
                             mvName, mvMeta.getBaseTable(), actualUserQueryTableToReplace);
                continue;
            }

            // Get schema definition for table we're trying to replace
            String tableKeyInConfig = tableDefinitions.keySet().stream()
                .filter(key -> key.equalsIgnoreCase(actualUserQueryTableToReplace))
                .findFirst()
                .orElse(actualUserQueryTableToReplace);

            TableDefinition targetTableSchema = tableDefinitions.get(tableKeyInConfig);
            if (targetTableSchema == null) {
                logger.warn("--> MV '{}' matches user query table '{}', but schema definition for '{}' (key: '{}') not found in TableConfig. Cannot verify column availability precisely. Skipping MV.",
                             mvName, actualUserQueryTableToReplace, actualUserQueryTableToReplace, tableKeyInConfig);
                continue; 
            }

            // Pass userMeta, mvMeta, the specific user table being targeted, and its schema to the matcher.
            RewriteMatcher.MatchResult result = RewriteMatcher.canSatisfy(userMeta, mvMeta, actualUserQueryTableToReplace, targetTableSchema);
            if (result.canSatisfy) {
                MaterializedViewDefinition mvDef = tableConfig.getMaterializedViews().get(mvName);
                if (mvDef != null) {
                    // The candidate stores the table name *from the user query* that gets replaced.
                    return new RewriteCandidate(mvName, mvDef.getTargetTable(), actualUserQueryTableToReplace);
                } else {
                    logger.error("Internal error: Found matching MV '{}' but couldn't find its definition.", mvName);
                }
            } else {
                logger.info("--> MV '{}' does not match for replacing table '{}'. Reasons:", mvName, actualUserQueryTableToReplace);
                for (String reason : result.mismatchReasons) {
                    logger.info("    - {}", reason);
                }
            }
        }
        return null;
    }

    private String rewriteAst(Statement originalStatement, String baseTableInUserQueryToReplace, String mvTargetTable) {
        String[] parts = mvTargetTable.split("\\.");
        QualifiedName targetQualifiedName;
        if (parts.length == 1) {
            targetQualifiedName = QualifiedName.of(parts[0]);
        } else {
            String first = parts[0];
            String[] rest = new String[parts.length - 1];
            System.arraycopy(parts, 1, rest, 0, parts.length - 1);
            targetQualifiedName = QualifiedName.of(first, rest);
        }
        logger.debug("Rewriting AST: Replacing '{}' with '{}'", baseTableInUserQueryToReplace, targetQualifiedName);
        TableReplacerVisitor visitor = new TableReplacerVisitor(baseTableInUserQueryToReplace, targetQualifiedName);
        Statement rewrittenStatement = (Statement) visitor.process(originalStatement, null);

        if (!visitor.didChange()) {
            logger.warn("TableReplacingVisitor ran but did not report any changes. Base table name ('{}') might be incorrect or not found in AST.", baseTableInUserQueryToReplace);
        } else {
             logger.info("AST rewrite successful.");
        }
        return SqlFormatter.formatSql(rewrittenStatement);
    }

    public static TableConfig loadConfig(String filename) {
        LoaderOptions opts = new LoaderOptions();
        opts.setAllowDuplicateKeys(false);
        Yaml yaml = new Yaml(new Constructor(TableConfig.class, opts));
        try (InputStream in = SQLRewriter.class.getClassLoader().getResourceAsStream(filename)) {
            if (in == null) {
                throw new RuntimeException("Config file not found in classpath: " + filename);
            }
            return yaml.load(in);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load or parse config: " + filename, e);
        }
    }
}