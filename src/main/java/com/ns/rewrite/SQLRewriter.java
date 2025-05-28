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
    private final Set<String> knownBaseTables = new HashSet<>();
    private final Map<String, TableDefinition> tableDefinitions = new HashMap<>();


    private static class RewriteCandidate {
        final String mvName;
        final String mvTargetTable;
        final String baseTableToReplace;

        RewriteCandidate(String mvName, String mvTargetTable, String baseTableToReplace) {
            this.mvName = mvName;
            this.mvTargetTable = mvTargetTable;
            this.baseTableToReplace = baseTableToReplace;
        }
    }

    public SQLRewriter(TableConfig config) {
        this.tableConfig = Objects.requireNonNull(config, "TableConfig cannot be null");
        this.sqlParser = new SqlParser();

        // Store known base tables and their definitions
        if (config.getTables() != null) {
            this.knownBaseTables.addAll(config.getTables().keySet());
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
                    logger.info("Loaded MV '{}': baseTable={}", mvName, metadata.getBaseTable());
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

            // 1. Extract initial metadata
            QueryMetadata userMeta = QueryMetadataExtractor.extractMetadataFromQuery(query);
            logger.info("User query initial metadata extracted for base table: {}", userMeta.getBaseTable());

            // 2. Find a suitable MV candidate using metadata
            RewriteCandidate candidate = findRewriteCandidate(userMeta);

            // 3. Perform rewrite if a candidate is found
            if (candidate != null) {
                logger.info("Found suitable MV '{}'. Rewriting query.", candidate.mvName);
                return rewriteAst(stmt, candidate.baseTableToReplace, candidate.mvTargetTable);
            } else {
                logger.info("No suitable MV found. Returning original query.");
                return sql;
            }

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
     * Finds a candidate using the full user metadata.
     */
    private RewriteCandidate findRewriteCandidate(QueryMetadata userMeta) {
        for (Map.Entry<String, QueryMetadata> entry : mvMetadataMap.entrySet()) {
            String mvName = entry.getKey();
            QueryMetadata mvMeta = entry.getValue();

            logger.info("Checking MV '{}' against user query...", mvName);

            // Use QueryMatcher with full metadata
            RewriteMatcher.MatchResult result = RewriteMatcher.canSatisfy(userMeta, mvMeta);

            if (result.canSatisfy) {
                MaterializedViewDefinition mvDef = tableConfig.getMaterializedViews().get(mvName);
                if (mvDef != null) {
                    return new RewriteCandidate(mvName, mvDef.getTargetTable(), userMeta.getBaseTable());
                } else {
                    logger.error("Internal error: Found matching MV '{}' but couldn't find its definition.", mvName);
                }
            } else {
                // Log the detailed reasons for the mismatch.
                logger.info("--> MV '{}' does not match. Reasons:", mvName);
                for (String reason : result.mismatchReasons) {
                    logger.info("    - {}", reason);
                }
            }
        }
        return null;
    }

    private String rewriteAst(Statement originalStatement, String baseTableToReplace, String mvTargetTable) {
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
        logger.debug("Rewriting AST: Replacing '{}' with '{}'", baseTableToReplace, targetQualifiedName);
        TableReplacerVisitor visitor = new TableReplacerVisitor(baseTableToReplace, targetQualifiedName);
        Statement rewrittenStatement = (Statement) visitor.process(originalStatement, null);

        if (!visitor.didChange()) {
            logger.warn("TableReplacingVisitor ran but did not report any changes. Base table name ('{}') might be incorrect or not found in AST.", baseTableToReplace);
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