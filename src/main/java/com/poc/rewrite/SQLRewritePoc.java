package com.poc.rewrite;

import com.poc.rewrite.config.MaterializedViewDefinition;
import com.poc.rewrite.config.MaterializedViewMetadata;
import com.poc.rewrite.config.PocConfig;
import com.poc.rewrite.config.TableDefinition;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.*;
import io.trino.sql.SqlFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.*;

public class SQLRewritePoc {

    private static final Logger logger = LoggerFactory.getLogger(SQLRewritePoc.class);
    private final PocConfig pocConfig;
    private final SqlParser sqlParser;
    private final Map<String, MaterializedViewMetadata> mvMetadataMap = new HashMap<>();
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

    public SQLRewritePoc(PocConfig config) {
        this.pocConfig = Objects.requireNonNull(config, "PocConfig cannot be null");
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
                    // Extract MV metadata - ensure it *doesn't* use *
                    MaterializedViewMetadata metadata = QueryMetadataExtractor.extractMetadataFromQuery(mvStmt);

                    // Validate: MVs should not use '*' (as per our plan)
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
        logger.info("Initialized with {} materialized views.", mvMetadataMap.size());
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
            MaterializedViewMetadata initialUserMeta = QueryMetadataExtractor.extractMetadataFromQuery(query);
            logger.info("User query initial metadata extracted for base table: {}", initialUserMeta.getBaseTable());

            // 2. Trace required columns
            RequiredColumnTracer tracer = new RequiredColumnTracer(query, knownBaseTables);
            Map<String, Set<String>> requiredColsMap = tracer.getRequiredBaseColumns();

            // We only support single base table, so get its required columns.
            // If the map is empty or has multiple entries, we can't handle it now.
            if (requiredColsMap.size() != 1) {
                logger.warn("Query tracer found {} base tables. Only single-table queries are supported for rewrite. No rewrite.", requiredColsMap.size());
                return sql;
            }
            Set<String> requiredColumns = requiredColsMap.values().iterator().next();
            logger.info("Traced required columns: {}", requiredColumns);

            // 3. Create Refined Metadata
            MaterializedViewMetadata refinedUserMeta = new MaterializedViewMetadata();
            refinedUserMeta.setBaseTable(initialUserMeta.getBaseTable());
            refinedUserMeta.setTableAlias(initialUserMeta.getTableAlias());
            refinedUserMeta.setFilterColumns(initialUserMeta.getFilterColumns());
            refinedUserMeta.setAggregations(initialUserMeta.getAggregations());
            refinedUserMeta.setGroupByColumns(initialUserMeta.getGroupByColumns());
            refinedUserMeta.setProjectionColumns(new ArrayList<>(requiredColumns)); // Use traced columns

            // 4. Find a suitable MV candidate using refined metadata
            RewriteCandidate candidate = findRewriteCandidate(refinedUserMeta);

            // 5. Perform rewrite if a candidate is found
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
     * Finds a candidate using the *refined* user metadata.
     */
    private RewriteCandidate findRewriteCandidate(MaterializedViewMetadata refinedUserMeta) {
        for (Map.Entry<String, MaterializedViewMetadata> entry : mvMetadataMap.entrySet()) {
            String mvName = entry.getKey();
            MaterializedViewMetadata mvMeta = entry.getValue();

            logger.info("Checking MV '{}' against refined user query...", mvName);

            // Use QueryMatcher with refined (explicit) metadata
            if (QueryMatcher.canSatisfy(refinedUserMeta, mvMeta)) {
                MaterializedViewDefinition mvDef = pocConfig.getMaterializedViews().get(mvName);
                if (mvDef != null) {
                    return new RewriteCandidate(mvName, mvDef.getTargetTable(), refinedUserMeta.getBaseTable());
                } else {
                    logger.error("Internal error: Found matching MV '{}' but couldn't find its definition.", mvName);
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
            // For multiple parts, use the varargs version: of(first, rest...)
            String first = parts[0];
            String[] rest = new String[parts.length - 1];
            System.arraycopy(parts, 1, rest, 0, parts.length - 1);
            targetQualifiedName = QualifiedName.of(first, rest);
        }
        logger.debug("Rewriting AST: Replacing '{}' with '{}'", baseTableToReplace, targetQualifiedName);
        TableReplacingVisitor visitor = new TableReplacingVisitor(baseTableToReplace, targetQualifiedName);
        Statement rewrittenStatement = (Statement) visitor.process(originalStatement, null);

        if (!visitor.didChange()) {
            logger.warn("TableReplacingVisitor ran but did not report any changes. Base table name ('{}') might be incorrect or not found in AST.", baseTableToReplace);
        } else {
             logger.info("AST rewrite successful.");
        }
        return SqlFormatter.formatSql(rewrittenStatement);
    }

    public static PocConfig loadConfig(String filename) {
        LoaderOptions opts = new LoaderOptions();
        opts.setAllowDuplicateKeys(false);
        Yaml yaml = new Yaml(new Constructor(PocConfig.class, opts));
        try (InputStream in = SQLRewritePoc.class.getClassLoader().getResourceAsStream(filename)) {
            if (in == null) {
                throw new RuntimeException("Config file not found in classpath: " + filename);
            }
            return yaml.load(in);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load or parse config: " + filename, e);
        }
    }
}