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
import java.util.*;
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
            config.getMaterializedViews().forEach((mvName, def) -> {
                try {
                    Statement mvStmt = sqlParser.createStatement(def.getDefinition());
                    MaterializedViewMetadata metadata = QueryMetadataExtractor.extractMetadataFromQuery(mvStmt);
                    mvMetadataMap.put(mvName, metadata);
                    logger.info("Loaded MV '{}': baseTable={}, cols={}, aggs={}, groupBy={}",
                            mvName,
                            metadata.getBaseTable(),
                            metadata.getProjectionColumns(),
                            metadata.getAggregations(),
                            metadata.getGroupByColumns());
                } catch (Exception e) {
                    logger.error("Failed to load MV '{}': {}", mvName, e.getMessage());
                }
            });
        }
        logger.info("Initialized with {} materialized views.", mvMetadataMap.size());
    }

    public String processUserQuery(String sql) {
        //logger.info("Processing query: {}", sql);
        Statement stmt = sqlParser.createStatement(sql);

        try {
            if (stmt instanceof Query) {
                Query query = (Query) stmt;

                // 1) CTE‐rewriting path
                if (query.getWith().isPresent()) {
                    With with = query.getWith().get();
                    QueryBody body = query.getQueryBody();

                    for (WithQuery cte : with.getQueries()) {
                        String name = cte.getName().getValue();
                        
                        // FIXED: Extract metadata from the ENTIRE query (including main SELECT)
                        // not just the CTE definition
                        MaterializedViewMetadata cteMeta = QueryMetadataExtractor.extractMetadataFromQuery(stmt);

                        // figure out what the outer query actually needs from this CTE
                        QueryMetadataExtractor.SourceColumnCollectorVisitor collector =
                                new QueryMetadataExtractor.SourceColumnCollectorVisitor(name);
                        Set<String> needed = new HashSet<>();
                        collector.process(body, needed);

                        boolean outerUsesStar = collector.isStarFound();
                        boolean cteHasStar    = cteMeta.getProjectionColumns().contains("*");

                        for (Map.Entry<String, MaterializedViewMetadata> e : mvMetadataMap.entrySet()) {
                            MaterializedViewMetadata mvMeta = e.getValue();
                            boolean mvIsAgg = !mvMeta.getGroupByColumns().isEmpty();

                            // CASE A: CTE did a SELECT * but outer never did cte.*, so only 'needed' cols matter
                            if (cteHasStar && !outerUsesStar) {
                                logger.info("CTE has * but outer query is limited to specific columns");
                                if (!mvProvidesAll(mvMeta, needed)) {
                                    logger.info("MV does not cover required columns");
                                    continue;
                                }
                            }
                            // CASE B: otherwise fall back to full-match logic
                            else {
                                // also guard: if outer did use cte.*, and MV is aggregated we can't rewrite
                                if (outerUsesStar && mvIsAgg) {
                                    logger.info("Outer query has * but MV is aggregation");
                                    continue;
                                }
                                logger.info("Matching {} in CTE", e.getKey());
                                if (!QueryMatcher.isMatch(cteMeta, mvMeta)) {
                                    continue;
                                }
                            }

                            // CTE→MV match found
                            logger.info("Rewriting CTE '{}' using MV '{}'.", name, e.getKey());
                            String target = pocConfig.getMaterializedViews()
                                                    .get(e.getKey())
                                                    .getTargetTable();
                            return rewriteAst(stmt, cteMeta, target);
                        }
                    }
                }

                // 2) Main‐query fallback
                MaterializedViewMetadata mainMeta = QueryMetadataExtractor.extractMetadataFromQuery(stmt);
                boolean mainHasStar = mainMeta.getProjectionColumns().contains("*");

                for (Map.Entry<String, MaterializedViewMetadata> e : mvMetadataMap.entrySet()) {
                    MaterializedViewMetadata mvMeta = e.getValue();
                    boolean mvIsAgg = !mvMeta.getGroupByColumns().isEmpty();

                    // if the final SELECT is a bare "*", aggregated MV can't satisfy it
                    if (mainHasStar && mvIsAgg) {
                        continue;
                    }
                    logger.info("Matching {} in main query", e.getKey());
                    if (!QueryMatcher.isMatch(mainMeta, mvMeta)) {
                        continue;
                    }

                    // check that the MV actually provides all explicit columns & filters
                    Set<String> needed = new HashSet<>();
                    if (!mainHasStar) {
                        // non-star: need every proj + filter
                        needed.addAll(mainMeta.getProjectionColumns().stream()
                                            .map(String::toLowerCase)
                                            .collect(Collectors.toSet()));
                    } else {
                        // star + matched: still need any explicit non-"*" projections
                        needed.addAll(mainMeta.getProjectionColumns().stream()
                                            .filter(col -> !col.equals("*"))
                                            .map(String::toLowerCase)
                                            .collect(Collectors.toSet()));
                    }
                    // filters always count
                    needed.addAll(mainMeta.getFilterColumns().stream()
                                        .map(String::toLowerCase)
                                        .collect(Collectors.toSet()));

                    if (!mvProvidesAll(mvMeta, needed)) {
                        continue;
                    }

                    logger.info("Rewriting main query using MV '{}'.", e.getKey());
                    String target = pocConfig.getMaterializedViews()
                                            .get(e.getKey())
                                            .getTargetTable();
                    return rewriteAst(stmt, mainMeta, target);
                }
            }
            else {
                logger.warn("Not a SELECT query, skipping rewrite.");
            }
        }
        catch (Exception ex) {
            logger.error("Rewrite failed: {}", ex.getMessage(), ex);
        }

        logger.info("No rewrite applied, returning original SQL.");
        return sql;
    }


    private String rewriteAst(Statement original, MaterializedViewMetadata meta, String mvTarget) {
        String base = meta.getBaseTable();
        
        // Fixed QualifiedName creation - handle single part vs multiple parts
        String[] parts = mvTarget.split("\\.");
        QualifiedName target;
        if (parts.length == 1) {
            target = QualifiedName.of(parts[0]);
        } else {
            // For multiple parts, use the varargs version: of(first, rest...)
            String first = parts[0];
            String[] rest = new String[parts.length - 1];
            System.arraycopy(parts, 1, rest, 0, parts.length - 1);
            target = QualifiedName.of(first, rest);
        }
        
        TableReplacingVisitor v = new TableReplacingVisitor(base, target);
        Statement out = (Statement) v.process(original, null);
        if (!v.didChange()) {
            logger.warn("AST rewrite did not change anything.");
        }
        return SqlFormatter.formatSql(out);
    }

    /**
     * Check if MV metadata provides all needed columns/expressions.
     */
    private boolean mvProvidesAll(MaterializedViewMetadata mvMeta, Set<String> needed) {
        Set<String> avail = new HashSet<>();
        mvMeta.getProjectionColumns().forEach(c -> avail.add(c.toLowerCase()));
        mvMeta.getAggregations().forEach(c -> avail.add(c.toLowerCase()));
        mvMeta.getGroupByColumns().forEach(c -> avail.add(c.toLowerCase()));
        
        boolean result = avail.containsAll(needed);
        
        if (!result) {
            Set<String> missing = new HashSet<>(needed);
            missing.removeAll(avail);
            logger.debug("MV does not provide all needed columns. Missing: {}, Available: {}, Needed: {}", 
                    missing, avail, needed);
        }
        
        return result;
    }

    public static PocConfig loadConfig(String filename) {
        LoaderOptions opts = new LoaderOptions();
        opts.setAllowDuplicateKeys(false);
        Yaml yaml = new Yaml(new Constructor(PocConfig.class, opts));
        try (InputStream in = SQLRewritePoc.class.getClassLoader().getResourceAsStream(filename)) {
            if (in == null) {
                throw new RuntimeException("Config not found: " + filename);
            }
            return yaml.load(in);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config: " + e.getMessage(), e);
        }
    }
}