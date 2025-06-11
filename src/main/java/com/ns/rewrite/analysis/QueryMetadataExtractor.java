package com.ns.rewrite.analysis;

import io.trino.sql.tree.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ns.rewrite.analysis.TemporalGranularityAnalyzer.TimeGranularity;
import com.ns.rewrite.model.AggregationInfo;
import com.ns.rewrite.model.QueryMetadata;

import java.util.*;
import java.util.stream.Collectors;

public class QueryMetadataExtractor {
    private static final Logger logger = LoggerFactory.getLogger(QueryMetadataExtractor.class);
    private static final Set<String> AGGREGATE_FUNCTIONS = Set.of("SUM", "COUNT", "AVG", "MIN", "MAX");

    public static QueryMetadata extractMetadataFromQuery(Statement queryStatement) {
        if (!(queryStatement instanceof Query)) {
            throw new IllegalArgumentException("Expected Query statement, got: " + queryStatement.getClass().getSimpleName());
        }

        logger.info("Starting metadata extraction from query");
        MetadataExtractor extractor = new MetadataExtractor();
        QueryMetadata metadata = extractor.extract((Query) queryStatement);
        
        logger.info("Metadata extraction completed for base table '{}' with {} projections, {} filters, {} aggregations, {} join columns, temporal granularity: {}", 
            metadata.getBaseTable(), 
            metadata.getProjectionColumns().size(),
            metadata.getFilterColumns().size(), 
            metadata.getAggregations().size(),
            metadata.getJoinColumns().size(),
            metadata.getTemporalGranularity());
            
        logger.debug("Detailed metadata - projections: {}, filters: {}, aggregations: {}, join columns: {}, temporal columns: {}", 
            metadata.getProjectionColumns(),
            metadata.getFilterColumns(), 
            metadata.getAggregations(),
            metadata.getJoinColumns(),
            metadata.getTemporalGroupByColumns());
        
        return metadata;
    }

    /**
     * Main extraction logic encapsulated in a single class.
     */
    private static class MetadataExtractor {
        private final Set<String> allProjections = new LinkedHashSet<>();
        private final Set<String> allFilters = new LinkedHashSet<>();
        private final Set<AggregationInfo> allAggregations = new LinkedHashSet<>();
        private final Set<String> allGroupBys = new LinkedHashSet<>();
        private final Set<String> allJoinColumns = new LinkedHashSet<>();
        private final Set<String> discoveredBaseTables = new LinkedHashSet<>();
        
        // Temporal analysis fields
        private TimeGranularity temporalGranularity = TimeGranularity.UNKNOWN;
        private final Set<String> temporalGroupByColumns = new LinkedHashSet<>();
        private final TemporalGranularityAnalyzer temporalAnalyzer = new TemporalGranularityAnalyzer();
        
        private Map<String, WithQuery> cteMap = new HashMap<>();
        private Map<String, Map<String, String>> cteColumnAliases = new HashMap<>(); // CTE name -> (alias -> original column)
        private Set<String> visitedCtes = new HashSet<>();
        private TableInfo primaryTable;

        QueryMetadata extract(Query query) {
            logger.debug("Initializing CTE map and processing query structure");
            initializeCteMap(query);
            
            logger.debug("Finding primary table from query");
            primaryTable = findPrimaryTable(query.getQueryBody());
            if (primaryTable != null) {
                logger.debug("Primary table identified: {}", primaryTable.name);
            } else {
                logger.warn("No primary table identified from query");
            }
            
            logger.debug("Collecting all base tables from query");
            collectAllTables(query.getQueryBody()); // Collects all base tables involved
            
            logger.debug("Extracting metadata from all query components");
            extractAllMetadata(query.getQueryBody()); // Extracts projections, filters, etc.
            
            logger.debug("CTE column aliases mapping: {}", cteColumnAliases);
            
            return buildMetadata();
        }

        private Map<String, String> buildSourceTableContext(Relation relation) {
            Map<String, String> contextMap = new HashMap<>();
            if (relation != null) {
                populateSourceTableContext(relation, contextMap);
                logger.debug("Built source table context: {}", contextMap);
            }
            return contextMap;
        }

        private void populateSourceTableContext(Relation relation, Map<String, String> contextMap) {
            if (relation instanceof Table) {
                Table tableNode = (Table) relation;
                String canonicalTableName = tableNode.getName().toString().toLowerCase(); // e.g., "schema.table" or "table"
                String tableSuffix = tableNode.getName().getSuffix().toLowerCase();     // e.g., "table"

                // Check if this is a CTE reference
                if (cteMap.containsKey(canonicalTableName)) {
                    // For CTE references, we need to resolve to the underlying base table
                    TableInfo cteBaseTable = findPrimaryTable(cteMap.get(canonicalTableName).getQuery().getQueryBody());
                    if (cteBaseTable != null) {
                        // Map the CTE name to its underlying base table
                        contextMap.put(canonicalTableName, cteBaseTable.name.toLowerCase());
                        contextMap.put(tableSuffix, cteBaseTable.name.toLowerCase());
                    }
                } else {
                    // Regular base table
                    // Columns might be qualified by the suffix (e.g., table.col)
                    contextMap.put(tableSuffix, canonicalTableName);
                    // Columns might also be qualified by the full name if it's different and used (e.g., schema.table.col)
                    if (!canonicalTableName.equals(tableSuffix)) {
                        contextMap.put(canonicalTableName, canonicalTableName);
                    }
                }
            } else if (relation instanceof AliasedRelation) {
                AliasedRelation aliasedNode = (AliasedRelation) relation;
                String alias = aliasedNode.getAlias().getValue().toLowerCase();

                Relation underlyingRelation = aliasedNode.getRelation();
                if (underlyingRelation instanceof Table) {
                    Table tableNode = (Table) underlyingRelation;
                    String canonicalTableName = tableNode.getName().toString().toLowerCase();
                    
                    // Check if the underlying relation is a CTE
                    if (cteMap.containsKey(canonicalTableName)) {
                        // For CTE references, resolve to the underlying base table
                        TableInfo cteBaseTable = findPrimaryTable(cteMap.get(canonicalTableName).getQuery().getQueryBody());
                        if (cteBaseTable != null) {
                            contextMap.put(alias, cteBaseTable.name.toLowerCase());
                        }
                    } else {
                        // Alias refers to a base table
                        contextMap.put(alias, canonicalTableName);
                    }
                } else if (underlyingRelation instanceof TableSubquery) {
                    // Handle aliased subqueries - resolve to the ultimate base table
                    TableSubquery subquery = (TableSubquery) underlyingRelation;
                    TableInfo subqueryBaseTable = findPrimaryTable(subquery.getQuery().getQueryBody());
                    if (subqueryBaseTable != null) {
                        contextMap.put(alias, subqueryBaseTable.name.toLowerCase());
                    }
                }
            } else if (relation instanceof Join) {
                Join joinNode = (Join) relation;
                populateSourceTableContext(joinNode.getLeft(), contextMap);
                populateSourceTableContext(joinNode.getRight(), contextMap);
            } else if (relation instanceof TableSubquery) {
                TableSubquery subquery = (TableSubquery) relation;
                // For subqueries, we need to recursively build context from the subquery's FROM clause
                QuerySpecification subquerySpec = (QuerySpecification) subquery.getQuery().getQueryBody();
                subquerySpec.getFrom().ifPresent(subFrom -> populateSourceTableContext(subFrom, contextMap));
            }
            // TODO (Future): Handle other Relation types like Unnest, etc.
            // For now, focusing on direct table references and their aliases.
        }

        private void initializeCteMap(Query query) {
            query.getWith().ifPresent(with -> {
                logger.debug("Processing WITH clause containing {} CTEs", with.getQueries().size());
                with.getQueries().forEach(cte -> {
                    String cteName = cte.getName().getValue().toLowerCase();
                    cteMap.put(cteName, cte);
                    logger.debug("Added CTE '{}' to processing map", cteName);
                    extractCteColumnAliases(cteName, cte);
                });
            });
        }
        
        private void extractCteColumnAliases(String cteName, WithQuery cte) {
            Map<String, String> columnAliases = new HashMap<>();
            
            QuerySpecification querySpec = (QuerySpecification) cte.getQuery().getQueryBody();
            for (SelectItem selectItem : querySpec.getSelect().getSelectItems()) {
                if (selectItem instanceof SingleColumn) {
                    SingleColumn singleColumn = (SingleColumn) selectItem;
                    singleColumn.getAlias().ifPresent(alias -> {
                        String aliasName = alias.getValue().toLowerCase();
                        
                        // For simple column references, store the mapping
                        // More complex expressions (functions, etc.) could be handled later
                        if (singleColumn.getExpression() instanceof Identifier) {
                            String originalColumn = ((Identifier) singleColumn.getExpression()).getValue().toLowerCase();
                            columnAliases.put(aliasName, originalColumn);
                            logger.debug("CTE '{}': alias '{}' -> column '{}'", cteName, aliasName, originalColumn);
                        } else if (singleColumn.getExpression() instanceof DereferenceExpression) {
                            // Handle qualified column references like A.policy -> derived_policy
                            DereferenceExpression deref = (DereferenceExpression) singleColumn.getExpression();
                            if (deref.getBase() instanceof Identifier && deref.getField().isPresent()) {
                                String originalColumn = deref.getField().get().getValue().toLowerCase();
                                columnAliases.put(aliasName, originalColumn);
                                logger.debug("CTE '{}': alias '{}' -> column '{}'", cteName, aliasName, originalColumn);
                            }
                        }
                    });
                }
            }
            
            cteColumnAliases.put(cteName, columnAliases);
        }

        private QueryMetadata buildMetadata() {
            QueryMetadata metadata = new QueryMetadata();
            
            if (primaryTable != null) {
                metadata.setBaseTable(primaryTable.name);
                metadata.setTableAlias(primaryTable.alias);
                discoveredBaseTables.add(primaryTable.name); // Ensure primary is in allBaseTables
            }
            
            metadata.setAllBaseTables(new ArrayList<>(discoveredBaseTables));
            metadata.setProjectionColumns(cleanProjections());
            metadata.setFilterColumns(new ArrayList<>(allFilters));
            metadata.setAggregations(new ArrayList<>(allAggregations));
            metadata.setGroupByColumns(new ArrayList<>(allGroupBys));
            metadata.setJoinColumns(new ArrayList<>(allJoinColumns));
            metadata.setTemporalGranularity(temporalGranularity);
            metadata.setTemporalGroupByColumns(new ArrayList<>(temporalGroupByColumns));
            
            return metadata;
        }

        private List<String> cleanProjections() {
            Set<String> cleaned = new LinkedHashSet<>(allProjections);
            if (cleaned.size() > 1) {
                cleaned.remove("*");
            }
            if (primaryTable != null && primaryTable.alias.isPresent()) {
                cleaned.remove(primaryTable.alias.get().toLowerCase());
            }
            return new ArrayList<>(cleaned);
        }

        private TableInfo findPrimaryTable(Node node) {
            if (node instanceof Table) {
                return handleTableNode((Table) node);
            } else if (node instanceof AliasedRelation) {
                AliasedRelation ar = (AliasedRelation) node;
                TableInfo base = findPrimaryTable(ar.getRelation());
                return base != null ? new TableInfo(base.name, Optional.of(ar.getAlias().getValue())) : null;
            } else if (node instanceof QuerySpecification) {
                return ((QuerySpecification) node).getFrom()
                    .map(this::findPrimaryTable)
                    .orElse(null);
            } else if (node instanceof Join) {
                Join join = (Join) node;
                TableInfo left = findPrimaryTable(join.getLeft());
                // For determining a single "primary" table, often the leftmost non-CTE table is chosen.
                // This logic might need refinement if a more sophisticated primary table detection is needed.
                return left != null ? left : findPrimaryTable(join.getRight());
            } else if (node instanceof TableSubquery) {
                return findPrimaryTable(((TableSubquery) node).getQuery().getQueryBody());
            }
            // Add other relation types if necessary (e.g., Unnest, Values, etc.)
            return null;
        }

        private TableInfo handleTableNode(Table table) {
            String tableName = table.getName().toString(); // This can be "schema.table" or just "table"
            String key = tableName.toLowerCase(); // Using the full name as key for CTE map
            
            if (!cteMap.containsKey(key)) {
                // Not a CTE, so it's a base table.
                return new TableInfo(table.getName().toString(), Optional.empty());
            } else if (!visitedCtes.contains(key)) {
                visitedCtes.add(key);
                TableInfo result = findPrimaryTable(cteMap.get(key).getQuery().getQueryBody());
                visitedCtes.remove(key); // Backtrack
                return result;
            }
            return null; // Cycle in CTE or other unhandled case
        }

        private void collectAllTables(Node node) {
            new DefaultTraversalVisitor<Void>() {
                @Override
                protected Void visitTable(Table table, Void context) {
                    String tableName = table.getName().toString();
                    String key = tableName.toLowerCase();
                    
                    if (!cteMap.containsKey(key)) {
                        discoveredBaseTables.add(table.getName().toString());
                    } else if (!visitedCtes.contains(key)) {
                        // It's a CTE, recurse.
                        visitedCtes.add(key);
                        process(cteMap.get(key).getQuery().getQueryBody(), context);
                        visitedCtes.remove(key); // Backtrack
                    }
                    return null;
                }
            }.process(node, null);
        }

        private void extractAllMetadata(Node node) {
            new DefaultTraversalVisitor<Void>() {
                @Override
                protected Void visitQuerySpecification(QuerySpecification spec, Void context) {
                    // Build the new source table context map
                    Map<String, String> sourceTableContext = spec.getFrom()
                        .map(MetadataExtractor.this::buildSourceTableContext)
                        .orElse(Collections.emptyMap());

                    logger.debug("Processing QuerySpecification with source table context: {}", sourceTableContext);
                    
                    Set<String> projections = extractProjections(spec, sourceTableContext);
                    Set<String> filters = extractFilters(spec, sourceTableContext);
                    Set<AggregationInfo> aggregations = extractAggregations(spec, sourceTableContext);
                    Set<String> groupBys = extractGroupBys(spec, sourceTableContext);
                    Set<String> joinColumns = extractJoinColumns(spec, sourceTableContext);
                    
                    logger.debug("Extracted projections: {}", projections);
                    logger.debug("Extracted filters: {}", filters);
                    logger.debug("Extracted aggregations: {}", aggregations);
                    logger.debug("Extracted group by columns: {}", groupBys);
                    logger.debug("Extracted join columns: {}", joinColumns);
                    
                    allProjections.addAll(projections);
                    allFilters.addAll(filters);
                    allAggregations.addAll(aggregations);
                    allGroupBys.addAll(groupBys);
                    allJoinColumns.addAll(joinColumns);

                    return super.visitQuerySpecification(spec, context);
                }

                @Override
                protected Void visitTable(Table table, Void context) {
                    // This is to ensure that if a CTE is referenced, its defining query is processed
                    // to extract metadata from within the CTE if it hasn't been processed already.
                    String key = table.getName().toString().toLowerCase();
                    if (cteMap.containsKey(key) && !visitedCtes.contains(key)) {
                        visitedCtes.add(key);
                        // Process the CTE's query body for metadata extraction
                        process(cteMap.get(key).getQuery().getQueryBody(), context);
                        visitedCtes.remove(key);
                    }
                    return null; // Continue traversal
                }
            }.process(node, null);
        }

        // 'alias' here is the context (current table alias or name) for unqualified columns
        private Set<String> extractProjections(QuerySpecification spec, Map<String, String> sourceTableContext) {
            List<SelectItem> selectItems = spec.getSelect().getSelectItems();
            // logger.debug("Processing {} select items", selectItems.size());
            
            Set<String> projections = new HashSet<>();
            for (SelectItem item : selectItems) {
                // logger.debug("Processing select item: {} (type: {})", item, item.getClass().getSimpleName());
                if (item instanceof SingleColumn) {
                    Expression expr = ((SingleColumn) item).getExpression();
                    Set<String> identifiers = extractIdentifiersFromExpression(expr, sourceTableContext);
                    // logger.debug("Extracted identifiers from expression {}: {}", expr, identifiers);
                    projections.addAll(identifiers);
                } else if (item instanceof AllColumns) {
                    // Skip * projections - assume required columns will be found explicitly
                }
            }
            
            return projections;
        }

        private Set<String> extractFilters(QuerySpecification spec, Map<String, String> sourceTableContext) {
            return spec.getWhere()
                .map(where -> extractIdentifiersFromExpression(where, sourceTableContext))
                .orElse(new HashSet<>());
        }

        private Set<AggregationInfo> extractAggregations(QuerySpecification spec, Map<String, String> sourceTableContext) {
            Set<AggregationInfo> aggregations = new HashSet<>();
            spec.getSelect().getSelectItems().stream()
                .filter(item -> item instanceof SingleColumn)
                .map(item -> ((SingleColumn) item).getExpression())
                .forEach(expr -> aggregations.addAll(extractAggregationsFromExpression(expr, sourceTableContext)));
            spec.getHaving().ifPresent(having ->
                aggregations.addAll(extractAggregationsFromExpression(having, sourceTableContext)));
            return aggregations;
        }

        private Set<String> extractGroupBys(QuerySpecification spec, Map<String, String> sourceTableContext) {
            if (!spec.getGroupBy().isPresent()) {
                return new HashSet<>();
            }
            
            List<SelectItem> selectItems = spec.getSelect().getSelectItems();
            Set<String> groupByColumns = new HashSet<>();
            
            // Process each GROUP BY expression
            for (GroupingElement ge : spec.getGroupBy().get().getGroupingElements()) {
                if (ge instanceof SimpleGroupBy) {
                    for (Expression expr : ((SimpleGroupBy) ge).getExpressions()) {
                        Expression resolvedExpr = resolveOrdinalReference(expr, selectItems);
                        
                        // Analyze for temporal granularity
                        TimeGranularity granularity = temporalAnalyzer.extractGranularity(resolvedExpr);
                        if (granularity != TimeGranularity.UNKNOWN) {
                            logger.debug("Found temporal GROUP BY expression with granularity {}: {}", granularity, resolvedExpr);
                            
                            // Update the overall temporal granularity (keep the finest)
                            if (temporalGranularity == TimeGranularity.UNKNOWN || 
                                granularity.isFinnerThan(temporalGranularity)) {
                                temporalGranularity = granularity;
                                logger.debug("Updated overall temporal granularity to: {}", temporalGranularity);
                            }
                            
                            // Extract column identifiers from the temporal expression
                            Set<String> temporalColumns = extractIdentifiersFromExpression(resolvedExpr, sourceTableContext);
                            temporalGroupByColumns.addAll(temporalColumns);
                        }
                        
                        // Extract regular column identifiers
                        Set<String> columns = extractIdentifiersFromExpression(resolvedExpr, sourceTableContext);
                        groupByColumns.addAll(columns);
                    }
                }
            }
            
            return groupByColumns;
        }


        private Set<String> extractJoinColumns(QuerySpecification spec, Map<String, String> sourceTableContext) {
            Set<String> joinColumns = new HashSet<>();
            spec.getFrom().ifPresent(from -> {
                JoinColumnExtractor extractor = new JoinColumnExtractor(sourceTableContext, cteColumnAliases, primaryTable);
                extractor.process(from, null);
                joinColumns.addAll(extractor.getJoinColumns());
            });
            return joinColumns;
        }

        private Expression resolveOrdinalReference(Expression expr, List<SelectItem> selectItems) {
            if (expr instanceof LongLiteral) {
                int index = (int) Long.parseLong(((LongLiteral) expr).getValue()) - 1;
                if (index >= 0 && index < selectItems.size() && selectItems.get(index) instanceof SingleColumn) {
                    return ((SingleColumn) selectItems.get(index)).getExpression();
                }
            }
            return expr;
        }

        private Set<String> extractIdentifiersFromExpression(Expression expr, Map<String, String> sourceTableContext) {
            IdentifierExtractor extractor = new IdentifierExtractor(sourceTableContext, cteColumnAliases); // Pass both maps
            extractor.process(expr, null);
            return extractor.getIdentifiers();
        }

        private Set<AggregationInfo> extractAggregationsFromExpression(Expression expr, Map<String, String> sourceTableContext) {
            AggregationExtractor extractor = new AggregationExtractor(sourceTableContext, cteColumnAliases); // Pass both maps
            extractor.process(expr, null);
            return extractor.getAggregations();
        }
    }

    /**
     * Specialized visitor for extracting column identifiers.
     */
    private static class IdentifierExtractor extends DefaultTraversalVisitor<Void> {
        private final Set<String> identifiers = new HashSet<>();
        private final Map<String, String> sourceTableContext;
        private final Map<String, Map<String, String>> cteColumnAliases;

        IdentifierExtractor(Map<String, String> sourceTableContext, Map<String, Map<String, String>> cteColumnAliases) {
            this.sourceTableContext = sourceTableContext != null ? sourceTableContext : Collections.emptyMap();
            this.cteColumnAliases = cteColumnAliases != null ? cteColumnAliases : Collections.emptyMap();
        }

        Set<String> getIdentifiers() { 
            return identifiers; 
        }

        @Override
        protected Void visitIdentifier(Identifier node, Void context) {
            String columnName = node.getValue().toLowerCase();
            // Attempt to find an unambiguous base table name from the context
            Set<String> distinctBaseTablesInContext = new HashSet<>(sourceTableContext.values());
            if (distinctBaseTablesInContext.size() == 1) {
                String resolvedBaseTable = distinctBaseTablesInContext.iterator().next();
                identifiers.add(resolvedBaseTable + "." + columnName);
            } else {
                // Ambiguous or no context for unqualified column, or multiple different base tables in FROM
                // For now, add as unqualified. Further disambiguation might require schema knowledge.
                identifiers.add(columnName);
            }
            return null;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void context) {
            Expression base = node.getBase();
            Optional<Identifier> field = node.getField();
            if (base instanceof Identifier && field.isPresent()) {
                String qualifier = ((Identifier) base).getValue().toLowerCase();
                String fieldName = field.get().getValue().toLowerCase();

                // First, check if this is a CTE column alias that needs resolution
                String resolvedFieldName = resolveCteColumnAlias(qualifier, fieldName);
                
                if (sourceTableContext.containsKey(qualifier)) {
                    String resolvedBaseTable = sourceTableContext.get(qualifier);
                    identifiers.add(resolvedBaseTable + "." + resolvedFieldName);
                } else {
                    // Qualifier not in context map (e.g., alias for a subquery, or a direct schema.table qualifier)
                    // Fallback to using the string representation, which might be "qualifier.field"
                    // or even "schema.qualifier.field"
                    identifiers.add(node.toString().toLowerCase());
                }
            } else {
                // Base is not a simple Identifier (e.g., function call, nested dereference)
                // or field is not present (should not happen for column access).
                // Fallback to old behavior or process children.
                // For now, stick to Phase 1 behavior for complex cases.
                identifiers.add(node.toString().toLowerCase());
            }
            return null;
        }
        
        private String resolveCteColumnAlias(String cteAlias, String columnName) {
            // Check if this CTE has column aliases and if this column is aliased
            Map<String, String> aliases = cteColumnAliases.get(cteAlias);
            if (aliases != null && aliases.containsKey(columnName)) {
                String originalColumn = aliases.get(columnName);
                logger.debug("Resolved CTE column alias: {}.{} -> {}", cteAlias, columnName, originalColumn);
                return originalColumn;
            }
            // No alias found, return original column name
            return columnName;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Void context) {
            // We need to extract identifiers from arguments of non-aggregate functions too.
            // Example: SELECT transform(t1.col, 1) FROM table1 t1; -> "t1.col" should be extracted.
            // The AGGREGATE_FUNCTIONS check is in AggregationExtractor.
            // Here, we just traverse into arguments.
            node.getArguments().forEach(arg -> process(arg, context));
            // Do not process node.getName() or node.getWindow() unless they can contain column identifiers.
            return null;
        }
    }

    /**
     * Specialized visitor for extracting aggregation functions.
     */
    private static class AggregationExtractor extends DefaultTraversalVisitor<Void> {
        private final Set<AggregationInfo> aggregations = new HashSet<>();
        private final Map<String, String> sourceTableContext;
        private final Map<String, Map<String, String>> cteColumnAliases;

        AggregationExtractor(Map<String, String> sourceTableContext, Map<String, Map<String, String>> cteColumnAliases) {
            this.sourceTableContext = sourceTableContext != null ? sourceTableContext : Collections.emptyMap();
            this.cteColumnAliases = cteColumnAliases != null ? cteColumnAliases : Collections.emptyMap();
        }

        Set<AggregationInfo> getAggregations() { 
            return aggregations; 
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Void context) {
            String functionName = node.getName().toString().toUpperCase(); // Keep function name case-insensitive for matching
            
            if (AGGREGATE_FUNCTIONS.contains(functionName)) {
                List<String> args = new ArrayList<>();
                for (Expression arg : node.getArguments()) {
                    IdentifierExtractor extractor = new IdentifierExtractor(this.sourceTableContext, this.cteColumnAliases);
                    extractor.process(arg, null);
                    args.addAll(extractor.getIdentifiers());

                }
                // AggregationInfo expects function name (can be normalized there) and arguments
                aggregations.add(new AggregationInfo(node.getName().toString(), args, node.isDistinct()));
            } else {
                // If not an aggregate function we care about, still process its arguments
                // in case they contain nested aggregate functions (though less common for this extractor's direct purpose).
                // More importantly, this allows IdentifierExtractor (if called from a higher level)
                // to find non-aggregated columns inside regular function calls.
                node.getArguments().forEach(arg -> process(arg, context));
            }
            return null;
        }
    }

    /**
     * Specialized visitor for extracting join columns from JOIN conditions.
     */
    private static class JoinColumnExtractor extends DefaultTraversalVisitor<Void> {
        private final Set<String> joinColumns = new HashSet<>();
        private final Map<String, String> sourceTableContext;
        private final Map<String, Map<String, String>> cteColumnAliases;
        private final TableInfo primaryTable;

        JoinColumnExtractor(Map<String, String> sourceTableContext, Map<String, Map<String, String>> cteColumnAliases, TableInfo primaryTable) {
            this.sourceTableContext = sourceTableContext != null ? sourceTableContext : Collections.emptyMap();
            this.cteColumnAliases = cteColumnAliases != null ? cteColumnAliases : Collections.emptyMap();
            this.primaryTable = primaryTable;
        }

        Set<String> getJoinColumns() {
            return joinColumns;
        }

        @Override
        protected Void visitJoin(Join node, Void context) {
            logger.debug("Processing join: {}", node.getType());
            
            // Extract columns from the join condition
            node.getCriteria().ifPresent(criteria -> {
                if (criteria instanceof JoinOn) {
                    JoinOn joinOn = (JoinOn) criteria;
                    Expression condition = joinOn.getExpression();
                    logger.debug("Extracting columns from join condition: {}", condition);
                    
                    // Extract all column references from the join condition
                    IdentifierExtractor extractor = new IdentifierExtractor(sourceTableContext, cteColumnAliases);
                    extractor.process(condition, null);
                    Set<String> allColumns = extractor.getIdentifiers();
                    
                    // Filter to only keep columns that belong to the primary table
                    if (primaryTable != null) {
                        String primaryTableName = primaryTable.name.toLowerCase();
                        Set<String> primaryTableColumns = allColumns.stream()
                            .filter(col -> col.startsWith(primaryTableName + "."))
                            .collect(Collectors.toSet());
                        
                        joinColumns.addAll(primaryTableColumns);
                        logger.debug("Found join columns for primary table '{}': {}", primaryTableName, primaryTableColumns);
                    }
                }
            });
            
            // Continue processing child relations
            process(node.getLeft(), context);
            process(node.getRight(), context);
            return null;
        }
    }

    /**
     * Simple holder for table information (name and alias).
     */
    private static class TableInfo {
        final String name; // Base table name
        final Optional<String> alias;

        TableInfo(String name, Optional<String> alias) {
            this.name = name;
            this.alias = alias;
        }
    }
}