package com.ns.rewrite.analysis;

import io.trino.sql.tree.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        MetadataExtractor extractor = new MetadataExtractor();
        QueryMetadata metadata = extractor.extract((Query) queryStatement);
        
        logger.info("Extracted metadata for base table '{}' with projections: {}, filters: {}, aggregations: {}", 
            metadata.getBaseTable(), 
            metadata.getProjectionColumns(),
            metadata.getFilterColumns(), 
            metadata.getAggregations());
        
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
        private final Set<String> discoveredBaseTables = new LinkedHashSet<>();
        
        private Map<String, WithQuery> cteMap = new HashMap<>();
        private Set<String> visitedCtes = new HashSet<>();
        private TableInfo primaryTable;

        QueryMetadata extract(Query query) {
            initializeCteMap(query);
            
            primaryTable = findPrimaryTable(query.getQueryBody());
            collectAllTables(query.getQueryBody());
            extractAllMetadata(query.getQueryBody());
            
            return buildMetadata();
        }

        private void initializeCteMap(Query query) {
            query.getWith().ifPresent(with -> 
                with.getQueries().forEach(cte -> 
                    cteMap.put(cte.getName().getValue().toLowerCase(), cte)));
        }

        private QueryMetadata buildMetadata() {
            QueryMetadata metadata = new QueryMetadata();
            
            if (primaryTable != null) {
                metadata.setBaseTable(primaryTable.name);
                metadata.setTableAlias(primaryTable.alias);
                discoveredBaseTables.add(primaryTable.name);
            }
            
            metadata.setAllBaseTables(new ArrayList<>(discoveredBaseTables));
            metadata.setProjectionColumns(cleanProjections());
            metadata.setFilterColumns(new ArrayList<>(allFilters));
            metadata.setAggregations(new ArrayList<>(allAggregations));
            metadata.setGroupByColumns(new ArrayList<>(allGroupBys));
            
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
                return left != null ? left : findPrimaryTable(join.getRight());
            } else if (node instanceof TableSubquery) {
                return findPrimaryTable(((TableSubquery) node).getQuery().getQueryBody());
            }
            return null;
        }

        private TableInfo handleTableNode(Table table) {
            String tableName = table.getName().toString();
            String key = tableName.toLowerCase();
            
            if (!cteMap.containsKey(key)) {
                return new TableInfo(tableName, Optional.empty());
            } else if (!visitedCtes.contains(key)) {
                visitedCtes.add(key);
                TableInfo result = findPrimaryTable(cteMap.get(key).getQuery().getQueryBody());
                visitedCtes.remove(key);
                return result;
            }
            return null;
        }

        private void collectAllTables(Node node) {
            new DefaultTraversalVisitor<Void>() {
                @Override
                protected Void visitTable(Table table, Void context) {
                    String tableName = table.getName().toString();
                    String key = tableName.toLowerCase();
                    
                    if (!cteMap.containsKey(key)) {
                        discoveredBaseTables.add(tableName);
                    } else if (!visitedCtes.contains(key)) {
                        visitedCtes.add(key);
                        process(cteMap.get(key).getQuery().getQueryBody(), context);
                        visitedCtes.remove(key);
                    }
                    return null;
                }
            }.process(node, null);
        }

        private void extractAllMetadata(Node node) {
            new DefaultTraversalVisitor<Void>() {
                @Override
                protected Void visitQuerySpecification(QuerySpecification spec, Void context) {
                    Optional<String> alias = extractTableAlias(spec.getFrom().orElse(null));
                    
                    allProjections.addAll(extractProjections(spec, alias));
                    allFilters.addAll(extractFilters(spec, alias));
                    allAggregations.addAll(extractAggregations(spec, alias));
                    allGroupBys.addAll(extractGroupBys(spec, alias));
                    
                    return super.visitQuerySpecification(spec, context);
                }

                @Override
                protected Void visitTable(Table table, Void context) {
                    String key = table.getName().toString().toLowerCase();
                    if (cteMap.containsKey(key) && !visitedCtes.contains(key)) {
                        visitedCtes.add(key);
                        process(cteMap.get(key).getQuery(), context);
                        visitedCtes.remove(key);
                    }
                    return null;
                }
            }.process(node, null);
        }

        private Optional<String> extractTableAlias(Relation relation) {
            return relation instanceof AliasedRelation ? 
                Optional.of(((AliasedRelation) relation).getAlias().getValue()) : 
                Optional.empty();
        }

        private Set<String> extractProjections(QuerySpecification spec, Optional<String> alias) {
            return spec.getSelect().getSelectItems().stream()
                .filter(item -> item instanceof SingleColumn)
                .map(item -> ((SingleColumn) item).getExpression())
                .flatMap(expr -> extractIdentifiersFromExpression(expr, alias).stream())
                .collect(Collectors.toSet());
        }

        private Set<String> extractFilters(QuerySpecification spec, Optional<String> alias) {
            return spec.getWhere()
                .map(where -> extractIdentifiersFromExpression(where, alias))
                .orElse(new HashSet<>());
        }

        private Set<AggregationInfo> extractAggregations(QuerySpecification spec, Optional<String> alias) {
            Set<AggregationInfo> aggregations = new HashSet<>();
            
            // From SELECT clause
            spec.getSelect().getSelectItems().stream()
                .filter(item -> item instanceof SingleColumn)
                .map(item -> ((SingleColumn) item).getExpression())
                .forEach(expr -> aggregations.addAll(extractAggregationsFromExpression(expr, alias)));
            
            // From HAVING clause
            spec.getHaving().ifPresent(having -> 
                aggregations.addAll(extractAggregationsFromExpression(having, alias)));
            
            return aggregations;
        }

        private Set<String> extractGroupBys(QuerySpecification spec, Optional<String> alias) {
            if (!spec.getGroupBy().isPresent()) {
                return new HashSet<>();
            }
            
            List<SelectItem> selectItems = spec.getSelect().getSelectItems();
            return spec.getGroupBy().get().getGroupingElements().stream()
                .filter(ge -> ge instanceof SimpleGroupBy)
                .flatMap(ge -> ((SimpleGroupBy) ge).getExpressions().stream())
                .map(expr -> resolveOrdinalReference(expr, selectItems))
                .flatMap(expr -> extractIdentifiersFromExpression(expr, alias).stream())
                .collect(Collectors.toSet());
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

        private Set<String> extractIdentifiersFromExpression(Expression expr, Optional<String> alias) {
            IdentifierExtractor extractor = new IdentifierExtractor(alias);
            extractor.process(expr, null);
            return extractor.getIdentifiers();
        }

        private Set<AggregationInfo> extractAggregationsFromExpression(Expression expr, Optional<String> alias) {
            AggregationExtractor extractor = new AggregationExtractor(alias);
            extractor.process(expr, null);
            return extractor.getAggregations();
        }
    }

    /**
     * Specialized visitor for extracting column identifiers.
     */
    private static class IdentifierExtractor extends DefaultTraversalVisitor<Void> {
        private final Set<String> identifiers = new HashSet<>();
        private final Optional<String> aliasToStrip;

        IdentifierExtractor(Optional<String> aliasToStrip) {
            this.aliasToStrip = aliasToStrip;
        }

        Set<String> getIdentifiers() { 
            return identifiers; 
        }

        @Override
        protected Void visitIdentifier(Identifier node, Void context) {
            identifiers.add(node.getValue().toLowerCase());
            return null;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void context) {
            if (node.getField().isPresent()) {
                String fieldName = node.getField().get().getValue().toLowerCase();
                
                if (node.getBase() instanceof Identifier) {
                    String baseName = ((Identifier) node.getBase()).getValue().toLowerCase();
                    if (aliasToStrip.isPresent() && baseName.equals(aliasToStrip.get().toLowerCase())) {
                        identifiers.add(fieldName);
                        return null;
                    }
                }
                identifiers.add(fieldName);
            }
            
            if (!(node.getBase() instanceof Identifier)) {
                process(node.getBase(), context);
            }
            return null;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Void context) {
            node.getArguments().forEach(arg -> process(arg, context));
            return null;
        }
    }

    /**
     * Specialized visitor for extracting aggregation functions.
     */
    private static class AggregationExtractor extends DefaultTraversalVisitor<Void> {
        private final Set<AggregationInfo> aggregations = new HashSet<>();
        private final Optional<String> aliasToStrip;

        AggregationExtractor(Optional<String> aliasToStrip) {
            this.aliasToStrip = aliasToStrip;
        }

        Set<AggregationInfo> getAggregations() { 
            return aggregations; 
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Void context) {
            String functionName = node.getName().toString().toUpperCase();
            
            if (AGGREGATE_FUNCTIONS.contains(functionName)) {
                List<String> args = new ArrayList<>();
                for (Expression arg : node.getArguments()) {
                    IdentifierExtractor extractor = new IdentifierExtractor(aliasToStrip);
                    extractor.process(arg, null);
                    args.addAll(extractor.getIdentifiers());
                }
                
                aggregations.add(new AggregationInfo(node.getName().toString(), args, node.isDistinct()));
            } else {
                node.getArguments().forEach(arg -> process(arg, context));
            }
            return null;
        }
    }

    /**
     * Simple holder for table information.
     */
    private static class TableInfo {
        final String name;
        final Optional<String> alias;

        TableInfo(String name, Optional<String> alias) {
            this.name = name;
            this.alias = alias;
        }
    }
}