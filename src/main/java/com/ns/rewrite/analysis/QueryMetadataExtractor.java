package com.ns.rewrite.analysis;

import io.trino.sql.SqlFormatter;
import io.trino.sql.tree.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ns.rewrite.model.AggregationInfo;
import com.ns.rewrite.model.QueryMetadata;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryMetadataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetadataExtractor.class);

    private static class RelationInfo {
        final String baseTableName;
        final Optional<String> alias;

        RelationInfo(String baseTableName, Optional<String> alias) {
            this.baseTableName = baseTableName;
            this.alias = alias;
        }

        @Override
        public String toString() {
            return baseTableName + alias.map(a -> " AS " + a).orElse("");
        }
    }

    public static QueryMetadata extractMetadataFromQuery(Statement queryStatement) {
        if (!(queryStatement instanceof Query)) {
            throw new IllegalArgumentException("Expected a Query statement but got: " + queryStatement.getClass().getSimpleName());
        }

        MetadataVisitor visitor = new MetadataVisitor();
        QueryMetadata metadata = visitor.process((Query) queryStatement);

        logger.info("Extracted metadata: baseTable={}, alias={}, allBaseTables={}, projections={}, groupBy={}, aggregations={}, filters={}",
                metadata.getBaseTable(),
                metadata.getTableAlias().orElse("N/A"),
                metadata.getAllBaseTables(),
                metadata.getProjectionColumns(),
                metadata.getGroupByColumns(),
                metadata.getAggregations().stream().map(AggregationInfo::toString).collect(Collectors.toList()),
                metadata.getFilterColumns());

        return metadata;
    }
    
    // Enhanced findBaseIdentifiers to better handle complex expressions
    private static Set<String> findBaseIdentifiers(Expression expression, Optional<String> aliasToStrip, Map<String, WithQuery> cteMap) {
        final Set<String> identifiers = new HashSet<>();
        
        Expression processedExpr = aliasToStrip
            .map(alias -> ExpressionTreeRewriter.rewriteWith(new AliasStripper(alias), expression))
            .orElse(expression);

        new DefaultTraversalVisitor<Void>() { 
            @Override
            protected Void visitIdentifier(Identifier node, Void passedContext) {
                String identifier = node.getValue().toLowerCase();
                if (cteMap.containsKey(identifier)) {
                    logger.trace("findBaseIdentifiers - Skipping CTE identifier: {}", identifier);
                } else {
                    identifiers.add(identifier);
                    logger.trace("findBaseIdentifiers - Added identifier: {}", identifier);
                }
                return null;
            }

            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, Void passedContext) {
                if (node.getField().isPresent()) {
                    String fieldName = node.getField().get().getValue().toLowerCase();
                    
                    if (node.getBase() instanceof Identifier) {
                        String baseName = ((Identifier) node.getBase()).getValue().toLowerCase();
                        if (cteMap.containsKey(baseName)) {
                            // This handles cases like alert_event.alert_type where alert_event is a CTE
                            logger.trace("findBaseIdentifiers - Found CTE dereference: {}.{}", baseName, fieldName);
                            identifiers.add(fieldName);
                        } else {
                            // This handles cases like A.column_name where A is a table alias
                            identifiers.add(fieldName);
                            logger.trace("findBaseIdentifiers - Added dereferenced field: {}", fieldName);
                        }
                    } else {
                        identifiers.add(fieldName);
                        logger.trace("findBaseIdentifiers - Added dereferenced field: {}", fieldName);
                    }
                }
                if (!(node.getBase() instanceof Identifier)) {
                    process(node.getBase(), passedContext);
                }
                return null; 
            }

            @Override
            protected Void visitFunctionCall(FunctionCall node, Void passedContext) {
                // Extract identifiers from function arguments (like SPLIT, CONCAT)
                String functionName = node.getName().toString().toUpperCase();
                logger.info("findBaseIdentifiers - Processing function: {}", functionName);
                
                node.getArguments().forEach(arg -> process(arg, passedContext));
                return null;
            }

            @Override 
            protected Void visitCast(Cast node, Void passedContext) {
                process(node.getExpression(), passedContext);
                return null;
            }

        }.process(processedExpr, null);

        logger.info("findBaseIdentifiers for expression '{}' (aliasToStrip: {}) resulted in: {}", 
            SqlFormatter.formatSql(expression), aliasToStrip.orElse("none"), identifiers);
        return identifiers;
    }

    private static class MetadataVisitor extends AstVisitor<Void, MetadataVisitor.Context> {

        private final QueryMetadata metadata = new QueryMetadata();
        private final Set<String> allProjections = new LinkedHashSet<>();
        private final Set<String> allFilters = new LinkedHashSet<>();
        private final Set<AggregationInfo> allAggregations = new LinkedHashSet<>();
        private final Set<String> allGroupBys = new LinkedHashSet<>();
        private RelationInfo primaryBaseRelationInfo = null; 
        private final Set<String> visitedCtesForPrimarySearch = new HashSet<>();
        private final Set<String> discoveredBaseTables = new LinkedHashSet<>(); 

        static class Context {
            final Map<String, WithQuery> cteMap;
            final Set<String> visitedCtesInCurrentPathTraversal;

            Context(Optional<With> withClause) {
                this.cteMap = new HashMap<>();
                withClause.ifPresent(w -> w.getQueries().forEach(q -> this.cteMap.put(q.getName().getValue().toLowerCase(), q)));
                this.visitedCtesInCurrentPathTraversal = new HashSet<>();
            }

            Context(Map<String, WithQuery> cteMap, Set<String> visitedCtesInCurrentPath) {
                 this.cteMap = cteMap;
                 this.visitedCtesInCurrentPathTraversal = visitedCtesInCurrentPath;
            }
        }

        public QueryMetadata process(Query query) {
            Context initialContext = new Context(query.getWith());
            
            primaryBaseRelationInfo = findPrimaryBaseTableRecursive(query.getQueryBody(), initialContext);
            collectAllTableReferences(query.getQueryBody(), initialContext);

            if (primaryBaseRelationInfo == null && !discoveredBaseTables.isEmpty()) {
                primaryBaseRelationInfo = new RelationInfo(discoveredBaseTables.iterator().next(), Optional.empty());
            } else if (primaryBaseRelationInfo == null && discoveredBaseTables.isEmpty()){
                 logger.warn("Could not determine any base table for the query. The query might not reference any base tables directly.");
            }

            if (primaryBaseRelationInfo != null && primaryBaseRelationInfo.baseTableName != null) {
                metadata.setBaseTable(primaryBaseRelationInfo.baseTableName);
                metadata.setTableAlias(primaryBaseRelationInfo.alias);
                if (!primaryBaseRelationInfo.baseTableName.isEmpty()) {
                     discoveredBaseTables.add(primaryBaseRelationInfo.baseTableName); 
                }
            }
            metadata.setAllBaseTables(new ArrayList<>(discoveredBaseTables));
            
            logger.debug("Starting main metadata extraction traversal.");
            super.process(query, initialContext); 
            logger.debug("COMPLETED main metadata extraction traversal (super.process returned).");

            if (allProjections.size() > 1 && allProjections.contains("*")) {
                allProjections.remove("*"); 
            }
            if (primaryBaseRelationInfo != null && primaryBaseRelationInfo.alias.isPresent()) {
                 allProjections.remove(primaryBaseRelationInfo.alias.get().toLowerCase());
            }

            metadata.setProjectionColumns(new ArrayList<>(allProjections));
            metadata.setFilterColumns(new ArrayList<>(allFilters));
            metadata.setAggregations(new ArrayList<>(allAggregations));
            metadata.setGroupByColumns(new ArrayList<>(allGroupBys));

            return metadata;
        }

        private RelationInfo findPrimaryBaseTableRecursive(Node node, Context context) {
            if (node instanceof Table) {
                Table table = (Table) node;
                String tableName = table.getName().toString();
                String tableNameKey = tableName.toLowerCase();
                if (!context.cteMap.containsKey(tableNameKey)) {
                    return new RelationInfo(tableName, Optional.empty());
                } else if (!visitedCtesForPrimarySearch.contains(tableNameKey)) { 
                    visitedCtesForPrimarySearch.add(tableNameKey); 
                    RelationInfo infoFromCte = findPrimaryBaseTableRecursive(context.cteMap.get(tableNameKey).getQuery().getQueryBody(), context);
                    visitedCtesForPrimarySearch.remove(tableNameKey); 
                    return infoFromCte;
                }
            } else if (node instanceof AliasedRelation) {
                AliasedRelation ar = (AliasedRelation) node;
                RelationInfo base = findPrimaryBaseTableRecursive(ar.getRelation(), context);
                if (base != null) {
                    return new RelationInfo(base.baseTableName, Optional.of(ar.getAlias().getValue()));
                }
            } else if (node instanceof TableSubquery) {
                return findPrimaryBaseTableRecursive(((TableSubquery) node).getQuery().getQueryBody(), context);
            } else if (node instanceof QuerySpecification) {
                return ((QuerySpecification) node).getFrom()
                           .map(from -> findPrimaryBaseTableRecursive(from, context))
                           .orElse(null);
            } else if (node instanceof Join) {
                RelationInfo left = findPrimaryBaseTableRecursive(((Join) node).getLeft(), context);
                if (left != null) return left;
                return findPrimaryBaseTableRecursive(((Join) node).getRight(), context);
            }
            return null;
        }
        
        private void collectAllTableReferences(Node rootNode, Context parentContext) {
            Context traversalContext = new Context(parentContext.cteMap, new HashSet<>());
            new DefaultTraversalVisitor<Context>() { 
                @Override
                protected Void visitTable(Table node, Context currentLocalContext) {
                    String tableName = node.getName().toString();
                    String tableNameKey = tableName.toLowerCase();
                    if (!currentLocalContext.cteMap.containsKey(tableNameKey)) {
                        discoveredBaseTables.add(tableName);
                    } else {
                        if (!currentLocalContext.visitedCtesInCurrentPathTraversal.contains(tableNameKey)) {
                            WithQuery cte = currentLocalContext.cteMap.get(tableNameKey);
                            if (cte != null) {
                                currentLocalContext.visitedCtesInCurrentPathTraversal.add(tableNameKey);
                                process(cte.getQuery().getQueryBody(), currentLocalContext); 
                                currentLocalContext.visitedCtesInCurrentPathTraversal.remove(tableNameKey); 
                            }
                        }
                    }
                    return null; 
                }

                @Override
                protected Void visitQuerySpecification(QuerySpecification node, Context currentLocalContext) {
                    node.getFrom().ifPresent(from -> process(from, currentLocalContext));
                    return super.visitQuerySpecification(node, currentLocalContext);
                }

                @Override
                protected Void visitTableSubquery(TableSubquery node, Context currentLocalContext) {
                    process(node.getQuery().getQueryBody(), currentLocalContext); 
                    return null; 
                }

                @Override
                protected Void visitAliasedRelation(AliasedRelation node, Context currentLocalContext) {
                    process(node.getRelation(), currentLocalContext); 
                    return null; 
                }

                @Override
                protected Void visitJoin(Join node, Context currentLocalContext) {
                    process(node.getLeft(), currentLocalContext);
                    process(node.getRight(), currentLocalContext);
                    return null; 
                }
            }.process(rootNode, traversalContext);
        }

        @Override
        protected Void visitQuery(Query node, Context context) {
            logger.debug("MetadataVisitor.visitQuery: Processing Query node. Has WITH? {}, Query Body Type: {}",
                        node.getWith().isPresent(), node.getQueryBody().getClass().getSimpleName());

            node.getWith().ifPresent(withClause -> {
                logger.debug("MetadataVisitor.visitQuery: Processing WITH clause.");
                process(withClause, context); // This should dispatch to visitWith
            });

            logger.debug("MetadataVisitor.visitQuery: Processing QueryBody.");
            process(node.getQueryBody(), context); // This should dispatch to visitQuerySpecification, visitTable, etc.
            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Context context) {
            String fromString = node.getFrom().map(Object::toString).orElse("NONE");
            logger.debug("Visiting QuerySpecification. FROM: {}", fromString.substring(0, Math.min(fromString.length(), 100)));

            Optional<String> currentAlias = node.getFrom()
                .filter(AliasedRelation.class::isInstance)
                .map(AliasedRelation.class::cast)
                .map(ar -> Optional.of(ar.getAlias().getValue()))
                .orElse(Optional.empty()); 
            logger.debug("Determined currentAlias for this QuerySpecification: {}", currentAlias.orElse("none"));
            
            Set<String> projectionsThisScope = extractProjections(node, currentAlias, context.cteMap);
            Set<String> filtersThisScope = extractFilterColumns(node, currentAlias, context.cteMap);
            List<AggregationInfo> aggregationsThisScope = extractAggregations(node, currentAlias, context.cteMap);
            Set<String> groupBysThisScope = extractGroupBy(node, currentAlias, context.cteMap);

            if (!projectionsThisScope.isEmpty()) logger.debug("Projections in this QS (alias {}): {}", currentAlias.orElse("none"), projectionsThisScope);
            if (!filtersThisScope.isEmpty()) logger.debug("Filters in this QS (alias {}): {}", currentAlias.orElse("none"), filtersThisScope);
            if (!aggregationsThisScope.isEmpty()) logger.debug("Aggregations in this QS (alias {}): {}", currentAlias.orElse("none"), aggregationsThisScope);
            if (!groupBysThisScope.isEmpty()) logger.debug("GroupBys in this QS (alias {}): {}", currentAlias.orElse("none"), groupBysThisScope);
            
            allProjections.addAll(projectionsThisScope);
            allFilters.addAll(filtersThisScope);
            allAggregations.addAll(aggregationsThisScope);
            allGroupBys.addAll(groupBysThisScope);

            logger.debug("Manually processing children of QuerySpecification (Select, From, Where, GroupBy, Having)");
            process(node.getSelect(), context); 
            node.getFrom().ifPresent(from -> process(from, context)); 
            node.getWhere().ifPresent(where -> process(where, context)); 
            node.getGroupBy().ifPresent(groupBy -> process(groupBy, context));
            node.getHaving().ifPresent(having -> process(having, context)); 
            
            return null;
        }
        
        @Override
        protected Void visitTable(Table node, Context context) {
            String tableName = node.getName().toString();
            String tableNameKey = tableName.toLowerCase();
            logger.debug("Visiting Table: {}", tableName);

            if (context.cteMap.containsKey(tableNameKey)) {
                WithQuery cte = context.cteMap.get(tableNameKey);
                Context cteProcessingContext = new Context(context.cteMap, new HashSet<>(context.visitedCtesInCurrentPathTraversal));

                if (cte != null && !cteProcessingContext.visitedCtesInCurrentPathTraversal.contains(tableNameKey)) {
                    logger.debug("Recursively processing Query node of CTE: {}", tableName);
                    cteProcessingContext.visitedCtesInCurrentPathTraversal.add(tableNameKey);
                    processCteQuery(cte.getQuery(), cteProcessingContext);
                } else if (cte != null) {
                    logger.debug("Already visited CTE path for {} in this traversal.", tableNameKey);
                }
            }
            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Context context) {
            logger.debug("Visiting AliasedRelation: {}, Alias: {}", node.getRelation(), node.getAlias().getValue());
            return process(node.getRelation(), context);
        }

        @Override
        protected Void visitJoin(Join node, Context context) {
            logger.debug("Visiting Join: Left={}, Right={}", node.getLeft(), node.getRight());
             process(node.getLeft(), context);
             process(node.getRight(), context);
             return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Context context) {
            logger.debug("Visiting TableSubquery. Processing its Query node.");
            return process(node.getQuery(), context); 
        }
        
        @Override
        protected Void visitSelect(Select node, Context context) {
            logger.debug("Visiting Select node. Items: {}", node.getSelectItems().size());
            for (SelectItem item : node.getSelectItems()) {
                process(item, context); 
            }
            return null;
        }
        
        @Override
        protected Void visitSingleColumn(SingleColumn node, Context context) {
            logger.debug("Visiting SingleColumn: {} Alias: {}", node.getExpression(), node.getAlias().orElse(null));
            return super.visitSingleColumn(node, context); 
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Context context) {
            logger.debug("Visiting AllColumns: Target: {}", node.getTarget().orElse(null));
            return null;
        }

        @Override
        protected Void visitWith(With node, Context context) {
            logger.debug("Visiting With...");
            // Process all CTE definitions and collect their metadata
            for (WithQuery wq : node.getQueries()) {
                String cteName = wq.getName().getValue().toLowerCase();
                logger.debug("Found cte with name: {}", cteName);
                if (!context.visitedCtesInCurrentPathTraversal.contains(cteName)) { 
                     logger.debug("visitWith: Processing CTE definition body for: {}", cteName);
                     Context cteProcessingContext = new Context(context.cteMap, new HashSet<>(context.visitedCtesInCurrentPathTraversal));
                     cteProcessingContext.visitedCtesInCurrentPathTraversal.add(cteName);
                     
                     // Process the CTE query and collect its metadata
                     processCteQuery(wq.getQuery(), cteProcessingContext);
                }
            }
            // Allow default visitor to proceed to the main query body after WITH
            return super.visitWith(node, context);
        }

        private void processCteQuery(Query cteQuery, Context context) {
            if (!(cteQuery.getQueryBody() instanceof QuerySpecification)) {
                logger.debug("Processing non-QuerySpecification CTE...");
                process(cteQuery, context);
                return;
            }

            QuerySpecification outerSpec = (QuerySpecification) cteQuery.getQueryBody();
            Optional<String> aliasOpt = findImmediateAlias(outerSpec.getFrom().orElse(null));
            String alias = aliasOpt.orElse("");
            logger.debug("processCteQuery - Found immediate alias: {}", alias);

            // Extract projections from outer spec (ignores SELECT *)
            Set<String> cteProjections = extractProjections(outerSpec, Optional.of(alias), context.cteMap);
            logger.debug("processCteQuery - Found cteProjections from outer spec: {}", cteProjections);
            allProjections.addAll(cteProjections);

            // Extract filters from outer spec WHERE clause
            Set<String> cteFilters = extractFilterColumns(outerSpec, Optional.of(alias), context.cteMap);
            logger.debug("processCteQuery - Found cteFilters from outer spec: {}", cteFilters);

            // CRITICAL: Process nested subqueries recursively to find explicit columns and filters
            if (outerSpec.getFrom().isPresent()) {
                logger.debug("processCteQuery - Processing FROM clause recursively");
                processFromClauseRecursively(outerSpec.getFrom().get(), Optional.of(alias), context);
            }

            allFilters.addAll(cteFilters);
            
            // Continue with regular processing
            process(cteQuery, context);
        }

        // Enhanced method to recursively process FROM clauses and nested subqueries
        private void processFromClauseRecursively(Relation from, Optional<String> currentAlias, Context context) {
            logger.debug("processFromClauseRecursively - Processing relation: {}", from.getClass().getSimpleName());
            
            if (from instanceof TableSubquery) {
                TableSubquery ts = (TableSubquery) from;
                logger.debug("processFromClauseRecursively - Found TableSubquery, processing inner query");
                
                if (ts.getQuery().getQueryBody() instanceof QuerySpecification) {
                    QuerySpecification innerSpec = (QuerySpecification) ts.getQuery().getQueryBody();
                    
                    // Determine the alias for the inner query
                    Optional<String> innerAlias = findImmediateAlias(innerSpec.getFrom().orElse(null));
                    logger.debug("processFromClauseRecursively - Inner query alias: {}", innerAlias.orElse("none"));
                    
                    // Extract projections and filters from the inner query
                    Set<String> innerProjections = extractProjections(innerSpec, innerAlias, context.cteMap);
                    Set<String> innerFilters = extractFilterColumns(innerSpec, innerAlias, context.cteMap);
                    
                    logger.debug("processFromClauseRecursively - Found inner projections: {}", innerProjections);
                    logger.debug("processFromClauseRecursively - Found inner filters: {}", innerFilters);
                    
                    allProjections.addAll(innerProjections);
                    allFilters.addAll(innerFilters);
                    
                    // Continue recursively if there are more nested levels
                    if (innerSpec.getFrom().isPresent()) {
                        logger.debug("processFromClauseRecursively - Continuing recursion deeper");
                        processFromClauseRecursively(innerSpec.getFrom().get(), innerAlias, context);
                    }
                }
            } else if (from instanceof AliasedRelation) {
                AliasedRelation ar = (AliasedRelation) from;
                String newAlias = ar.getAlias().getValue();
                logger.debug("processFromClauseRecursively - Found AliasedRelation with alias: {}", newAlias);
                processFromClauseRecursively(ar.getRelation(), Optional.of(newAlias), context);
            } else if (from instanceof Table) {
                Table table = (Table) from;
                logger.debug("processFromClauseRecursively - Reached base table: {}", table.getName().toString());
                // This is the base table - no further processing needed
            } else if (from instanceof Join) {
                Join join = (Join) from;
                logger.debug("processFromClauseRecursively - Found Join, processing left and right");
                processFromClauseRecursively(join.getLeft(), currentAlias, context);
                processFromClauseRecursively(join.getRight(), currentAlias, context);
            }
        }

        /**
         * If `fromNode` is an AliasedRelation, return its alias immediately.
         * If it's a TableSubquery, unwrap it until we find an AliasedRelation.
         * Otherwise return Optional.empty().
         */
        private Optional<String> findImmediateAlias(Relation relation) {
            if (relation instanceof AliasedRelation) {
                return Optional.of(((AliasedRelation) relation).getAlias().getValue());
            }
            if (relation instanceof TableSubquery) {
                Query innerQuery = ((TableSubquery) relation).getQuery();
                if (innerQuery.getQueryBody() instanceof QuerySpecification) {
                    QuerySpecification innerSpec = (QuerySpecification) innerQuery.getQueryBody();
                    // innerSpec.getFrom() returns Optional<Relation>
                    return innerSpec.getFrom().map(this::findImmediateAlias).orElse(Optional.empty());
                }
            }
            return Optional.empty();
        }

        private Set<String> extractProjections(QuerySpecification spec, Optional<String> alias, Map<String, WithQuery> cteMap) {
            Set<String> cols = new LinkedHashSet<>();
            logger.debug("extractProjections for QS with alias '{}'. SelectItems: {}", alias.orElse("none"), spec.getSelect().getSelectItems().size());
            
            for (SelectItem item : spec.getSelect().getSelectItems()) {
                if (item instanceof SingleColumn) {
                    SingleColumn sc = (SingleColumn) item;
                    logger.trace("extractProjections - SingleColumn: {}, Alias: {}", sc.getExpression(), sc.getAlias().orElse(null));
                    cols.addAll(findBaseIdentifiers(sc.getExpression(), alias, cteMap));
                } else if (item instanceof AllColumns) {
                    // Ignore SELECT * - assume required columns are mentioned explicitly elsewhere
                    AllColumns ac = (AllColumns) item;
                    logger.trace("extractProjections - Ignoring SELECT * (assuming explicit columns mentioned elsewhere)");
                    if (ac.getTarget().isPresent()) {
                        logger.trace("extractProjections - Ignoring SELECT {}.* (target: {})", 
                            ac.getTarget().get().toString(), ac.getTarget().get().toString());
                    }
                }
            }
            return cols;
        }

       private Set<String> extractGroupBy(QuerySpecification spec, Optional<String> alias, Map<String, WithQuery> cteMap) {
            List<SelectItem> selectItems = spec.getSelect().getSelectItems();
            Set<String> groupBys = new LinkedHashSet<>();
            if (!spec.getGroupBy().isPresent()) return groupBys;

            logger.debug("extractGroupBy for QS with alias '{}'", alias.orElse("none"));
            spec.getGroupBy().get().getGroupingElements().stream() 
                    .flatMap(ge -> (ge instanceof SimpleGroupBy) ? ((SimpleGroupBy) ge).getExpressions().stream() : Stream.empty())
                    .map(expr -> {
                        Expression resolvedExpr = expr;
                        if (expr instanceof LongLiteral) {
                            long ordinal = Long.parseLong(((LongLiteral) expr).getValue());
                            int index = (int) ordinal - 1; 
                            if (index >= 0 && index < selectItems.size() && selectItems.get(index) instanceof SingleColumn) {
                                resolvedExpr = ((SingleColumn) selectItems.get(index)).getExpression();
                                logger.trace("extractGroupBy - Resolved ordinal {} to expression: {}", ordinal, resolvedExpr);
                            } else {
                                logger.warn("GROUP BY ordinal {} is out of bounds or not a SingleColumn.", ordinal);
                            }
                        }
                        return resolvedExpr;
                    })
                    .forEach(expr -> groupBys.addAll(findBaseIdentifiers(expr, alias, cteMap))); 
            return groupBys;
        }

        private List<AggregationInfo> extractAggregations(QuerySpecification spec, Optional<String> alias, Map<String, WithQuery> cteMap) {
            Set<AggregationInfo> aggs = new LinkedHashSet<>();
            logger.debug("extractAggregations for QS with alias '{}'", alias.orElse("none"));
            spec.getSelect().getSelectItems().stream()
                    .filter(SingleColumn.class::isInstance)
                    .map(SingleColumn.class::cast)
                    .forEach(sc -> findAggsRecursive(sc.getExpression(), alias, aggs, cteMap));
            spec.getHaving().ifPresent(h -> findAggsRecursive(h, alias, aggs, cteMap));
            return new ArrayList<>(aggs);
        }

        private void findAggsRecursive(Expression expr, Optional<String> alias, Set<AggregationInfo> collector, Map<String, WithQuery> cteMap) {
             new DefaultTraversalVisitor<Void>() { 
                @Override
                protected Void visitFunctionCall(FunctionCall fc, Void passedContext) {
                    String name = fc.getName().toString().toUpperCase();
                    Set<String> agNames = Set.of("SUM", "COUNT", "AVG", "MIN", "MAX"); 

                    if (agNames.contains(name)) {
                        logger.trace("findAggsRecursive - Found aggregate function: {}", name);
                        List<String> args = fc.getArguments().stream()
                                .flatMap(a -> findBaseIdentifiers(a, alias, cteMap).stream()) 
                                .collect(Collectors.toList());
                        
                        AggregationInfo aggInfo = new AggregationInfo(
                                fc.getName().toString(),
                                args,
                                fc.isDistinct()
                        );
                        collector.add(aggInfo);
                        return null; 
                    } else {
                         return super.visitFunctionCall(fc, passedContext); 
                    }
                }
            }.process(expr, null);
        }

       private Set<String> extractFilterColumns(QuerySpecification spec, Optional<String> alias, Map<String, WithQuery> cteMap) {
            Set<String> cols = new LinkedHashSet<>();
            if (!spec.getWhere().isPresent()) return cols;
            logger.debug("extractFilterColumns for QS with alias '{}', WHERE: {}", alias.orElse("none"), spec.getWhere().get());
            cols.addAll(findBaseIdentifiers(spec.getWhere().get(), alias, cteMap));
            return cols;
        }
    } 

    private static class AliasStripper extends ExpressionRewriter<Void> {
        private final String aliasToStrip;

        AliasStripper(String alias) {
            this.aliasToStrip = alias.toLowerCase();
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void ctx, ExpressionTreeRewriter<Void> rw) {
            if (node.getBase() instanceof Identifier && node.getField().isPresent()
                    && ((Identifier) node.getBase()).getValue().equalsIgnoreCase(aliasToStrip)) {
                logger.trace("AliasStripper: Stripped alias '{}' from '{}', returning '{}'", aliasToStrip, node, node.getField().get());
                return node.getField().get(); 
            }
            Expression newBase = rw.rewrite(node.getBase(), ctx);
            if (newBase != node.getBase()) {
                Identifier field = node.getField().orElseThrow(() -> new IllegalStateException("DereferenceExpression missing field"));
                if (node.getLocation().isPresent()) {
                    return new DereferenceExpression(node.getLocation().get(), newBase, field);
                } else {
                    return new DereferenceExpression(newBase, field);
                }
            }
            return node; 
        }

        @Override
        public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> rewriter) {
            return node;
        }
    }
}