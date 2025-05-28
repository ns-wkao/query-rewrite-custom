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

/**
 * Extracts metadata (base table, projections, filters, etc.) from a Trino SQL AST.
 * It uses a visitor pattern to traverse the query structure and now extracts
 * aggregations into structured AggregationInfo objects.
 * It also handles GROUP BY ordinals by resolving them against the SELECT list.
 * It now attempts to resolve projections/group-bys to their base columns.
 */
public class QueryMetadataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetadataExtractor.class);

    /**
     * Represents the identified base relation and its optional alias.
     */
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

    /**
     * Extracts metadata from the given SQL query Statement.
     *
     * @param queryStatement The Trino AST Statement.
     * @return The extracted MaterializedViewMetadata.
     * @throws IllegalArgumentException if the statement is not a Query or metadata cannot be extracted.
     */
    public static QueryMetadata extractMetadataFromQuery(Statement queryStatement) {
        if (!(queryStatement instanceof Query)) {
            throw new IllegalArgumentException("Expected a Query statement but got: " + queryStatement.getClass().getSimpleName());
        }

        MetadataVisitor visitor = new MetadataVisitor();
        QueryMetadata metadata = visitor.process((Query) queryStatement);

        logger.info("Extracted metadata: baseTable={}, alias={}, projections={}, groupBy={}, aggregations={}, filters={}",
                metadata.getBaseTable(),
                metadata.getTableAlias().orElse("N/A"),
                metadata.getProjectionColumns(),
                metadata.getGroupByColumns(),
                metadata.getAggregations().stream().map(AggregationInfo::toString).collect(Collectors.toList()),
                metadata.getFilterColumns());

        return metadata;
    }

    /**
     * Finds all base identifiers (potential column names) within an Expression,
     * traversing through functions and aliases.
     */
    private static Set<String> findBaseIdentifiers(Expression expression, Optional<String> aliasToStrip) {
        Set<String> identifiers = new HashSet<>();
        
        // Apply alias stripping first if an alias exists
        Expression processedExpr = aliasToStrip.map(alias -> 
            ExpressionTreeRewriter.rewriteWith(new AliasStripper(alias), expression))
            .orElse(expression);

        new DefaultTraversalVisitor<Void>() {
            @Override
            protected Void visitIdentifier(Identifier node, Void context) {
                identifiers.add(node.getValue().toLowerCase());
                return null;
            }

            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, Void context) {
                // If the base is an Identifier, we assume it's a table/alias and only take the field.
                // If the base is complex (like a function), we process it.
                // This aims to get only the column name at the end.
                if (node.getField().isPresent()) {
                    identifiers.add(node.getField().get().getValue().toLowerCase());
                    // Only recurse if base is *not* a simple identifier.
                    if (!(node.getBase() instanceof Identifier)) {
                        process(node.getBase(), context);
                    }
                } else {
                    // Should not happen often, but traverse base if no field.
                    process(node.getBase(), context);
                }
                return null; // Stop default traversal down this path
            }

            @Override
            protected Void visitFunctionCall(FunctionCall node, Void context) {
                 // Recurse into arguments.
                 node.getArguments().forEach(arg -> process(arg, context));
                 return null;
            }
            
            // Handle * in COUNT(*) - add * so it can be handled/ignored later.
            @Override
            protected Void visitAllColumns(AllColumns node, Void context) {
                identifiers.add("*");
                return null;
            }

        }.process(processedExpr, null);

        logger.trace("Found base identifiers for '{}': {}", SqlFormatter.formatSql(expression), identifiers);
        return identifiers;
    }


    /**
     * The main AST visitor for extracting query metadata.
     */
    private static class MetadataVisitor extends AstVisitor<Void, MetadataVisitor.Context> {

        private final QueryMetadata metadata = new QueryMetadata();
        private final Set<String> allProjections = new HashSet<>();
        private final Set<String> allFilters = new HashSet<>();
        private final Set<AggregationInfo> allAggregations = new HashSet<>();
        private final Set<String> allGroupBys = new HashSet<>();
        private RelationInfo baseRelationInfo = null;
        // Keep track of visited CTEs during base table search to avoid loops
        private final Set<String> visitedCtesInSearch = new HashSet<>();
        private final Map<String, WithQuery> cteMap = new HashMap<>();

        static class Context {
            final Optional<With> withClause;
            final Map<String, WithQuery> cteMap; // Pass CTE map in context

            Context(Optional<With> withClause) {
                this.withClause = withClause;
                this.cteMap = new HashMap<>();
                withClause.ifPresent(w -> w.getQueries().forEach(q -> this.cteMap.put(q.getName().getValue().toLowerCase(), q)));
            }
             Context(Optional<With> withClause, Map<String, WithQuery> cteMap) {
                this.withClause = withClause;
                this.cteMap = cteMap;
            }
        }

        public QueryMetadata process(Query query) {
            Context context = new Context(query.getWith());
            
            // Find base table first
            baseRelationInfo = findBaseTableRecursive(query.getQueryBody(), context);
            if (baseRelationInfo == null) {
                throw new IllegalStateException("Could not determine base table for the query.");
            }

            // Now, traverse the *whole* tree again to collect *all* columns/aggs/filters.
            process(query.getQueryBody(), context);

            metadata.setBaseTable(baseRelationInfo.baseTableName);
            metadata.setTableAlias(baseRelationInfo.alias);
            
            // Remove '*' for now
            allProjections.remove("*");
            // Post-process: Remove table alias if present.
            baseRelationInfo.alias.ifPresent(a -> allProjections.remove(a.toLowerCase()));

            metadata.setProjectionColumns(new ArrayList<>(allProjections));
            metadata.setFilterColumns(new ArrayList<>(allFilters));
            metadata.setAggregations(new ArrayList<>(allAggregations));
            metadata.setGroupByColumns(new ArrayList<>(allGroupBys));

            return metadata;
        }

        private RelationInfo findBaseTableRecursive(Node node, Context context) {
            if (node instanceof Table) {
                Table table = (Table) node;
                String tableName = table.getName().toString().toLowerCase();
                if (!context.cteMap.containsKey(tableName)) {
                    return new RelationInfo(table.getName().toString(), Optional.empty());
                } else if (!visitedCtesInSearch.contains(tableName)) {
                    visitedCtesInSearch.add(tableName); // Mark as visited
                    return findBaseTableRecursive(context.cteMap.get(tableName).getQuery().getQueryBody(), context);
                }
            } else if (node instanceof AliasedRelation) {
                AliasedRelation ar = (AliasedRelation) node;
                RelationInfo base = findBaseTableRecursive(ar.getRelation(), context);
                if (base != null) {
                    return new RelationInfo(base.baseTableName, Optional.of(ar.getAlias().getValue()));
                }
            } else if (node instanceof TableSubquery) {
                return findBaseTableRecursive(((TableSubquery) node).getQuery().getQueryBody(), context);
            } else if (node instanceof QuerySpecification) {
                return ((QuerySpecification) node).getFrom()
                           .map(from -> findBaseTableRecursive(from, context))
                           .orElse(null);
            } else if (node instanceof Join) {
                RelationInfo left = findBaseTableRecursive(((Join) node).getLeft(), context);
                return (left != null) ? left : findBaseTableRecursive(((Join) node).getRight(), context);
            }
            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Context context) {
            Optional<String> alias = node.getFrom()
                                         .filter(AliasedRelation.class::isInstance)
                                         .map(AliasedRelation.class::cast)
                                         .map(ar -> Optional.of(ar.getAlias().getValue()))
                                         .orElse(baseRelationInfo != null ? baseRelationInfo.alias : Optional.empty());

            // Extract from ALL levels
            allProjections.addAll(extractProjections(node, alias));
            allFilters.addAll(extractFilterColumns(node, alias));
            allAggregations.addAll(extractAggregations(node, alias));
            allGroupBys.addAll(extractGroupBy(node, alias));

            // Continue traversal into FROM
            node.getFrom().ifPresent(from -> process(from, context));
            return null;
        }
        
        @Override
        protected Void visitTable(Table node, Context context) {
            String tableName = node.getName().toString().toLowerCase();
            // If it's a CTE, traverse into it
            if (context.cteMap.containsKey(tableName)) {
                logger.trace("Traversing into CTE: {}", tableName);
                process(context.cteMap.get(tableName).getQuery().getQueryBody(), context);
            }
            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Context context) {
            return process(node.getQuery().getQueryBody(), context);
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Context context) {
             return process(node.getRelation(), context);
        }

        @Override
        protected Void visitJoin(Join node, Context context) {
             process(node.getLeft(), context);
             process(node.getRight(), context);
             return null;
        }

        private String formatExpr(Expression expr) {
            return SqlFormatter.formatSql(expr);
        }

        /**
         * Extracts base columns used in projections.
         */
        private Set<String> extractProjections(QuerySpecification spec, Optional<String> alias) {
            Set<String> cols = new HashSet<>();
            for (SelectItem item : spec.getSelect().getSelectItems()) {
                if (item instanceof SingleColumn) {
                    cols.addAll(findBaseIdentifiers(((SingleColumn) item).getExpression(), alias));
                } else if (item instanceof AllColumns) {
                    cols.add("*");
                }
            }
            return cols;
        }

       /**
         * Extracts base columns used in GROUP BY, resolving ordinals.
         */
       private Set<String> extractGroupBy(QuerySpecification spec, Optional<String> alias) {
            List<SelectItem> selectItems = spec.getSelect().getSelectItems();
            Set<String> groupBys = new HashSet<>();

            spec.getGroupBy().stream()
                    .flatMap(gb -> gb.getGroupingElements().stream())
                    .flatMap(ge -> (ge instanceof SimpleGroupBy) ? ((SimpleGroupBy) ge).getExpressions().stream() : Stream.empty())
                    .map(expr -> {
                        Expression resolvedExpr = expr;
                        if (expr instanceof LongLiteral) {
                            long ordinal = Long.parseLong(((LongLiteral) expr).getValue());
                            int index = (int) ordinal - 1;
                            if (index >= 0 && index < selectItems.size() && selectItems.get(index) instanceof SingleColumn) {
                                resolvedExpr = ((SingleColumn) selectItems.get(index)).getExpression();
                            } else {
                                logger.warn("GROUP BY ordinal {} is out of bounds or not a SingleColumn.", ordinal);
                            }
                        }
                        return resolvedExpr;
                    })
                    .forEach(expr -> groupBys.addAll(findBaseIdentifiers(expr, alias))); // Use findBaseIdentifiers
            return groupBys;
        }

        /**
         * Extracts aggregations into a List of AggregationInfo objects.
         */
        private List<AggregationInfo> extractAggregations(QuerySpecification spec, Optional<String> alias) {
            Set<AggregationInfo> aggs = new HashSet<>();
            spec.getSelect().getSelectItems().stream()
                    .filter(SingleColumn.class::isInstance)
                    .map(SingleColumn.class::cast)
                    .forEach(sc -> findAggsRecursive(sc.getExpression(), alias, aggs));
            spec.getHaving().ifPresent(h -> findAggsRecursive(h, alias, aggs));
            return new ArrayList<>(aggs);
        }

        /**
         * Recursively finds FunctionCalls representing aggregations and adds them
         * as AggregationInfo objects to the collector set.
         */
        private void findAggsRecursive(Expression expr, Optional<String> alias, Set<AggregationInfo> collector) {
             new DefaultTraversalVisitor<Void>() {
                @Override
                protected Void visitFunctionCall(FunctionCall fc, Void context) {
                    String name = fc.getName().toString().toUpperCase();
                    Set<String> agNames = Set.of("SUM", "COUNT", "AVG", "MIN", "MAX");

                    if (agNames.contains(name)) {
                        List<String> args = fc.getArguments().stream()
                                // Find base identifiers for each argument
                                .flatMap(a -> findBaseIdentifiers(a, alias).stream())
                                .collect(Collectors.toList());
                        
                        // Use the normalized arguments for AggregationInfo
                        AggregationInfo aggInfo = new AggregationInfo(
                                fc.getName().toString(),
                                args,
                                fc.isDistinct()
                        );
                        collector.add(aggInfo);
                        return null; // Stop recursion here for this aggregation
                    } else {
                         // Recurse into arguments if not an aggregation
                         return super.visitFunctionCall(fc, context);
                    }
                }
            }.process(expr, null);
        }

        /**
         * Extracts base columns used in filter (WHERE) clauses.
         */
       private Set<String> extractFilterColumns(QuerySpecification spec, Optional<String> alias) {
            Set<String> cols = new HashSet<>();
            spec.getWhere().ifPresent(where -> cols.addAll(findBaseIdentifiers(where, alias)));
            return cols;
        }


        private Expression normalizeExpression(Expression expr, Optional<String> alias) {
            if (alias.isEmpty() || expr == null) {
                return expr;
            }
            return ExpressionTreeRewriter.rewriteWith(new AliasStripper(alias.get()), expr);
        }
    }

    /**
     * Rewriter to strip a specific alias from DereferenceExpressions.
     */
    private static class AliasStripper extends ExpressionRewriter<Void> {
        private final String aliasToStrip;

        AliasStripper(String alias) {
            this.aliasToStrip = alias.toLowerCase();
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void ctx, ExpressionTreeRewriter<Void> rw) {
            if (node.getBase() instanceof Identifier && node.getField().isPresent()
                    && ((Identifier) node.getBase()).getValue().equalsIgnoreCase(aliasToStrip)) {
                return node.getField().get();
            }
            return rw.defaultRewrite(node, ctx);
        }

        @Override
        public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> rewriter) {
            return node;
        }
    }
}