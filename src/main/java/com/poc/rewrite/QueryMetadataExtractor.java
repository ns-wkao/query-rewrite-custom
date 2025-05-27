package com.poc.rewrite;

import com.poc.rewrite.config.AggregationInfo; // Import the new class
import com.poc.rewrite.config.MaterializedViewMetadata;
import io.trino.sql.SqlFormatter;
import io.trino.sql.tree.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Extracts metadata (base table, projections, filters, etc.) from a Trino SQL AST.
 * It uses a visitor pattern to traverse the query structure and now extracts
 * aggregations into structured AggregationInfo objects.
 * (Simplified: No COUNT(*) handling).
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
    public static MaterializedViewMetadata extractMetadataFromQuery(Statement queryStatement) {
        if (!(queryStatement instanceof Query)) {
            throw new IllegalArgumentException("Expected a Query statement but got: " + queryStatement.getClass().getSimpleName());
        }

        MetadataVisitor visitor = new MetadataVisitor();
        MaterializedViewMetadata metadata = visitor.process((Query) queryStatement);

        logger.info("Extracted metadata: baseTable={}, alias={}, projections={}, groupBy={}, aggregations={}, filters={}",
                metadata.getBaseTable(),
                metadata.getTableAlias().orElse("N/A"),
                metadata.getProjectionColumns(),
                metadata.getGroupByColumns(),
                metadata.getAggregations().stream().map(AggregationInfo::toString).collect(Collectors.toList()), // Log as string
                metadata.getFilterColumns());

        return metadata;
    }

    /**
     * The main AST visitor for extracting query metadata.
     */
    private static class MetadataVisitor extends AstVisitor<Void, MetadataVisitor.Context> {

        private final MaterializedViewMetadata metadata = new MaterializedViewMetadata();
        private final Set<String> allProjections = new HashSet<>();
        private final Set<String> allFilters = new HashSet<>();
        private final Set<AggregationInfo> allAggregations = new HashSet<>();
        private final Set<String> allGroupBys = new HashSet<>();
        private RelationInfo baseRelationInfo = null;
        private boolean processedMainQuerySpec = false;

        static class Context {
            final Optional<With> withClause;

            Context(Optional<With> withClause) {
                this.withClause = withClause;
            }
        }

        public MaterializedViewMetadata process(Query query) {
            Context context = new Context(query.getWith());
            process(query.getQueryBody(), context);

            if (baseRelationInfo == null) {
                findAnyTable(query.getQueryBody());
                if(baseRelationInfo == null){
                     throw new IllegalStateException("Could not determine base table for the query.");
                }
            }

            metadata.setBaseTable(baseRelationInfo.baseTableName);
            metadata.setTableAlias(baseRelationInfo.alias);
            metadata.setProjectionColumns(new ArrayList<>(allProjections));
            metadata.setFilterColumns(new ArrayList<>(allFilters));
            metadata.setAggregations(new ArrayList<>(allAggregations));
            metadata.setGroupByColumns(new ArrayList<>(allGroupBys));

            return metadata;
        }

        private void findAnyTable(Node node) {
            new DefaultTraversalVisitor<Void>() {
                @Override
                protected Void visitTable(Table tableNode, Void context) {
                    if (baseRelationInfo == null) {
                        baseRelationInfo = new RelationInfo(tableNode.getName().toString(), Optional.empty());
                    }
                    return null;
                }
            }.process(node, null);
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Context context) {
            if (processedMainQuerySpec) {
                return null;
            }

            RelationInfo currentRelationInfo = node.getFrom()
                    .map(from -> findRelationInfoRecursive(from, context))
                    .orElse(null);

            if (currentRelationInfo != null && baseRelationInfo == null) {
                baseRelationInfo = currentRelationInfo;
                allProjections.addAll(extractProjections(node, baseRelationInfo.alias));
                allFilters.addAll(extractFilterColumns(node, baseRelationInfo.alias));
                allAggregations.addAll(extractAggregations(node, baseRelationInfo.alias));
                allGroupBys.addAll(extractGroupBy(node, baseRelationInfo.alias));
                processedMainQuerySpec = true;
            } else if (currentRelationInfo != null) {
                logger.debug("Skipping metadata extraction from nested QuerySpecification based on: {}", currentRelationInfo);
            } else if (baseRelationInfo == null) {
                 logger.warn("QuerySpecification without FROM clause encountered, base table might be missed.");
            }

            return null;
        }

        @Override
        protected Void visitSetOperation(SetOperation node, Context context) {
            logger.warn("Encountered SetOperation (e.g., UNION). Processing only the first relation for metadata extraction.");
            process(node.getRelations().get(0), context);
            return null;
        }

        @Override
        protected Void visitWith(With node, Context context) {
            return super.visitWith(node, context);
        }

        private RelationInfo findRelationInfoRecursive(Relation node, Context context) {
             if (node instanceof Table) {
                Table table = (Table) node;
                String tableName = table.getName().toString();
                if (context.withClause.isPresent()) {
                    for (WithQuery cte : context.withClause.get().getQueries()) {
                        if (cte.getName().getValue().equalsIgnoreCase(tableName)) {
                            logger.debug("Tracing into CTE: {}", tableName);
                            MetadataVisitor subVisitor = new MetadataVisitor();
                            subVisitor.process(cte.getQuery().getQueryBody(), context);
                            return subVisitor.baseRelationInfo;
                        }
                    }
                }
                return new RelationInfo(tableName, Optional.empty());
            } else if (node instanceof AliasedRelation) {
                AliasedRelation ar = (AliasedRelation) node;
                RelationInfo base = findRelationInfoRecursive(ar.getRelation(), context);
                return new RelationInfo(base.baseTableName, Optional.of(ar.getAlias().getValue()));
            } else if (node instanceof TableSubquery) {
                logger.debug("Tracing into TableSubquery.");
                MetadataVisitor subVisitor = new MetadataVisitor();
                subVisitor.process(((TableSubquery) node).getQuery().getQueryBody(), context);
                return subVisitor.baseRelationInfo;
            } else if (node instanceof Join) {
                logger.warn("JOIN encountered; defaulting to left relation for base table.");
                return findRelationInfoRecursive(((Join) node).getLeft(), context);
            }
            throw new IllegalArgumentException("Unsupported relation type: " + node.getClass().getSimpleName());
        }

        /**
         * Formats an expression to its SQL string representation.
         */
        private String formatExpr(Expression expr) {
            return SqlFormatter.formatSql(expr);
        }

        private List<String> extractProjections(QuerySpecification spec, Optional<String> alias) {
            List<String> cols = new ArrayList<>();
            for (SelectItem item : spec.getSelect().getSelectItems()) {
                if (item instanceof SingleColumn) {
                    SingleColumn sc = (SingleColumn) item;
                    Expression norm = normalizeExpression(sc.getExpression(), alias);
                    cols.add(sc.getAlias().map(Identifier::getValue).orElse(formatExpr(norm)));
                } else if (item instanceof AllColumns) {
                    cols.add("*");
                } else {
                   logger.warn("Unsupported projection type: {}", item.getClass().getSimpleName());
                   cols.add(item.toString());
                }
            }
            return cols;
        }

       private List<String> extractGroupBy(QuerySpecification spec, Optional<String> alias) {
            return spec.getGroupBy().stream()
                    .flatMap(gb -> gb.getGroupingElements().stream())
                    .flatMap(ge -> {
                        if (ge instanceof SimpleGroupBy) {
                            return ((SimpleGroupBy) ge).getExpressions().stream();
                        } else {
                            logger.warn("Unsupported GroupingElement: {}", ge.getClass().getSimpleName());
                            return Stream.empty();
                        }
                    })
                    .map(expr -> normalizeExpression(expr, alias))
                    .map(this::formatExpr)
                    .collect(Collectors.toList());
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
                        // Extract arguments as strings
                        List<String> args = fc.getArguments().stream()
                                .map(a -> normalizeExpression(a, alias))
                                .map(QueryMetadataExtractor.MetadataVisitor.this::formatExpr)
                                .collect(Collectors.toList());

                        // Create and add AggregationInfo
                        AggregationInfo aggInfo = new AggregationInfo(
                                fc.getName().toString(),
                                args,
                                fc.isDistinct()
                        );
                        collector.add(aggInfo);
                        return null; // Stop recursion here
                    } else {
                         return super.visitFunctionCall(fc, context); // Recurse
                    }
                }
            }.process(expr, null);
        }


       private List<String> extractFilterColumns(QuerySpecification spec, Optional<String> alias) {
            Set<String> cols = new HashSet<>();
            spec.getWhere().ifPresent(where -> {
                new DefaultTraversalVisitor<Void>() {
                    @Override
                    protected Void visitIdentifier(Identifier id, Void ctx) {
                        cols.add(formatExpr(normalizeExpression(id, alias)));
                        return null;
                    }
                    @Override
                    protected Void visitDereferenceExpression(DereferenceExpression d, Void ctx) {
                        cols.add(formatExpr(normalizeExpression(d, alias)));
                        return null;
                    }
                    @Override
                    protected Void visitFunctionCall(FunctionCall node, Void context) {
                         node.getArguments().forEach(arg -> process(arg, context));
                         return null;
                    }
                }.process(where, null);
            });
            return new ArrayList<>(cols);
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