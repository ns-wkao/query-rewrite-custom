package com.poc.rewrite;

import com.poc.rewrite.config.MaterializedViewMetadata;

import io.trino.sql.SqlFormatter;
import io.trino.sql.tree.*;
import io.trino.sql.parser.ParsingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryMetadataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetadataExtractor.class);

    private static class RelationInfo {
        final String baseTableName;
        final Optional<String> alias;

        RelationInfo(String baseTableName, Optional<String> alias) {
            this.baseTableName = baseTableName;
            this.alias = alias;
        }
    }

    public static MaterializedViewMetadata extractMetadataFromQuery(Statement queryStatement) {
        if (!(queryStatement instanceof Query)) {
            throw new IllegalArgumentException("Expected a Query statement but got: " + queryStatement.getClass().getSimpleName());
        }

        Query query = (Query) queryStatement;
        MaterializedViewMetadata metadata = new MaterializedViewMetadata();

        // Handle WITH clauses (CTEs) differently
        if (query.getWith().isPresent()) {
            return extractMetadataFromCTEQuery(query);
        } else {
            return extractMetadataFromSimpleQuery(query);
        }
    }

    private static MaterializedViewMetadata extractMetadataFromCTEQuery(Query query) {
        MaterializedViewMetadata metadata = new MaterializedViewMetadata();
        
        // For CTE queries, we need to analyze the main query (after WITH)
        // and trace back to find the ultimate base table through the CTE chain
        
        QueryBody mainQueryBody = query.getQueryBody();
        if (!(mainQueryBody instanceof QuerySpecification)) {
            throw new IllegalArgumentException("Complex query bodies with CTEs are not fully supported yet");
        }
        
        QuerySpecification mainSpec = (QuerySpecification) mainQueryBody;
        
        // Find the base table by tracing through CTEs
        With withClause = query.getWith().get();
        RelationInfo baseRelation = findBaseTableThroughCTEs(mainSpec, withClause);
        
        metadata.setBaseTable(baseRelation.baseTableName);
        metadata.setTableAlias(baseRelation.alias);
        
        // Extract metadata from the main query (the one that actually has GROUP BY, etc.)
        metadata.setProjectionColumns(extractProjections(mainSpec, Optional.empty()));
        metadata.setFilterColumns(extractFilterColumns(mainSpec, Optional.empty()));
        metadata.setAggregations(extractAggregations(mainSpec, Optional.empty()));
        metadata.setGroupByColumns(extractGroupBy(mainSpec, Optional.empty()));

        logger.info("Extracted CTE metadata: baseTable={}, alias={}, projections={}, groupBy={}, aggregations={}, filters={}",
                metadata.getBaseTable(),
                metadata.getTableAlias().orElse("N/A"),
                metadata.getProjectionColumns(),
                metadata.getGroupByColumns(),
                metadata.getAggregations(),
                metadata.getFilterColumns());
        
        return metadata;
    }

    private static MaterializedViewMetadata extractMetadataFromSimpleQuery(Query query) {
        MaterializedViewMetadata metadata = new MaterializedViewMetadata();

        List<QuerySpecification> specs = extractAllQuerySpecifications(query);
        Set<String> allProjections = new HashSet<>();
        Set<String> allFilters = new HashSet<>();
        Set<String> allAggregations = new HashSet<>();
        Set<String> allGroupBys = new HashSet<>();
        RelationInfo relationInfo = null;

        for (QuerySpecification spec : specs) {
            RelationInfo specRelation = extractRelationInfo(spec);
            if (relationInfo == null) {
                relationInfo = specRelation;
            }
            allProjections.addAll(extractProjections(spec, specRelation.alias));
            allFilters.addAll(extractFilterColumns(spec, specRelation.alias));
            allAggregations.addAll(extractAggregations(spec, specRelation.alias));
            allGroupBys.addAll(extractGroupBy(spec, specRelation.alias));
        }

        if (relationInfo == null) {
            throw new IllegalArgumentException("Could not determine base table.");
        }

        metadata.setBaseTable(relationInfo.baseTableName);
        metadata.setTableAlias(relationInfo.alias);
        metadata.setProjectionColumns(new ArrayList<>(allProjections));
        metadata.setFilterColumns(new ArrayList<>(allFilters));
        metadata.setAggregations(new ArrayList<>(allAggregations));
        metadata.setGroupByColumns(new ArrayList<>(allGroupBys));

        logger.info("Extracted metadata: baseTable={}, alias={}, projections={}, groupBy={}, aggregations={}, filters={}",
                metadata.getBaseTable(),
                metadata.getTableAlias().orElse("N/A"),
                metadata.getProjectionColumns(),
                metadata.getGroupByColumns(),
                metadata.getAggregations(),
                metadata.getFilterColumns());
        return metadata;
    }

    private static RelationInfo findBaseTableThroughCTEs(QuerySpecification mainSpec, With withClause) {
        // Start from the main query's FROM clause
        if (!mainSpec.getFrom().isPresent()) {
            throw new IllegalArgumentException("Query must have a FROM clause.");
        }
        
        Relation fromRelation = mainSpec.getFrom().get();
        return traceToBaseTable(fromRelation, withClause);
    }

    private static RelationInfo traceToBaseTable(Relation relation, With withClause) {
        if (relation instanceof Table) {
            Table table = (Table) relation;
            String tableName = table.getName().toString();
            
            // Check if this is a CTE reference
            for (WithQuery cte : withClause.getQueries()) {
                if (cte.getName().getValue().equalsIgnoreCase(tableName)) {
                    // This is a CTE reference, trace into the CTE definition
                    return traceToBaseTableInQuery(cte.getQuery(), withClause);
                }
            }
            
            // Not a CTE, this is the actual base table
            return new RelationInfo(tableName, Optional.empty());
            
        } else if (relation instanceof AliasedRelation) {
            AliasedRelation ar = (AliasedRelation) relation;
            RelationInfo base = traceToBaseTable(ar.getRelation(), withClause);
            return new RelationInfo(base.baseTableName, Optional.of(ar.getAlias().getValue()));
        
        } else if (relation instanceof TableSubquery) {
        TableSubquery ts = (TableSubquery) relation;
        // dive into the subquery’s body just like in traceToBaseTableInQuery
        return traceToBaseTableInQuery(ts.getQuery(), withClause);
        
        } else if (relation instanceof Join) {
            logger.warn("JOIN encountered; defaulting to left relation for base table.");
            return traceToBaseTable(((Join) relation).getLeft(), withClause);
            
        } else {
            throw new IllegalArgumentException("Unsupported relation type: " + relation.getClass().getSimpleName());
        }
    }

    private static RelationInfo traceToBaseTableInQuery(Query query, With withClause) {
        if (!(query.getQueryBody() instanceof QuerySpecification)) {
            throw new IllegalArgumentException("Complex CTE query bodies are not supported yet");
        }
        
        QuerySpecification spec = (QuerySpecification) query.getQueryBody();
        if (!spec.getFrom().isPresent()) {
            throw new IllegalArgumentException("CTE query must have a FROM clause.");
        }
        
        return traceToBaseTable(spec.getFrom().get(), withClause);
    }

    private static List<QuerySpecification> extractAllQuerySpecifications(Query query) {
        List<QuerySpecification> specs = new ArrayList<>();
        extractQuerySpecificationsRecursive(query.getQueryBody(), specs);
        return specs;
    }

    private static void extractQuerySpecificationsRecursive(QueryBody body, List<QuerySpecification> specs) {
        if (body instanceof QuerySpecification) {
            QuerySpecification spec = (QuerySpecification) body;
            specs.add(spec);

            // Drill into FROM
            spec.getFrom().ifPresent(from -> {
                findNestedSpecsInRelation(from, specs);
            });

            // Drill into WHERE, HAVING, SELECT expressions for any subqueries
            spec.getWhere().ifPresent(w -> extractSpecsFromExpression(w, specs));
            spec.getHaving().ifPresent(h -> extractSpecsFromExpression(h, specs));

        } else if (body instanceof SetOperation) {
            SetOperation setOp = (SetOperation) body;
            for (Relation rel : setOp.getRelations()) {
                if (rel instanceof TableSubquery) {
                    extractQuerySpecificationsRecursive(
                        ((TableSubquery) rel).getQuery().getQueryBody(),
                        specs
                    );
                }
            }
        } else if (body instanceof TableSubquery) {
            extractQuerySpecificationsRecursive(
                ((TableSubquery) body).getQuery().getQueryBody(),
                specs
            );
        }
    }

    /** 
     * Look inside a Relation (FROM or JOIN), recursing into any sub-query
     * and also descending through joins/aliased relations.
     */
    private static void findNestedSpecsInRelation(Relation relation, List<QuerySpecification> specs) {
        if (relation instanceof Table) {
            // no subquery here
            return;
        }
        if (relation instanceof AliasedRelation) {
            findNestedSpecsInRelation(
                ((AliasedRelation) relation).getRelation(),
                specs
            );
            return;
        }
        if (relation instanceof TableSubquery) {
            extractQuerySpecificationsRecursive(
                ((TableSubquery) relation).getQuery().getQueryBody(),
                specs
            );
            return;
        }
        if (relation instanceof Join) {
            Join join = (Join) relation;
            findNestedSpecsInRelation(join.getLeft(), specs);
            findNestedSpecsInRelation(join.getRight(), specs);
            return;
        }
        // add other relation types (Lateral, Unnest, etc.) as needed
    }

    /**
     * Walk an expression looking for any embedded sub-query, so that set-ops or
     * nested SELECTs inside WHERE/HAVING/SELECT also get their QuerySpecifications collected.
     */
    private static void extractSpecsFromExpression(Expression expr, List<QuerySpecification> specs) {
        new DefaultTraversalVisitor<Void>() {
            @Override
            protected Void visitSubqueryExpression(SubqueryExpression node, Void context) {
                extractQuerySpecificationsRecursive(
                    node.getQuery().getQueryBody(), specs
                );
                return super.visitSubqueryExpression(node, context);
            }
            @Override
            protected Void visitFunctionCall(FunctionCall node, Void context) {
                node.getArguments().forEach(arg -> process(arg, context));
                node.getFilter().ifPresent(f -> process(f, context));
                return null;
            }
            @Override
            protected Void visitExpression(Expression node, Void context) {
                for (Node child : node.getChildren()) {
                    if (child instanceof Expression) {
                        process((Expression) child, context);
                    }
                }
                return null;
            }
        }.process(expr, null);
    }

    private static QuerySpecification extractQuerySpecification(Query query) {
        if (query.getQueryBody() instanceof QuerySpecification) {
            return (QuerySpecification) query.getQueryBody();
        }
        throw new IllegalArgumentException("Unsupported query structure. Only simple SELECT queries are supported.");
    }

    private static String formatExpr(Expression expr) {
        return SqlFormatter.formatSql(expr);
    }

    private static RelationInfo extractRelationInfo(QuerySpecification spec) {
        return spec.getFrom()
                .map(QueryMetadataExtractor::findRelationInfoRecursive)
                .orElseThrow(() -> new IllegalArgumentException("Query must have a FROM clause."));
    }

    private static RelationInfo findRelationInfoRecursive(Relation node) {
        if (node instanceof Table) {
            return new RelationInfo(((Table) node).getName().toString(), Optional.empty());
        } else if (node instanceof AliasedRelation) {
            AliasedRelation ar = (AliasedRelation) node;
            RelationInfo base = findRelationInfoRecursive(ar.getRelation());
            return new RelationInfo(base.baseTableName, Optional.of(ar.getAlias().getValue()));
        } else if (node instanceof TableSubquery) {
            Query nested = ((TableSubquery) node).getQuery();
            QuerySpecification nestedSpec = extractQuerySpecification(nested);
            return extractRelationInfo(nestedSpec);
        } else if (node instanceof Join) {
            logger.warn("JOIN encountered; defaulting to left relation for base table.");
            return findRelationInfoRecursive(((Join) node).getLeft());
        }
        throw new IllegalArgumentException("Unsupported relation type: " + node.getClass().getSimpleName());
    }

    private static List<String> extractProjections(QuerySpecification spec, Optional<String> alias) {
        List<String> cols = new ArrayList<>();
        for (SelectItem item : spec.getSelect().getSelectItems()) {
            if (item instanceof SingleColumn) {
                SingleColumn sc = (SingleColumn) item;
                if (sc.getAlias().isPresent()) {
                    cols.add(sc.getAlias().get().getValue());
                } else {
                    Expression norm = normalizeExpression(sc.getExpression(), alias);
                    cols.add(formatExpr(norm));
                }
            } else if (item instanceof AllColumns) {
                AllColumns ac = (AllColumns) item;
                if (ac.getTarget().isPresent() && alias.filter(a -> a.equalsIgnoreCase(ac.getTarget().get().toString())).isPresent()) {
                    cols.add("*");
                } else {
                    cols.add(ac.toString());
                }
            } else {
                throw new IllegalArgumentException("Unsupported projection: " + item.getClass().getSimpleName());
            }
        }
        return cols;
    }

    private static List<String> extractGroupBy(QuerySpecification spec, Optional<String> alias) {
        List<String> groupCols = new ArrayList<>();
        if (spec.getGroupBy().isPresent()) {
            for (GroupingElement ge : spec.getGroupBy().get().getGroupingElements()) {
                if (ge instanceof SimpleGroupBy) {
                    for (Expression expr : ((SimpleGroupBy) ge).getExpressions()) {
                        Expression norm = normalizeExpression(expr, alias);
                        groupCols.add(formatExpr(norm));
                    }
                } else {
                    // fall back to raw SQL
                    groupCols.add(ge.toString());
                }
            }
        }
        return groupCols;
    }

    private static List<String> extractAggregations(QuerySpecification spec, Optional<String> alias) {
        Set<String> aggs = new HashSet<>();
        for (SelectItem item : spec.getSelect().getSelectItems()) {
            if (item instanceof SingleColumn) {
                findAggsRecursive(((SingleColumn) item).getExpression(), alias, aggs);
            }
        }
        spec.getHaving().ifPresent(h -> findAggsRecursive(h, alias, aggs));
        return new ArrayList<>(aggs);
    }

    private static void findAggsRecursive(Expression expr, Optional<String> alias, Set<String> collector) {
        if (expr instanceof FunctionCall) {
            FunctionCall fc = (FunctionCall) expr;
            String name = fc.getName().toString().toUpperCase();
            Set<String> agNames = Set.of("SUM","COUNT","AVG","MIN","MAX");
            if (agNames.contains(name)) {
                List<Expression> args = new ArrayList<>();
                for (Expression a : fc.getArguments()) {
                    args.add(normalizeExpression(a, alias));
                }
                FunctionCall rewritten = new FunctionCall(
                        fc.getLocation(), fc.getName(), fc.getWindow(),
                        fc.getFilter().map(f -> normalizeExpression(f, alias)),
                        fc.getOrderBy(), fc.isDistinct(), fc.getNullTreatment(),
                        fc.getProcessingMode(), args);
                collector.add(formatExpr(rewritten));
                return;
            }
            // recurse
            for (Expression a : fc.getArguments()) {
                findAggsRecursive(a, alias, collector);
            }
            fc.getFilter().ifPresent(flt -> findAggsRecursive(flt, alias, collector));
        } else {
            for (Node child : expr.getChildren()) {
                if (child instanceof Expression) {
                    findAggsRecursive((Expression) child, alias, collector);
                }
            }
        }
    }

    private static List<String> extractFilterColumns(QuerySpecification spec, Optional<String> alias) {
        List<String> cols = new ArrayList<>();
        if (spec.getWhere().isPresent()) {
            Expression where = spec.getWhere().get();
            List<Expression> raws = new ArrayList<>();

            // Visitor that collects every Identifier and DereferenceExpression in the WHERE
            new DefaultTraversalVisitor<List<Expression>>() {
                @Override
                protected Void visitIdentifier(Identifier id, List<Expression> ctx) {
                    ctx.add(id);
                    return null;
                }
                @Override
                protected Void visitDereferenceExpression(DereferenceExpression d, List<Expression> ctx) {
                    ctx.add(d);
                    return null;
                }
            }.process(where, raws);

            // Normalize (strip alias if present) and format each collected expression
            for (Expression e : raws) {
                Expression norm = normalizeExpression(e, alias);
                cols.add(SqlFormatter.formatSql(norm));
            }
        }
        // Dedupe and return
        return cols.stream().distinct().collect(Collectors.toList());
    }


    private static Expression normalizeExpression(Expression expr, Optional<String> alias) {
        if (alias.isEmpty() || expr == null) {
            return expr;
        }
        return ExpressionTreeRewriter.rewriteWith(new AliasStripper(alias.get()), expr);
    }

    private static class AliasStripper extends ExpressionRewriter<Void> {
        private final String alias;
        AliasStripper(String alias) {
            this.alias = alias.toLowerCase();
        }
        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void ctx, ExpressionTreeRewriter<Void> rw) {
            if (node.getBase() instanceof Identifier && node.getField().isPresent()
                    && ((Identifier) node.getBase()).getValue().equalsIgnoreCase(alias)) {
                return node.getField().get();
            }
            return rw.defaultRewrite(node, ctx);
        }
    }

    private static class ColumnIdentifierCollector extends DefaultTraversalVisitor<List<Expression>> {
        private final Optional<String> alias;
        private ColumnIdentifierCollector(Optional<String> alias) {
            this.alias = alias.map(String::toLowerCase);
        }
        @Override
        protected Void visitIdentifier(Identifier id, List<Expression> ctx) {
            ctx.add(id);
            return null;
        }
        @Override
        protected Void visitDereferenceExpression(DereferenceExpression d, List<Expression> ctx) {
            ctx.add(d);
            return null;
        }
        @Override
        protected Void visitFunctionCall(FunctionCall f, List<Expression> ctx) {
            for (Expression a : f.getArguments()) {
                process(a, ctx);
            }
            f.getFilter().ifPresent(flt -> process(flt, ctx));
            return null;
        }
    }

    // Fixed SourceColumnCollectorVisitor class
    public static class SourceColumnCollectorVisitor extends DefaultTraversalVisitor<Set<String>> {
        private final String sourceNameToTrack;
        private boolean starFound = false;

        public SourceColumnCollectorVisitor(String sourceNameToTrack) {
            // normalize to lowercase for case‐insensitive matching
            this.sourceNameToTrack = sourceNameToTrack.toLowerCase();
        }

        /**
         * @return true if an unqualified "*" or "sourceName.*" was encountered
         */
        public boolean isStarFound() {
            return starFound;
        }

        @Override
        protected Void visitDereferenceExpression(
                DereferenceExpression node,
                Set<String> context) {
            // only capture columns qualified by our target CTE/table name
            if (node.getBase() instanceof Identifier
                    && node.getField().isPresent()) {
                String base = ((Identifier) node.getBase()).getValue().toLowerCase();
                if (base.equals(sourceNameToTrack)) {
                    context.add(node.getField().get().getValue().toLowerCase());
                }
            }
            // don't recurse further into this path
            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Set<String> context) {
            // capture both "*" and "sourceName.*"
            boolean appliesToSource = node.getTarget()
                    .map(qn -> qn.toString().equalsIgnoreCase(sourceNameToTrack))
                    .orElse(true);
            if (appliesToSource) {
                starFound = true;
            }
            return null;
        }

        @Override
        protected Void visitFunctionCall(
                FunctionCall node,
                Set<String> context) {
            // dive into arguments and any FILTER(...) clause
            for (Expression arg : node.getArguments()) {
                process(arg, context);
            }
            node.getFilter().ifPresent(f -> process(f, context));
            return null;
        }
    }
}