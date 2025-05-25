package com.poc.rewrite;

import com.poc.rewrite.config.MaterializedViewMetadata;

// We need these for FunctionCall, even if not directly processing them now
import io.trino.sql.tree.*;
import io.trino.sql.parser.ParsingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Objects;
import java.util.stream.Collectors;

import io.trino.sql.tree.FunctionCall.NullTreatment;


public class QueryMetadataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetadataExtractor.class);

    // Helper class to store relation info (base table name and its primary alias)
    private static class RelationInfo {
        final String baseTableName;
        final Optional<String> alias; // Alias of the base table itself

        RelationInfo(String baseTableName, Optional<String> alias) {
            this.baseTableName = baseTableName;
            this.alias = alias;
        }
    }

    /**
     * Extracts metadata from a Statement, expecting it to be a Query.
     * It now identifies the base table, its alias (if any), and performs
     * basic normalization on projections, group by clauses, and aggregations
     * based on this alias.
     */
    public static MaterializedViewMetadata extractMetadataFromQuery(Statement queryStatement) {
        if (!(queryStatement instanceof Query)) {
            throw new IllegalArgumentException("Expected a Query statement but got: " + queryStatement.getClass().getSimpleName());
        }

        MaterializedViewMetadata metadata = new MaterializedViewMetadata();
        Query query = (Query) queryStatement;
        QuerySpecification querySpec = extractQuerySpecification(query); // Assumes simple Query

        if (querySpec != null) {
            RelationInfo relationInfo = extractRelationInfo(querySpec);
            metadata.setBaseTable(relationInfo.baseTableName);
            metadata.setTableAlias(relationInfo.alias); // Store the identified alias

            // Pass the identified alias for normalization purposes
            metadata.setProjectionColumns(extractProjections(querySpec, relationInfo.alias));
            metadata.setGroupByColumns(extractGroupBy(querySpec, relationInfo.alias));
            metadata.setAggregations(extractAggregations(querySpec, relationInfo.alias));
            metadata.setFilterColumns(extractFilterColumns(querySpec, relationInfo.alias));

        }

        logger.info("Extracted metadata: baseTable={}, alias={}, projections={}, groupBy={}, aggregations={}, filters={}", // Added filters
                metadata.getBaseTable(),
                metadata.getTableAlias().orElse("N/A"),
                metadata.getProjectionColumns(),
                metadata.getGroupByColumns(),
                metadata.getAggregations(), // Now a list
                metadata.getFilterColumns()); // Added filters
        return metadata;
    }

    private static QuerySpecification extractQuerySpecification(Query query) {
        if (query.getQueryBody() instanceof QuerySpecification) {
            return (QuerySpecification) query.getQueryBody();
        }
        throw new IllegalArgumentException("Unsupported query structure. Only simple SELECT queries with a direct QuerySpecification body are supported.");
    }

    /**
     * Extracts RelationInfo (base table name and its alias) from the FROM clause.
     */
    private static RelationInfo extractRelationInfo(QuerySpecification querySpec) {
        Optional<Relation> fromRelationOptional = querySpec.getFrom();
        if (fromRelationOptional.isPresent()) {
            return findRelationInfoRecursive(fromRelationOptional.get());
        }
        throw new IllegalArgumentException("Query specification must have a FROM clause.");
    }

    /**
     * Recursively navigates a Relation node to find the ultimate base Table and its direct alias.
     * Now supports TableSubquery by recursively processing nested queries.
     */
    private static RelationInfo findRelationInfoRecursive(Relation relationNode) {
        if (relationNode instanceof Table) {
            return new RelationInfo(((Table) relationNode).getName().toString(), Optional.empty());
        } else if (relationNode instanceof AliasedRelation) {
            AliasedRelation aliasedRelation = (AliasedRelation) relationNode;
            String aliasName = aliasedRelation.getAlias().getValue();
            logger.debug("Found AliasedRelation with alias: {}. Processing its underlying relation.", aliasName);
            RelationInfo underlyingInfo = findRelationInfoRecursive(aliasedRelation.getRelation());
            return new RelationInfo(underlyingInfo.baseTableName, Optional.of(aliasName));
        } else if (relationNode instanceof TableSubquery) {
            TableSubquery tableSubquery = (TableSubquery) relationNode;
            logger.debug("Found TableSubquery. Processing nested query to find base table.");
            
            // Extract the nested query and process it recursively
            Query nestedQuery = tableSubquery.getQuery();
            QuerySpecification nestedQuerySpec = extractQuerySpecification(nestedQuery);
            
            if (nestedQuerySpec != null && nestedQuerySpec.getFrom().isPresent()) {
                return findRelationInfoRecursive(nestedQuerySpec.getFrom().get());
            } else {
                logger.warn("TableSubquery does not contain a valid FROM clause or QuerySpecification.");
                throw new IllegalArgumentException("TableSubquery must contain a valid FROM clause for base table extraction.");
            }
        } else if (relationNode instanceof Join) {
            logger.warn("Encountered Join in FROM clause. Base table identification from JOINs is not supported in this POC version.");
            throw new IllegalArgumentException("Joins in FROM clause are not supported for base table extraction.");
        }
        
        logger.warn("Encountered an unsupported relation type in FROM clause: {}", relationNode.getClass().getSimpleName());
        throw new IllegalArgumentException("Unsupported relation type in FROM clause for base table extraction: " + relationNode.getClass().getSimpleName());
    }

    // --- Expression Normalization Logic ---

    /**
     * Normalizes an expression by stripping a known table alias from column references
     * (e.g., "alias.column" -> "column") and from arguments within function calls.
     *
     * @param expression The expression to normalize.
     * @param tableAlias The Optional table alias to strip.
     * @return The normalized Expression node.
     */
    private static Expression normalizeExpression(Expression expression, Optional<String> tableAlias) {
        if (!tableAlias.isPresent() || expression == null) {
            return expression;
        }
        // Use the new ExpressionRewriter based visitor
        return ExpressionTreeRewriter.rewriteWith(
                new AliasStrippingExpressionVisitor(tableAlias.get()),
                expression);
    }

    private static class AliasStrippingExpressionVisitor extends ExpressionRewriter<Void> {
        private final String aliasToStrip;

        public AliasStrippingExpressionVisitor(String aliasToStrip) {
            this.aliasToStrip = Objects.requireNonNull(aliasToStrip, "aliasToStrip is null").toLowerCase();
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            // Check if the base is an Identifier and matches our alias
            if (node.getBase() instanceof Identifier) {
                Identifier baseIdentifier = (Identifier) node.getBase();
                if (node.getField().isPresent() && baseIdentifier.getValue().toLowerCase().equals(this.aliasToStrip)) {
                    // If it matches, just return the field (which is an Identifier)
                    // We don't need to rewrite it further.
                    return node.getField().get();
                }
            }
            // IMPORTANT: If it doesn't match, or base isn't Identifier,
            // we must still process its children (especially the base)
            // using the treeRewriter to handle nested cases.
            return treeRewriter.defaultRewrite(node, context);
        }

        // We can override other expression types if needed, but ExpressionRewriter
        // handles traversal fairly well.
    }

    /**
     * Visitor to collect Identifiers and DereferenceExpressions using DefaultTraversalVisitor
     * to ensure we visit all nodes in the WHERE clause.
     */
    private static class ColumnIdentifierCollectorVisitor extends DefaultTraversalVisitor<List<Expression>> {

        @Override
        protected Void visitIdentifier(Identifier node, List<Expression> context) {
            // Add the identifier found.
            // We might add function names too, but normalization might help later.
            // For now, let's add it.
            logger.trace("Filter Visitor: Found Identifier: {}", node.getValue());
            context.add(node);
            return null; // DefaultTraversalVisitor handles children, but Identifier has none.
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, List<Expression> context) {
            // We want the *whole* expression (e.g., alias.column)
            // We don't want to recurse and get 'alias' and 'column' separately.
            logger.trace("Filter Visitor: Found DereferenceExpression: {}", node);
            context.add(node);
            return null; // IMPORTANT: Stop traversal here, don't visit children.
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, List<Expression> context) {
            // We ONLY want to visit the *arguments* of a function, not its name.
            logger.trace("Filter Visitor: Visiting FunctionCall args: {}", node.getName());
            for(Expression arg : node.getArguments()) {
                process(arg, context);
            }
            node.getFilter().ifPresent(f -> process(f, context));
            return null; // Stop traversal here (don't process the function name itself).
        }

        // We don't override other methods, so DefaultTraversalVisitor will
        // automatically continue visiting children for nodes like
        // ComparisonExpression, LogicalBinaryExpression, etc., eventually
        // leading us to Identifiers or DereferenceExpressions.
    }
    

    private static List<String> extractProjections(QuerySpecification querySpec, Optional<String> tableAlias) {
        List<String> projections = new ArrayList<>();
        for (SelectItem item : querySpec.getSelect().getSelectItems()) {
            if (item instanceof SingleColumn) {
                SingleColumn singleColumn = (SingleColumn) item;
                Expression originalExpression = singleColumn.getExpression();
                Expression normalizedExpression = normalizeExpression(originalExpression, tableAlias);
                Optional<Identifier> columnAlias = singleColumn.getAlias();
                projections.add(normalizedExpression.toString());
            } else if (item instanceof AllColumns) {
                AllColumns allColumnsNode = (AllColumns) item;
                if (allColumnsNode.getTarget().isPresent() && tableAlias.isPresent()) {
                    if (allColumnsNode.getTarget().get().toString().equalsIgnoreCase(tableAlias.get())) {
                        projections.add("*");
                    } else {
                        projections.add(allColumnsNode.toString());
                    }
                } else if (!allColumnsNode.getTarget().isPresent()) {
                    projections.add("*");
                } else {
                    projections.add(allColumnsNode.toString());
                }
            } else {
                logger.error("Unsupported projection type: {}", item.getClass().getSimpleName());
                throw new IllegalArgumentException("Unsupported projection type: " + item.getClass().getSimpleName());
            }
        }
        return projections;
    }

    private static List<String> extractGroupBy(QuerySpecification querySpec, Optional<String> tableAlias) {
        List<String> groupByColumns = new ArrayList<>();
        Optional<GroupBy> groupByOptional = querySpec.getGroupBy();
        if (groupByOptional.isPresent()) {
            GroupBy groupBy = groupByOptional.get();
            for (GroupingElement groupingElement : groupBy.getGroupingElements()) {
                if (groupingElement instanceof SimpleGroupBy) {
                    for(Expression expr : ((SimpleGroupBy) groupingElement).getExpressions()){
                        groupByColumns.add(normalizeExpression(expr, tableAlias).toString());
                    }
                } else {
                    logger.warn("Encountered complex GroupingElement type: {}. Using its string representation (unnormalized).", groupingElement.getClass().getSimpleName());
                    groupByColumns.add(groupingElement.toString());
                }
            }
        }
        return groupByColumns;
    }

    private static List<String> extractAggregations(QuerySpecification querySpec, Optional<String> tableAlias) {
        List<String> aggregations = new ArrayList<>(); // Use List instead of Map
        AliasStrippingExpressionVisitor visitor = tableAlias.map(AliasStrippingExpressionVisitor::new).orElse(null);

        for (SelectItem item : querySpec.getSelect().getSelectItems()) {
            if (item instanceof SingleColumn) {
                Expression expression = ((SingleColumn) item).getExpression();
                if (expression instanceof FunctionCall) {
                    FunctionCall originalFunctionCall = (FunctionCall) expression;

                    List<Expression> normalizedArgs = originalFunctionCall.getArguments().stream()
                        .map(arg -> normalizeExpression(arg, tableAlias)) // Use normalizeExpression!
                        .collect(Collectors.toList());

                    Optional<Expression> newFilter = originalFunctionCall.getFilter()
                            .map(f -> normalizeExpression(f, tableAlias)); // Use normalizeExpression!

                    Optional<Window> newWindow = originalFunctionCall.getWindow();
                    Optional<OrderBy> newOrderBy = originalFunctionCall.getOrderBy();

                    boolean changed = !Objects.equals(originalFunctionCall.getArguments(), normalizedArgs) ||
                                      !Objects.equals(originalFunctionCall.getFilter(), newFilter);

                    FunctionCall processedFunctionCall = originalFunctionCall;
                    if (changed) {
                        processedFunctionCall = new FunctionCall(
                                originalFunctionCall.getLocation(),
                                originalFunctionCall.getName(),
                                newWindow,
                                newFilter,
                                newOrderBy,
                                originalFunctionCall.isDistinct(),
                                originalFunctionCall.getNullTreatment(),
                                originalFunctionCall.getProcessingMode(),
                                normalizedArgs
                        );
                    }
                    // Add the (potentially normalized) function call string to the List
                    aggregations.add(processedFunctionCall.toString());
                }
            }
        }
        return aggregations; // Return the List
    }
    /**
     * Extracts columns used in the WHERE clause and normalizes them.
     */
    private static List<String> extractFilterColumns(QuerySpecification querySpec, Optional<String> tableAlias) {
        if (!querySpec.getWhere().isPresent()) {
            return new ArrayList<>(); // No WHERE clause, no filter columns.
        }

        Expression whereClause = querySpec.getWhere().get();
        List<Expression> foundExpressions = new ArrayList<>();
        new ColumnIdentifierCollectorVisitor().process(whereClause, foundExpressions);

        logger.debug("Found raw filter expressions: {}", foundExpressions);

        // Normalize and collect unique column names
        Set<String> filterColumns = foundExpressions.stream()
                .map(expr -> normalizeExpression(expr, tableAlias)) // Use existing normalizer
                .map(Expression::toString)
                .collect(Collectors.toSet()); // Use Set to get distinct columns

        logger.debug("Extracted normalized filter columns: {}", filterColumns);
        return new ArrayList<>(filterColumns);
    }
}