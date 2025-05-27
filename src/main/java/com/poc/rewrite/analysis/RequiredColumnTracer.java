package com.poc.rewrite.analysis;

import io.trino.sql.tree.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Traces required columns through a query, including CTEs,
 * to determine the specific columns needed from base tables.
 * It handles '*' as a pass-through and assumes MVs/base tables
 * will eventually be queried with explicit column names.
 */
public class RequiredColumnTracer {

    private static final Logger logger = LoggerFactory.getLogger(RequiredColumnTracer.class);
    private static final String STAR = "*"; // Represents needing all columns

    private final Map<String, WithQuery> cteMap = new HashMap<>();
    private final Map<String, Set<String>> requiredBaseColumns = new HashMap<>();
    private final Set<String> baseTableNames;

    /**
     * Constructor.
     * @param query The user query to trace.
     * @param knownBaseTables A set of known base table names (fully qualified).
     */
    public RequiredColumnTracer(Query query, Set<String> knownBaseTables) {
        this.baseTableNames = Objects.requireNonNull(knownBaseTables, "knownBaseTables cannot be null")
                                     .stream()
                                     .map(String::toLowerCase)
                                     .collect(Collectors.toSet());

        query.getWith().ifPresent(with -> {
            for (WithQuery wq : with.getQueries()) {
                cteMap.put(wq.getName().getValue().toLowerCase(), wq);
            }
        });

        traceQueryBody(query.getQueryBody(), Set.of(STAR), new HashMap<>());
    }

    /**
     * Returns the map of base tables to the set of columns required from them.
     */
    public Map<String, Set<String>> getRequiredBaseColumns() {
        return requiredBaseColumns.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty() && !entry.getValue().contains(STAR))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Main recursive tracing function for QueryBody.
     */
    private void traceQueryBody(QueryBody queryBody, Set<String> neededColumns, Map<String, String> currentAliases) {
        if (queryBody instanceof QuerySpecification) {
            traceQuerySpecification((QuerySpecification) queryBody, neededColumns, currentAliases);
        } else if (queryBody instanceof Table) {
             traceTable((Table) queryBody, neededColumns, currentAliases);
        } else if (queryBody instanceof TableSubquery) {
             traceQueryBody(((TableSubquery) queryBody).getQuery().getQueryBody(), neededColumns, currentAliases);
        } else if (queryBody instanceof SetOperation) {
            logger.warn("SetOperation found - tracing requirements into all branches (POC simplification).");
            ((SetOperation) queryBody).getRelations().forEach(rel -> {
                 // Simplified: Assume relations in SetOp are QueryBody-like
                 if (rel instanceof QueryBody) {
                    traceQueryBody((QueryBody)rel, neededColumns, currentAliases);
                 } else if (rel instanceof TableSubquery) {
                    traceQueryBody(((TableSubquery)rel).getQuery().getQueryBody(), neededColumns, currentAliases);
                 } else {
                     logger.error("Unsupported relation type in SetOperation: {}", rel.getClass());
                     throw new UnsupportedOperationException("SetOperation tracing for " + rel.getClass() + " not implemented.");
                 }
            });
        } else {
            throw new IllegalArgumentException("Unsupported QueryBody type: " + queryBody.getClass());
        }
    }


    /**
     * Traces requirements through a QuerySpecification (SELECT-FROM-WHERE).
     */
    private void traceQuerySpecification(QuerySpecification spec, Set<String> neededColumns, Map<String, String> currentAliases) {
        logger.debug("Tracing QSpec. Needed: {}", neededColumns);

        Relation fromRelation = spec.getFrom()
                .orElseThrow(() -> new IllegalArgumentException("QuerySpecification must have a FROM clause for tracing."));

        // Changed to Map<String, SelectItem>
        Map<String, SelectItem> projectionMap = buildProjectionMap(spec.getSelect());
        Set<String> nextNeededColumns = new HashSet<>();

        if (neededColumns.contains(STAR)) {
            projectionMap.forEach((name, item) -> {
                 if (item instanceof AllColumns) {
                    nextNeededColumns.add(STAR);
                 } else if (item instanceof SingleColumn) {
                    nextNeededColumns.addAll(findIdentifiers(((SingleColumn) item).getExpression()));
                 }
            });
        } else {
            neededColumns.forEach(neededCol -> {
                SelectItem item = projectionMap.get(neededCol.toLowerCase());
                if (item != null) {
                    if (item instanceof AllColumns) {
                         nextNeededColumns.add(STAR);
                    } else if (item instanceof SingleColumn){
                        nextNeededColumns.addAll(findIdentifiers(((SingleColumn) item).getExpression()));
                    }
                } else {
                    nextNeededColumns.add(neededCol);
                    //logger.warn("Needed column '{}' not found in projections, assuming pass-through.", neededCol);
                }
            });
        }

        spec.getWhere().ifPresent(where -> nextNeededColumns.addAll(findIdentifiers(where)));
        spec.getHaving().ifPresent(having -> nextNeededColumns.addAll(findIdentifiers(having)));

        logger.debug("Next needed: {}", nextNeededColumns);
        traceRelation(fromRelation, nextNeededColumns, currentAliases);
    }

    /**
     * Traces requirements through a Relation (Table, Subquery, Join, CTE).
     */
     private void traceRelation(Relation relation, Set<String> neededColumns, Map<String, String> currentAliases) {
        Map<String, String> newAliases = new HashMap<>(currentAliases);

        if (relation instanceof Table) {
            traceTable((Table) relation, neededColumns, newAliases);
        } else if (relation instanceof AliasedRelation) {
            AliasedRelation ar = (AliasedRelation) relation;
            // String alias = ar.getAlias().getValue().toLowerCase(); // We might need this later
            traceRelation(ar.getRelation(), neededColumns, newAliases);
        } else if (relation instanceof TableSubquery) {
            traceQueryBody(((TableSubquery) relation).getQuery().getQueryBody(), neededColumns, newAliases);
        } else if (relation instanceof Join) {
            logger.warn("JOIN found - tracing needed columns into both sides. This might be inaccurate.");
            traceRelation(((Join) relation).getLeft(), neededColumns, newAliases);
            traceRelation(((Join) relation).getRight(), neededColumns, newAliases);
        } else {
            throw new IllegalArgumentException("Unsupported Relation type: " + relation.getClass());
        }
    }


    /**
     * Traces requirements to a specific Table (Base or CTE).
     */
    private void traceTable(Table table, Set<String> neededColumns, Map<String, String> currentAliases) {
        String tableName = table.getName().toString().toLowerCase();
        logger.info("baseTableNames: {}", baseTableNames);
        logger.info("tableName: {}", tableName);

        if (cteMap.containsKey(tableName)) {
            logger.debug("Tracing into CTE '{}', needed: {}", tableName, neededColumns);
            WithQuery cte = cteMap.get(tableName);
            traceQueryBody(cte.getQuery().getQueryBody(), neededColumns, new HashMap<>());
        } else if (baseTableNames.contains(tableName)) {
            logger.debug("Reached base table '{}', needed: {}", tableName, neededColumns);
            if (neededColumns.contains(STAR)) {
                throw new UnsupportedOperationException("Cannot resolve query: Requires '*' from base table '" + tableName + "'.");
            }
            requiredBaseColumns.computeIfAbsent(table.getName().toString(), k -> new HashSet<>())
                    .addAll(neededColumns);
        } else {
            throw new IllegalArgumentException("Unknown table reference: '" + tableName + "'. Is it a base table or a CTE?");
        }
    }


    /**
     * Builds a map from output column name (lowercase) to the SelectItem that produces it.
     * Changed to return Map<String, SelectItem>.
     */
    private Map<String, SelectItem> buildProjectionMap(Select select) {
        Map<String, SelectItem> map = new HashMap<>();
        for (SelectItem item : select.getSelectItems()) {
            if (item instanceof SingleColumn) {
                SingleColumn sc = (SingleColumn) item;
                String name;
                if (sc.getAlias().isPresent()) {
                    name = sc.getAlias().get().getValue();
                } else if (sc.getExpression() instanceof Identifier) {
                    name = ((Identifier) sc.getExpression()).getValue();
                } else if (sc.getExpression() instanceof DereferenceExpression) {
                    name = ((DereferenceExpression) sc.getExpression()).getField()
                            .map(Identifier::getValue)
                            .orElse(sc.getExpression().toString());
                } else {
                   name = sc.getExpression().toString();
                   logger.warn("Projection '{}' has no clear alias, using full expression as key.", name);
                }
                map.put(name.toLowerCase(), sc); // Store the whole SingleColumn
            } else if (item instanceof AllColumns) {
                map.put(STAR, item); // Store the AllColumns item
            }
        }
        return map;
    }

    /**
     * Finds all identifiers (potential column names) within an Expression.
     */
    private Set<String> findIdentifiers(Expression expression) {
        Set<String> identifiers = new HashSet<>();
        new DefaultTraversalVisitor<Void>() {
            @Override
            protected Void visitIdentifier(Identifier node, Void context) {
                identifiers.add(node.getValue().toLowerCase());
                return null;
            }

            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, Void context) {
                node.getField().ifPresent(field -> identifiers.add(field.getValue().toLowerCase()));
                // If base is not an identifier, recurse to find identifiers within it.
                if (!(node.getBase() instanceof Identifier)) {
                    process(node.getBase(), context);
                }
                return null;
            }

            // Recurse into functions
            @Override
            protected Void visitFunctionCall(FunctionCall node, Void context) {
                 node.getArguments().forEach(arg -> process(arg, context));
                 return null;
            }

        }.process(expression, null);
        return identifiers;
    }
}