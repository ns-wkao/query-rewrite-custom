package com.ns.rewrite.rewriting;

import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryBody;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public class TableReplacerVisitor extends AstVisitor<Node, Void> {
    private static final Logger logger = LoggerFactory.getLogger(TableReplacerVisitor.class);
    
    private final String originalTableStringToMatch;
    private final QualifiedName newTableQualifiedName;
    private boolean changed = false;
    private int replacementCount = 0;

    public TableReplacerVisitor(String originalTableStringToMatch, QualifiedName newTableQualifiedName) {
        this.originalTableStringToMatch = Objects.requireNonNull(originalTableStringToMatch, "originalTableStringToMatch is null");
        this.newTableQualifiedName = Objects.requireNonNull(newTableQualifiedName, "newTableQualifiedName is null");
        
        logger.info("Starting AST transformation: '{}' -> '{}'", originalTableStringToMatch, newTableQualifiedName);
    }

    public boolean didChange() {
        if (changed) {
            logger.info("AST transformation completed successfully. Replaced {} table references.", replacementCount);
        } else {
            logger.debug("AST transformation completed with no changes - no matching table references found.");
        }
        return changed;
    }
    
    public int getReplacementCount() {
        return replacementCount;
    }

    @Override
    protected Node visitQuery(Query node, Void context) {
        logger.debug("Processing Query node");
        
        Optional<With> withNode = node.getWith();
        Optional<With> newWithNode = withNode; // Assume no change initially
        if (withNode.isPresent()) {
            logger.debug("Processing WITH clause containing {} CTEs", withNode.get().getQueries().size());
            With processedWith = (With) process(withNode.get(), context);
            if (processedWith != withNode.get()) { // Check if 'with' clause object itself changed
                newWithNode = Optional.of(processedWith);
                logger.debug("WITH clause was modified during transformation");
            }
        }

        QueryBody queryBody = node.getQueryBody();
        QueryBody newQueryBody = (QueryBody) process(queryBody, context);
        
        if (newQueryBody != queryBody || newWithNode != withNode) {
            this.changed = true;
            logger.debug("Query node was modified, reconstructing AST");
            
            try {
                NodeLocation location = node.getLocation().orElseThrow(() ->
                        new IllegalStateException("Original Query node is missing NodeLocation, " +
                                                  "cannot use non-deprecated constructor without a location.")
                );

                return new Query(
                        location,
                        node.getSessionProperties(),
                        node.getFunctions(),
                        newWithNode,
                        newQueryBody,
                        node.getOrderBy(),
                        node.getOffset(),
                        node.getLimit()
                );
            } catch (Exception e) {
                logger.error("Failed to reconstruct Query AST node: {}", e.getMessage(), e);
                throw e;
            }
        }
        return node;
    }

    @Override
    protected Node visitWith(With node, Void context) {
        boolean queriesChanged = false;
        List<WithQuery> newWithQueries = new ArrayList<>();

        for (WithQuery originalWithQuery : node.getQueries()) {
            WithQuery processedWithQuery = (WithQuery) process(originalWithQuery, context);
            if (processedWithQuery != originalWithQuery) {
                queriesChanged = true;
            }
            newWithQueries.add(processedWithQuery);
        }

        if (queriesChanged) {
            this.changed = true;
            if (node.getLocation().isPresent()) {
                return new With(node.getLocation().get(), node.isRecursive(), newWithQueries);
            } else {
                return new With(node.isRecursive(), newWithQueries);
            }
        }
        return node;
    }

    @Override
    protected Node visitWithQuery(WithQuery node, Void context) {
        Query originalInnerQuery = node.getQuery();
        Query newInnerQuery = (Query) process(originalInnerQuery, context);

        if (newInnerQuery != originalInnerQuery) {
            this.changed = true;
            if (node.getLocation().isPresent()) {
                return new WithQuery(
                        node.getLocation().get(),
                        node.getName(),           // Original CTE name
                        newInnerQuery,            // Potentially modified inner query defining the CTE
                        node.getColumnNames()     // Original column aliases for the CTE (e.g., CTE_NAME(col1, col2))
                );
            } else {
                return new WithQuery(
                        node.getName(),
                        newInnerQuery,
                        node.getColumnNames()
                );
            }
        }
        return node;
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, Void context) {
        Optional<Relation> from = node.getFrom();
        Optional<Relation> newFrom = from; 

        if (from.isPresent()) {
            Relation processedFrom = (Relation) process(from.get(), context);
            if (processedFrom != from.get()) { 
                newFrom = Optional.of(processedFrom);
            }
        }

        if (newFrom != from) { 
            this.changed = true;
            if (node.getLocation().isPresent()) {
                return new QuerySpecification(
                        node.getLocation().get(), 
                        node.getSelect(),
                        newFrom,
                        node.getWhere(),
                        node.getGroupBy(),
                        node.getHaving(),
                        node.getWindows(),
                        node.getOrderBy(),
                        node.getOffset(),
                        node.getLimit()
                );
            } else {
                return new QuerySpecification(
                        node.getSelect(),
                        newFrom,
                        node.getWhere(),
                        node.getGroupBy(),
                        node.getHaving(),
                        node.getWindows(),
                        node.getOrderBy(),
                        node.getOffset(),
                        node.getLimit()
                );
            }
        }
        return node;
    }
    
    @Override
    protected Node visitTable(Table node, Void context) {
        String tableName = node.getName().toString();
        logger.debug("Examining table reference: '{}'", tableName);
        
        if (tableName.equals(originalTableStringToMatch)) {
            this.changed = true;
            this.replacementCount++;
            logger.debug("Found matching table reference '{}' - replacing with '{}'", tableName, newTableQualifiedName);
            
            try {
                if (node.getLocation().isPresent()) {
                    return new Table(node.getLocation().get(), newTableQualifiedName);
                } else {
                    return new Table(newTableQualifiedName);
                }
            } catch (Exception e) {
                logger.error("Failed to replace table '{}' with '{}': {}", tableName, newTableQualifiedName, e.getMessage(), e);
                throw e;
            }
        }
        return node;
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, Void context) {
        logger.debug("Processing aliased relation with alias: '{}'", node.getAlias().getValue());
        
        Relation relation = node.getRelation();
        Relation newRelation = (Relation) process(relation, context);
        
        // Check if the relation was actually modified and is not null
        if (newRelation != relation && newRelation != null) {
            this.changed = true;
            logger.debug("Aliased relation '{}' was modified during transformation", node.getAlias().getValue());
            
            try {
                if (node.getLocation().isPresent()) {
                    return new AliasedRelation(
                            node.getLocation().get(), 
                            newRelation, 
                            node.getAlias(), 
                            node.getColumnNames()
                    );
                } else {
                    return new AliasedRelation(
                            newRelation, 
                            node.getAlias(), 
                            node.getColumnNames()
                    );
                }
            } catch (Exception e) {
                logger.error("Failed to reconstruct aliased relation '{}': {}", node.getAlias().getValue(), e.getMessage(), e);
                throw e;
            }
        } else if (newRelation == null) {
            logger.debug("Aliased relation '{}' processing returned null - keeping original", node.getAlias().getValue());
        }
        return node;
    }

    @Override
    protected Node visitJoin(Join node, Void context) {
        logger.debug("Processing {} join", node.getType());
        
        Relation left = (Relation) process(node.getLeft(), context);
        Relation right = (Relation) process(node.getRight(), context);
        if (left != node.getLeft() || right != node.getRight()) {
            this.changed = true;
            logger.debug("Join relations were modified during transformation");
            
            try {
                if (node.getLocation().isPresent()) {
                    return new Join(
                            node.getLocation().get(), 
                            node.getType(), 
                            left, 
                            right, 
                            node.getCriteria()
                    );
                } else {
                    return new Join(
                            node.getType(), 
                            left, 
                            right, 
                            node.getCriteria()
                    );
                }
            } catch (Exception e) {
                logger.error("Failed to reconstruct {} join: {}", node.getType(), e.getMessage(), e);
                throw e;
            }
        }
        return node;
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, Void context) {
        Query query = node.getQuery();
        Query newQuery = (Query) process(query, context); // Process the inner query
        if (newQuery != query) {
            this.changed = true;
            // Reconstruct TableSubquery, preserving location
            if (node.getLocation().isPresent()) {
                return new TableSubquery(node.getLocation().get(), newQuery);
            } else {
                return new TableSubquery(newQuery);
            }
        }
        return node;
    }
    
    @Override
    protected Node visitUnnest(Unnest node, Void context) {
        // UNNEST is a table-valued function, not a table reference
        // We never want to replace UNNEST functions, so just return the node unchanged
        logger.debug("Processing UNNEST function - keeping unchanged");
        return node;
    }
    
    @Override
    protected Node visitNode(Node node, Void context) {
        return super.visitNode(node, context);
    }
}