package com.poc.rewrite.rewriting;

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
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public class TableReplacerVisitor extends AstVisitor<Node, Void> {
    private final String originalTableStringToMatch;
    private final QualifiedName newTableQualifiedName;
    private boolean changed = false;

    public TableReplacerVisitor(String originalTableStringToMatch, QualifiedName newTableQualifiedName) {
        this.originalTableStringToMatch = Objects.requireNonNull(originalTableStringToMatch, "originalTableStringToMatch is null");
        this.newTableQualifiedName = Objects.requireNonNull(newTableQualifiedName, "newTableQualifiedName is null");
    }

    public boolean didChange() {
        return changed;
    }

    @Override
    protected Node visitQuery(Query node, Void context) {
        Optional<With> withNode = node.getWith();
        Optional<With> newWithNode = withNode; // Assume no change initially
        if (withNode.isPresent()) {
            With processedWith = (With) process(withNode.get(), context);
            if (processedWith != withNode.get()) { // Check if 'with' clause object itself changed
                newWithNode = Optional.of(processedWith);
            }
        }

        QueryBody queryBody = node.getQueryBody();
        QueryBody newQueryBody = (QueryBody) process(queryBody, context);
        
        if (newQueryBody != queryBody || newWithNode != withNode) {
            this.changed = true;
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
        if (node.getName().toString().equals(originalTableStringToMatch)) {
            this.changed = true;
            if (node.getLocation().isPresent()) {
                return new Table(node.getLocation().get(), newTableQualifiedName);
            } else {
                return new Table(newTableQualifiedName);
            }
        }
        return node;
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, Void context) {
        Relation relation = node.getRelation();
        Relation newRelation = (Relation) process(relation, context);
        if (newRelation != relation) {
            this.changed = true;
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
        }
        return node;
    }

    @Override
    protected Node visitJoin(Join node, Void context) {
        Relation left = (Relation) process(node.getLeft(), context);
        Relation right = (Relation) process(node.getRight(), context);
        if (left != node.getLeft() || right != node.getRight()) {
            this.changed = true;
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
    protected Node visitNode(Node node, Void context) {
        return super.visitNode(node, context);
    }
}