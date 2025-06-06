package com.ns.rewrite.model;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AggregationInfo {
    private final String functionName;
    private final List<String> arguments;
    private final boolean distinct;

    /**
     * Normalizes an argument string by removing qualifiers.
     * "count" -> count
     */
    private String normalizeArgument(String arg) {
        //String noQuotes = arg.replace("\"", "");
        //return noQuotes.toLowerCase();
        return arg.toLowerCase();
    }

    public AggregationInfo(String functionName, List<String> arguments, boolean distinct) {
        this.functionName = functionName.toLowerCase();
        this.arguments = arguments.stream()
                                  .map(this::normalizeArgument)
                                  .collect(Collectors.toList());
        this.distinct = distinct;
    }

    public String getFunctionName() { return functionName; }
    public List<String> getArguments() { return arguments; }
    public boolean isDistinct() { return distinct; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregationInfo that = (AggregationInfo) o;
        return distinct == that.distinct &&
               functionName.equals(that.functionName) &&
               arguments.equals(that.arguments); // Now compares normalized lists
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName, arguments, distinct);
    }

    @Override
    public String toString() {
        // Internal state is actually normalized
        return functionName.toUpperCase() + "(" +
               (distinct ? "DISTINCT " : "") +
               String.join(", ", arguments) + ")";
    }
}