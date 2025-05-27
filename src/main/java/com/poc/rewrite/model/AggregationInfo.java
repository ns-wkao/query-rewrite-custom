package com.poc.rewrite.model;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AggregationInfo {
    private final String functionName;
    private final List<String> arguments;
    private final boolean distinct;

    /**
     * Normalizes an argument string by removing quotes and qualifiers.
     * Example: "schema"."column" -> column
     * alert_event.count -> count
     * "count" -> count
     */
    private String normalizeArgument(String arg) {
        String noQuotes = arg.replace("\"", "");
        int dot = noQuotes.lastIndexOf('.');
        // Take part after last dot, or the whole thing if no dot.
        String unqualified = (dot >= 0 ? noQuotes.substring(dot + 1) : noQuotes);
        return unqualified.toLowerCase();
    }

    public AggregationInfo(String functionName, List<String> arguments, boolean distinct) {
        this.functionName = functionName.toLowerCase();
        // Apply normalization during construction
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