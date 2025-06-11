package com.ns.rewrite.analysis;

import io.trino.sql.tree.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;

/**
 * Analyzes temporal granularity in SQL expressions, particularly date_trunc function calls.
 * Based on Trino SQL official documentation for date_trunc units.
 */
public class TemporalGranularityAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(TemporalGranularityAnalyzer.class);
    
    /**
     * Temporal granularity levels based on Trino SQL date_trunc function units.
     * Ordered from finest to coarsest granularity.
     */
    public enum TimeGranularity {
        MILLISECOND(1L, "millisecond"),
        SECOND(1_000L, "second"),
        MINUTE(60_000L, "minute"),
        HOUR(3_600_000L, "hour"),
        DAY(86_400_000L, "day"),
        WEEK(604_800_000L, "week"),
        MONTH(2_592_000_000L, "month"), // Approximate: 30 days
        QUARTER(7_776_000_000L, "quarter"), // Approximate: 90 days
        YEAR(31_536_000_000L, "year"), // Approximate: 365 days
        UNKNOWN(Long.MAX_VALUE, "unknown");
        
        private final long approximateMilliseconds;
        private final String unitName;
        
        TimeGranularity(long approximateMilliseconds, String unitName) {
            this.approximateMilliseconds = approximateMilliseconds;
            this.unitName = unitName;
        }
        
        /**
         * Returns true if this granularity is finer (more precise) than the other.
         * For example, HOUR.isFinnerThan(DAY) returns true.
         */
        public boolean isFinnerThan(TimeGranularity other) {
            return this.approximateMilliseconds < other.approximateMilliseconds;
        }
        
        /**
         * Returns true if this granularity is coarser (less precise) than the other.
         * For example, DAY.isCoarserThan(HOUR) returns true.
         */
        public boolean isCoarserThan(TimeGranularity other) {
            return this.approximateMilliseconds > other.approximateMilliseconds;
        }
        
        /**
         * Returns true if this MV granularity can satisfy the given user query granularity.
         * An MV can satisfy a user query if the MV granularity divides evenly into the user granularity.
         * For example, an MV with HOUR granularity can satisfy a user query requiring DAY granularity (24 hours),
         * but an MV with 45-MINUTE granularity cannot satisfy a 30-MINUTE query (no clean division).
         */
        public boolean canSatisfy(TimeGranularity userRequirement) {
            if (userRequirement == UNKNOWN || this == UNKNOWN) {
                return true; // Unknown granularities are treated as compatible
            }
            
            // Same granularity always works
            if (this == userRequirement) {
                return true;
            }
            
            // MV must be finer (smaller period) than user requirement
            if (this.approximateMilliseconds > userRequirement.approximateMilliseconds) {
                return false;
            }
            
            // Check for special incompatible combinations
            if (hasIncompatibleCalendarBoundaries(this, userRequirement)) {
                return false;
            }
            
            // Check if MV granularity divides evenly into user granularity
            long mvPeriod = this.approximateMilliseconds;
            long userPeriod = userRequirement.approximateMilliseconds;
            
            // For very large periods (month, quarter, year), use approximation with tolerance
            if (mvPeriod >= MONTH.approximateMilliseconds || userPeriod >= MONTH.approximateMilliseconds) {
                return isApproximatelyDivisible(mvPeriod, userPeriod);
            }
            
            // For smaller periods, use exact division
            return userPeriod % mvPeriod == 0;
        }
        
        /**
         * Checks for known incompatible calendar boundary combinations.
         */
        private static boolean hasIncompatibleCalendarBoundaries(TimeGranularity mv, TimeGranularity user) {
            // Week and month boundaries don't align cleanly
            if ((mv == WEEK && user == MONTH) || (mv == MONTH && user == WEEK)) {
                return true;
            }
            
            // Week and quarter boundaries don't align cleanly  
            if ((mv == WEEK && user == QUARTER) || (mv == QUARTER && user == WEEK)) {
                return true;
            }
            
            // Add other known problematic combinations as needed
            return false;
        }
        
        /**
         * Checks if two periods are approximately divisible, with tolerance for calendar variations.
         */
        private static boolean isApproximatelyDivisible(long mvPeriod, long userPeriod) {
            if (mvPeriod > userPeriod) {
                return false;
            }
            
            double ratio = (double) userPeriod / mvPeriod;
            double roundedRatio = Math.round(ratio);
            
            // Allow 5% tolerance for calendar variations (months, quarters, years)
            double tolerance = 0.05;
            return Math.abs(ratio - roundedRatio) <= tolerance;
        }
        
        /**
         * Parse granularity from unit string (case-insensitive).
         */
        public static TimeGranularity fromUnit(String unit) {
            if (unit == null) {
                return UNKNOWN;
            }
            
            String normalizedUnit = unit.toLowerCase(Locale.ROOT).trim();
            for (TimeGranularity granularity : values()) {
                if (granularity.unitName.equals(normalizedUnit)) {
                    return granularity;
                }
            }
            
            logger.debug("Unknown temporal unit: '{}'", unit);
            return UNKNOWN;
        }
        
        public String getUnitName() {
            return unitName;
        }
        
        @Override
        public String toString() {
            return unitName.toUpperCase(Locale.ROOT);
        }
    }
    
    /**
     * Extracts temporal granularity from a SQL expression.
     * Currently focuses on date_trunc function calls.
     */
    public TimeGranularity extractGranularity(Expression expr) {
        if (expr == null) {
            return TimeGranularity.UNKNOWN;
        }
        
        logger.debug("Analyzing expression for temporal granularity: {}", expr);
        
        if (expr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) expr;
            return extractFromFunctionCall(func);
        }
        
        // Future: Could extend to handle other temporal expressions like EXTRACT, date arithmetic, etc.
        logger.debug("Expression is not a function call, no temporal granularity detected");
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Extracts granularity from function calls, specifically looking for date_trunc.
     */
    private TimeGranularity extractFromFunctionCall(FunctionCall func) {
        String functionName = func.getName().toString().toLowerCase(Locale.ROOT);
        
        if ("date_trunc".equals(functionName)) {
            return extractFromDateTruncFunction(func);
        }
        
        // Future: Could handle other temporal functions like date_format, extract, etc.
        logger.debug("Function '{}' is not a recognized temporal function", functionName);
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Extracts granularity from date_trunc function calls.
     * Expected format: date_trunc('unit', timestamp_column)
     */
    private TimeGranularity extractFromDateTruncFunction(FunctionCall func) {
        List<Expression> arguments = func.getArguments();
        
        if (arguments.size() < 2) {
            logger.debug("date_trunc function has insufficient arguments: {}", arguments.size());
            return TimeGranularity.UNKNOWN;
        }
        
        Expression unitExpression = arguments.get(0);
        
        // The unit should be a string literal like 'day', 'hour', etc.
        if (unitExpression instanceof StringLiteral) {
            StringLiteral unitLiteral = (StringLiteral) unitExpression;
            String unit = unitLiteral.getValue();
            
            TimeGranularity granularity = TimeGranularity.fromUnit(unit);
            logger.debug("Extracted temporal granularity from date_trunc('{}', ...): {}", unit, granularity);
            return granularity;
        }
        
        logger.debug("date_trunc first argument is not a string literal: {}", unitExpression.getClass().getSimpleName());
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Determines if an MV with the given granularity can satisfy a user query with the required granularity.
     * This is a convenience method that handles null values and logging.
     */
    public boolean canMvSatisfyQuery(TimeGranularity mvGranularity, TimeGranularity queryGranularity) {
        if (mvGranularity == null) mvGranularity = TimeGranularity.UNKNOWN;
        if (queryGranularity == null) queryGranularity = TimeGranularity.UNKNOWN;
        
        boolean canSatisfy = mvGranularity.canSatisfy(queryGranularity);
        
        logger.debug("Temporal granularity compatibility check: MV({}) vs Query({}) = {}", 
                    mvGranularity, queryGranularity, canSatisfy ? "COMPATIBLE" : "INCOMPATIBLE");
        
        return canSatisfy;
    }
}