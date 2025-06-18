package com.ns.rewrite.analysis;

import io.trino.sql.tree.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Analyzes temporal granularity in SQL expressions to determine materialized view compatibility.
 * 
 * This analyzer traverses SQL AST expressions to find temporal patterns and determine the finest
 * temporal granularity required. It supports various temporal functions, interval arithmetic,
 * conditional expressions, and literal analysis.
 * 
 * Based on Trino SQL official documentation for temporal functions.
 */
public class TemporalGranularityAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(TemporalGranularityAnalyzer.class);
    
    // ===== TEMPORAL FUNCTION CONSTANTS =====
    private static final String DATE_TRUNC = "date_trunc";
    private static final String FROM_UNIXTIME = "from_unixtime";
    private static final String FROM_MILLISECONDS = "from_milliseconds";
    private static final String FROM_UNIXTIME_NANOS = "from_unixtime_nanos";
    private static final String DATE_FORMAT = "date_format";
    private static final String DATE_ADD = "date_add";
    private static final String EXTRACT = "extract";
    private static final String TO_UNIXTIME = "to_unixtime";
    private static final String TO_MILLISECONDS = "to_milliseconds";
    private static final String TO_UNIXTIME_NANOS = "to_unixtime_nanos";
    
    // Standard Trino temporal extraction functions
    private static final String YEAR = "year";
    private static final String YEAR_OF_WEEK = "year_of_week";
    private static final String YOW = "yow";
    private static final String QUARTER = "quarter";
    private static final String MONTH = "month";
    private static final String WEEK = "week";
    private static final String WEEK_OF_YEAR = "week_of_year";
    private static final String DAY = "day";
    private static final String DAY_OF_MONTH = "day_of_month";
    private static final String DAY_OF_WEEK = "day_of_week";
    private static final String DOW = "dow";
    private static final String DAY_OF_YEAR = "day_of_year";
    private static final String DOY = "doy";
    private static final String HOUR = "hour";
    private static final String TIMEZONE_HOUR = "timezone_hour";
    private static final String MINUTE = "minute";
    private static final String TIMEZONE_MINUTE = "timezone_minute";
    private static final String SECOND = "second";
    private static final String MILLISECOND = "millisecond";
    
    // ===== TEMPORAL FUNCTION SETS =====
    private static final Set<String> COMPLEX_TEMPORAL_FUNCTIONS = Set.of(
        DATE_TRUNC, FROM_UNIXTIME, FROM_MILLISECONDS, FROM_UNIXTIME_NANOS, 
        DATE_FORMAT, DATE_ADD
    );
    
    private static final Set<String> STANDARD_TEMPORAL_FUNCTIONS = Set.of(
        YEAR, YEAR_OF_WEEK, YOW, QUARTER, MONTH, WEEK, WEEK_OF_YEAR,
        DAY, DAY_OF_MONTH, DAY_OF_WEEK, DOW, DAY_OF_YEAR, DOY,
        HOUR, TIMEZONE_HOUR, MINUTE, TIMEZONE_MINUTE, SECOND, MILLISECOND
    );
    
    // Combined set of all temporal functions
    private static final Set<String> TEMPORAL_FUNCTIONS;
    static {
        Set<String> combined = new HashSet<>(COMPLEX_TEMPORAL_FUNCTIONS);
        combined.addAll(STANDARD_TEMPORAL_FUNCTIONS);
        TEMPORAL_FUNCTIONS = Set.copyOf(combined);
    }
    
    /**
     * Represents temporal granularity with flexible support for custom intervals.
     * Can represent both standard units (hour, day, week) and custom intervals (30 minutes, 45 seconds).
     */
    public static class TimeGranularity {
        private final long milliseconds;
        private final String description;
        private final boolean isApproximate;
        
        // Standard granularities (for backward compatibility)
        public static final TimeGranularity NONE = new TimeGranularity(0L, "NONE", false);
        public static final TimeGranularity EXACT = new TimeGranularity(1L, "EXACT", false);
        public static final TimeGranularity MILLISECOND = new TimeGranularity(1L, "MILLISECOND", false);
        public static final TimeGranularity SECOND = new TimeGranularity(1_000L, "SECOND", false);
        public static final TimeGranularity MINUTE = new TimeGranularity(60_000L, "MINUTE", false);
        public static final TimeGranularity HOUR = new TimeGranularity(3_600_000L, "HOUR", false);
        public static final TimeGranularity DAY = new TimeGranularity(86_400_000L, "DAY", false);
        public static final TimeGranularity WEEK = new TimeGranularity(604_800_000L, "WEEK", false);
        public static final TimeGranularity MONTH = new TimeGranularity(2_592_000_000L, "MONTH", true); // Approximate: 30 days
        public static final TimeGranularity QUARTER = new TimeGranularity(7_776_000_000L, "QUARTER", true); // Approximate: 90 days
        public static final TimeGranularity YEAR = new TimeGranularity(31_536_000_000L, "YEAR", true); // Approximate: 365 days
        public static final TimeGranularity UNKNOWN = new TimeGranularity(Long.MAX_VALUE, "UNKNOWN", false);
        
        private TimeGranularity(long milliseconds, String description, boolean isApproximate) {
            this.milliseconds = milliseconds;
            this.description = description;
            this.isApproximate = isApproximate;
        }
        
        /**
         * Creates a custom granularity from milliseconds.
         */
        public static TimeGranularity fromMilliseconds(long milliseconds) {
            if (milliseconds <= 0) {
                return NONE;
            }
            
            // Check if it matches a standard granularity
            if (milliseconds == MILLISECOND.milliseconds) return MILLISECOND;
            if (milliseconds == SECOND.milliseconds) return SECOND;
            if (milliseconds == MINUTE.milliseconds) return MINUTE;
            if (milliseconds == HOUR.milliseconds) return HOUR;
            if (milliseconds == DAY.milliseconds) return DAY;
            if (milliseconds == WEEK.milliseconds) return WEEK;
            
            // Create custom granularity with descriptive name
            String description = generateDescription(milliseconds);
            return new TimeGranularity(milliseconds, description, false);
        }
        
        /**
         * Creates a custom granularity from seconds (for Unix timestamp patterns).
         */
        public static TimeGranularity fromSeconds(long seconds) {
            return fromMilliseconds(seconds * 1000L);
        }
        
        /**
         * Generates a human-readable description for custom granularities.
         */
        private static String generateDescription(long milliseconds) {
            if (milliseconds % HOUR.milliseconds == 0) {
                long hours = milliseconds / HOUR.milliseconds;
                return hours + (hours == 1 ? " HOUR" : " HOURS");
            } else if (milliseconds % MINUTE.milliseconds == 0) {
                long minutes = milliseconds / MINUTE.milliseconds;
                return minutes + (minutes == 1 ? " MINUTE" : " MINUTES");
            } else if (milliseconds % SECOND.milliseconds == 0) {
                long seconds = milliseconds / SECOND.milliseconds;
                return seconds + (seconds == 1 ? " SECOND" : " SECONDS");
            } else {
                return milliseconds + " MILLISECONDS";
            }
        }
        
        /**
         * Returns true if this granularity is finer (more precise) than the other.
         */
        public boolean isFinnerThan(TimeGranularity other) {
            return this.milliseconds < other.milliseconds;
        }
        
        /**
         * Returns true if this granularity is coarser (less precise) than the other.
         */
        public boolean isCoarserThan(TimeGranularity other) {
            return this.milliseconds > other.milliseconds;
        }
        
        /**
         * Returns true if this MV granularity can satisfy the given user query granularity.
         */
        public boolean canSatisfy(TimeGranularity userRequirement) {
            if (userRequirement == null) {
                return false; // Null is not compatible
            }
            
            // EXACT granularity can never be satisfied by any MV
            if (userRequirement.equals(EXACT)) {
                return false; // Raw timestamp GROUP BY requires exact precision - no MV can satisfy
            }
            
            // UNKNOWN granularities are rejected for safety
            if (userRequirement.equals(UNKNOWN) || this.equals(UNKNOWN)) {
                return false; // Cannot determine compatibility for unknown patterns
            }
            
            // Handle NONE cases correctly
            if (userRequirement.equals(NONE)) {
                return true; // Any MV can satisfy a non-temporal query
            }
            
            if (this.equals(NONE) && !userRequirement.equals(NONE)) {
                return false; // Non-temporal MV cannot satisfy temporal query
            }
            
            // Same granularity always works
            if (this.equals(userRequirement)) {
                return true;
            }
            
            // MV must be finer (smaller period) than user requirement
            if (this.milliseconds > userRequirement.milliseconds) {
                return false;
            }
            
            // Check for special incompatible combinations
            if (hasIncompatibleCalendarBoundaries(this, userRequirement)) {
                return false;
            }
            
            // Check if MV granularity divides evenly into user granularity
            long mvPeriod = this.milliseconds;
            long userPeriod = userRequirement.milliseconds;
            
            // For very large periods (month, quarter, year), use approximation with tolerance
            if (this.isApproximate || userRequirement.isApproximate || 
                mvPeriod >= MONTH.milliseconds || userPeriod >= MONTH.milliseconds) {
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
            if ((mv.equals(WEEK) && user.equals(MONTH)) || (mv.equals(MONTH) && user.equals(WEEK))) {
                return true;
            }
            
            // Week and quarter boundaries don't align cleanly  
            if ((mv.equals(WEEK) && user.equals(QUARTER)) || (mv.equals(QUARTER) && user.equals(WEEK))) {
                return true;
            }
            
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
                return NONE;
            }
            
            String normalizedUnit = unit.toLowerCase(Locale.ROOT).trim();
            
            // Check standard units
            switch (normalizedUnit) {
                case "millisecond": return MILLISECOND;
                case "second": return SECOND;
                case "minute": return MINUTE;
                case "hour": return HOUR;
                case "day": return DAY;
                case "week": return WEEK;
                case "month": return MONTH;
                case "quarter": return QUARTER;
                case "year": return YEAR;
                default:
                    logger.debug("Unrecognized temporal unit: '{}' - returning UNKNOWN", unit);
                    return UNKNOWN;
            }
        }
        
        public long getMilliseconds() {
            return milliseconds;
        }
        
        public String getDescription() {
            return description;
        }
        
        public String getUnitName() {
            return description.toLowerCase();
        }
        
        @Override
        public String toString() {
            return description;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof TimeGranularity)) return false;
            TimeGranularity other = (TimeGranularity) obj;
            return milliseconds == other.milliseconds;
        }
        
        @Override
        public int hashCode() {
            return Long.hashCode(milliseconds);
        }
    }
    
    /**
     * Represents a timestamp pattern with its regex and optional fixed granularity.
     * Used for flexible timestamp string parsing.
     */
    private static class TimestampPattern {
        private final Pattern pattern;
        private final TimeGranularity fixedGranularity; // null means analyze groups
        private final String description;
        
        public TimestampPattern(Pattern pattern, TimeGranularity fixedGranularity, String description) {
            this.pattern = pattern;
            this.fixedGranularity = fixedGranularity;
            this.description = description;
        }
        
        public Pattern getPattern() {
            return pattern;
        }
        
        public TimeGranularity getFixedGranularity() {
            return fixedGranularity;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Represents Unix timestamp function pairs and their scaling factors.
     * Used for context-aware parsing of Unix timestamp patterns.
     */
    private enum UnixTimeScale {
        SECONDS(FROM_UNIXTIME, TO_UNIXTIME, 1L, "seconds"),
        MILLISECONDS(FROM_MILLISECONDS, TO_MILLISECONDS, 1_000L, "milliseconds"),
        NANOSECONDS(FROM_UNIXTIME_NANOS, TO_UNIXTIME_NANOS, 1_000_000_000L, "nanoseconds");
        
        private final String fromFunction;
        private final String toFunction;
        private final long scalingFactorToSeconds;
        private final String description;
        
        UnixTimeScale(String fromFunction, String toFunction, long scalingFactorToSeconds, String description) {
            this.fromFunction = fromFunction;
            this.toFunction = toFunction;
            this.scalingFactorToSeconds = scalingFactorToSeconds;
            this.description = description;
        }
        
        public String getFromFunction() {
            return fromFunction;
        }
        
        public String getToFunction() {
            return toFunction;
        }
        
        public long getScalingFactorToSeconds() {
            return scalingFactorToSeconds;
        }
        
        public String getDescription() {
            return description;
        }
        
        /**
         * Finds the scale based on the outer function name.
         */
        public static UnixTimeScale fromOuterFunction(String functionName) {
            for (UnixTimeScale scale : values()) {
                if (scale.getFromFunction().equals(functionName)) {
                    return scale;
                }
            }
            return null;
        }
        
        /**
         * Validates that the inner function is compatible with this scale.
         */
        public boolean isCompatibleInnerFunction(String functionName) {
            return this.toFunction.equals(functionName);
        }
    }
    
    // ===== TIMESTAMP PATTERN DEFINITIONS =====
    private static final List<TimestampPattern> TIMESTAMP_PATTERNS = List.of(
        // Date only patterns - highest precision is DAY
        new TimestampPattern(
            Pattern.compile("^(?<year>\\d{4})-(?<month>\\d{2})-(?<day>\\d{2})$"),
            TimeGranularity.DAY,
            "ISO date: YYYY-MM-DD"
        ),
        
        new TimestampPattern(
            Pattern.compile("^(?<year>\\d{4})/(?<month>\\d{2})/(?<day>\\d{2})$"),
            TimeGranularity.DAY,
            "US date: YYYY/MM/DD"
        ),
        
        // Explicit hour boundary patterns (more specific, so checked first)
        new TimestampPattern(
            Pattern.compile("^(?<year>\\d{4})-(?<month>\\d{2})-(?<day>\\d{2})[T ](?<hour>\\d{2}):00:00(?:\\.000)?(?<timezone>[Z]|[+-]\\d{2}:?\\d{2}|\\s+[A-Z]{3,4})?$"),
            TimeGranularity.HOUR,
            "Hour boundary with optional timezone"
        ),
        
        new TimestampPattern(
            Pattern.compile("^(?<year>\\d{4})/(?<month>\\d{2})/(?<day>\\d{2}) (?<hour>\\d{2}):00:00$"),
            TimeGranularity.HOUR,
            "US hour boundary"
        ),
        
        // ISO 8601 without seconds (YYYY-MM-DD HH:MM and YYYY-MM-DDTHH:MM)
        new TimestampPattern(
            Pattern.compile("^(?<year>\\d{4})-(?<month>\\d{2})-(?<day>\\d{2})[T ](?<hour>\\d{2}):(?<minute>\\d{2})$"),
            TimeGranularity.MINUTE,
            "ISO 8601 without seconds: YYYY-MM-DD HH:MM or YYYY-MM-DDTHH:MM"
        ),
        
        // US format without seconds (YYYY/MM/DD HH:MM)
        new TimestampPattern(
            Pattern.compile("^(?<year>\\d{4})/(?<month>\\d{2})/(?<day>\\d{2}) (?<hour>\\d{2}):(?<minute>\\d{2})$"),
            TimeGranularity.MINUTE,
            "US format without seconds: YYYY/MM/DD HH:MM"
        ),
        
        // ISO 8601 full timestamp with comprehensive support
        new TimestampPattern(
            Pattern.compile("^(?<year>\\d{4})-(?<month>\\d{2})-(?<day>\\d{2})[T ](?<hour>\\d{2}):(?<minute>\\d{2}):(?<second>\\d{2})(?:\\.(?<millisecond>\\d{1,6}))?(?<timezone>[Z]|[+-]\\d{2}:?\\d{2}|\\s+[A-Z]{3,4})?$"),
            null, // Analyze groups
            "ISO 8601 timestamp with optional milliseconds and timezone"
        ),
        
        // US format timestamp
        new TimestampPattern(
            Pattern.compile("^(?<year>\\d{4})/(?<month>\\d{2})/(?<day>\\d{2}) (?<hour>\\d{2}):(?<minute>\\d{2}):(?<second>\\d{2})(?:\\.(?<millisecond>\\d{1,6}))?$"),
            null, // Analyze groups
            "US timestamp with optional milliseconds"
        ),
        
        // US date order with seconds (MM/DD/YYYY HH:MM:SS)
        new TimestampPattern(
            Pattern.compile("^(?<month>\\d{2})/(?<day>\\d{2})/(?<year>\\d{4}) (?<hour>\\d{2}):(?<minute>\\d{2}):(?<second>\\d{2})(?:\\.(?<millisecond>\\d{1,6}))?$"),
            null, // Analyze groups
            "US date order with seconds: MM/DD/YYYY HH:MM:SS"
        ),
        
        // US date order without seconds (MM/DD/YYYY HH:MM)
        new TimestampPattern(
            Pattern.compile("^(?<month>\\d{2})/(?<day>\\d{2})/(?<year>\\d{4}) (?<hour>\\d{2}):(?<minute>\\d{2})$"),
            TimeGranularity.MINUTE,
            "US date order without seconds: MM/DD/YYYY HH:MM"
        ),
        
        // European format (DD/MM/YYYY or DD-MM-YYYY)
        new TimestampPattern(
            Pattern.compile("^(?<day>\\d{2})[/-](?<month>\\d{2})[/-](?<year>\\d{4}) (?<hour>\\d{2}):(?<minute>\\d{2}):(?<second>\\d{2})(?:\\.(?<millisecond>\\d{1,6}))?$"),
            null, // Analyze groups  
            "European timestamp"
        ),
        
        // Compact format (no separators)
        new TimestampPattern(
            Pattern.compile("^(?<year>\\d{4})(?<month>\\d{2})(?<day>\\d{2})(?<hour>\\d{2})(?<minute>\\d{2})(?<second>\\d{2})(?<millisecond>\\d{3})?$"),
            null, // Analyze groups
            "Compact timestamp: YYYYMMDDHHMMSS"
        ),
        
        // Time only patterns (assume current date)
        new TimestampPattern(
            Pattern.compile("^(?<hour>\\d{2}):(?<minute>\\d{2}):(?<second>\\d{2})(?:\\.(?<millisecond>\\d{1,6}))?$"),
            null, // Analyze groups
            "Time only: HH:MM:SS"
        ),
        
        new TimestampPattern(
            Pattern.compile("^(?<hour>\\d{2}):(?<minute>\\d{2})$"),
            TimeGranularity.MINUTE,
            "Time only: HH:MM"
        )
    );
    
    // ===== PUBLIC API =====
    
    /**
     * Extracts temporal granularity from a SQL expression (unified method for GROUP BY and filter analysis).
     * 
     * @param expr The SQL expression to analyze
     * @return The finest temporal granularity found, or NONE if no temporal patterns detected
     * 
     * Examples:
     * - date_trunc('hour', timestamp) → HOUR
     * - date_add('minute', 30, date_trunc('hour', timestamp)) → MINUTE (finest of minute and hour)
     * - EXTRACT(HOUR FROM timestamp) → HOUR
     */
    public TimeGranularity extractGranularity(Expression expr) {
        return analyzeTemporalExpression(expr);
    }
    
    /**
     * Extracts temporal granularity requirements from filter expressions (WHERE clauses).
     * 
     * @param whereClause The WHERE clause expression to analyze
     * @return The minimum granularity required for the MV to satisfy the filter constraints
     * 
     * Examples:
     * - timestamp = '2025-01-01 15:30:00' → MINUTE
     * - EXTRACT(HOUR FROM timestamp) = 15 → HOUR
     */
    public TimeGranularity extractFilterGranularity(Expression whereClause) {
        if (whereClause == null) {
            return TimeGranularity.NONE;
        }
        
        logger.debug("Analyzing filter expression for temporal granularity requirements: {}", whereClause);
        
        // Handle special filter-specific expression types
        if (whereClause instanceof ComparisonExpression) {
            return analyzeComparisonFilter((ComparisonExpression) whereClause);
        }
        
        if (whereClause instanceof BetweenPredicate) {
            return analyzeBetweenFilter((BetweenPredicate) whereClause);
        }
        
        if (whereClause instanceof LogicalExpression) {
            return analyzeLogicalFilter((LogicalExpression) whereClause);
        }
        
        // For other expressions, use the unified temporal detection
        return analyzeTemporalExpression(whereClause);
    }
    
    /**
     * Determines if an MV with the given granularity can satisfy a user query with the required granularity.
     * 
     * @param mvGranularity The materialized view's temporal granularity
     * @param queryGranularity The user query's required temporal granularity
     * @return true if the MV can satisfy the query requirements
     */
    public boolean canMvSatisfyQuery(TimeGranularity mvGranularity, TimeGranularity queryGranularity) {
        if (mvGranularity == null) mvGranularity = TimeGranularity.NONE;
        if (queryGranularity == null) queryGranularity = TimeGranularity.NONE;
        
        boolean canSatisfy = mvGranularity.canSatisfy(queryGranularity);
        
        logger.debug("Temporal granularity compatibility check: MV({}) vs Query({}) = {}", 
                    mvGranularity, queryGranularity, canSatisfy ? "COMPATIBLE" : "INCOMPATIBLE");
        
        return canSatisfy;
    }
    
    // ===== CORE AST TRAVERSAL =====
    
    /**
     * Core method for unified temporal granularity extraction from any expression.
     * Uses recursive traversal to find all temporal patterns and returns the finest granularity.
     * 
     * @param expr The expression to analyze
     * @return The finest temporal granularity found across all nested patterns
     */
    private TimeGranularity analyzeTemporalExpression(Expression expr) {
        if (expr == null) {
            return TimeGranularity.NONE;
        }
        
        logger.debug("=== TEMPORAL ANALYSIS START ===");
        logger.debug("Expression type: {}", expr.getClass().getSimpleName());
        logger.debug("Expression toString: {}", expr.toString());
        
        logger.debug("Analyzing expression for temporal granularity: {}", expr);
        
        // Try function-based detection first (date_trunc, etc.)
        if (expr instanceof FunctionCall) {
            logger.debug("Processing as FunctionCall");
            TimeGranularity funcResult = handleFunctionCall((FunctionCall) expr);
            if (funcResult != TimeGranularity.NONE) {
                return funcResult;
            }
        }
        
        // Try direct AST analysis for temporal literals
        TimeGranularity literalResult = analyzeTemporalLiterals(expr);
        if (literalResult != TimeGranularity.NONE) {
            return literalResult;
        }
        
        // Handle complex expressions that might contain temporal patterns
        return handleComplexExpression(expr);
    }
    
    /**
     * Handles complex expression types that might contain nested temporal patterns.
     * Dispatches to specific handlers based on expression type.
     * 
     * @param expr The complex expression to analyze
     * @return The finest temporal granularity found
     */
    private TimeGranularity handleComplexExpression(Expression expr) {
        if (expr instanceof ArithmeticBinaryExpression) {
            return handleArithmeticExpression((ArithmeticBinaryExpression) expr);
        }
        
        if (expr instanceof Cast) {
            return handleCastExpression((Cast) expr);
        }
        
        if (expr instanceof SearchedCaseExpression) {
            logger.debug("Processing as SearchedCaseExpression");
            return handleSearchedCaseExpression((SearchedCaseExpression) expr);
        }
        
        if (expr instanceof SimpleCaseExpression) {
            return handleSimpleCaseExpression((SimpleCaseExpression) expr);
        }
        
        if (expr instanceof IfExpression) {
            return handleIfExpression((IfExpression) expr);
        }
        
        if (expr instanceof ComparisonExpression) {
            logger.debug("Processing as ComparisonExpression");
            return handleComparisonExpression((ComparisonExpression) expr);
        }
        
        if (expr instanceof Extract) {
            logger.debug("Processing as Extract expression");
            return handleExtractExpression((Extract) expr);
        }
        
        if (expr instanceof InPredicate) {
            logger.debug("Processing as InPredicate");
            return handleInPredicate((InPredicate) expr);
        }
        
        //logger.debug("Expression type '{}' not recognized for temporal analysis - no temporal granularity found", expr.getClass().getSimpleName());
        return TimeGranularity.NONE;
    }
    
    // ===== EXPRESSION TYPE HANDLERS =====
    
    /**
     * Handles FunctionCall expressions by identifying temporal functions and processing them.
     * Routes to appropriate processors based on function type (standard vs complex).
     * 
     * @param func The function call to analyze
     * @return The temporal granularity from the function or its arguments
     */
    private TimeGranularity handleFunctionCall(FunctionCall func) {
        String functionName = func.getName().toString().toLowerCase(Locale.ROOT);
        logger.debug("Temporal function found as {}", functionName);

        // Early exit for non-temporal functions
        if (!TEMPORAL_FUNCTIONS.contains(functionName)) {
            logger.debug("No corresponding temporal function found, searching nested arguements...");
            return searchNestedArguments(func);
        }
        
        // Check if this is a standard temporal function with fixed granularity
        if (STANDARD_TEMPORAL_FUNCTIONS.contains(functionName)) {
            return processStandardTemporalFunction(functionName);
        }
        
        // Handle complex temporal functions that require parameter analysis
        if (COMPLEX_TEMPORAL_FUNCTIONS.contains(functionName)) {
            // Check for Unix timestamp patterns first (they have higher priority for custom granularities)
            UnixTimeScale scale = UnixTimeScale.fromOuterFunction(functionName);
            if (scale != null) {
                TimeGranularity unixPattern = detectUnixTimestampGranularity(func, scale);
                if (unixPattern != TimeGranularity.NONE && unixPattern != TimeGranularity.UNKNOWN) {
                    return unixPattern;
                }
            }
            
            // Handle specific complex temporal functions
            switch (functionName) {
                case DATE_TRUNC:
                    return processDateTruncFunction(func);
                case DATE_FORMAT:
                    return processDateFormatFunction(func);
                case DATE_ADD:
                    return processDateAddFunction(func);
                default:
                    // For other complex temporal functions, recursively search their arguments
                    return searchNestedArguments(func);
            }
        }
        
        // This should not happen if our sets are properly maintained
        logger.warn("Temporal function '{}' found in TEMPORAL_FUNCTIONS but not in specific category sets", functionName);
        return searchNestedArguments(func);
    }
    
    /**
     * Handles arithmetic expressions that might contain temporal functions or interval arithmetic.
     * 
     * @param expr The arithmetic expression to analyze
     * @return The finest granularity from operands or interval patterns
     * 
     * Examples:
     * - timestamp + interval '3' hour → HOUR
     * - date_trunc('day', ts) - interval '2' minute → MINUTE (finest of day and minute)
     */
    private TimeGranularity handleArithmeticExpression(ArithmeticBinaryExpression expr) {
        ArithmeticBinaryExpression.Operator operator = expr.getOperator();
        
        // Special handling for ADD and SUBTRACT operations with intervals
        if (operator == ArithmeticBinaryExpression.Operator.ADD || 
            operator == ArithmeticBinaryExpression.Operator.SUBTRACT) {
            
            TimeGranularity intervalGranularity = extractIntervalGranularity(expr);
            if (intervalGranularity != TimeGranularity.NONE && intervalGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found interval arithmetic granularity {} in {} expression", intervalGranularity, operator);
                return intervalGranularity;
            }
        }
        
        // Check both left and right operands for temporal patterns (existing behavior)
        TimeGranularity leftGranularity = extractGranularity(expr.getLeft());
        TimeGranularity rightGranularity = extractGranularity(expr.getRight());
        
        // Return the finest granularity found
        TimeGranularity finestGranularity = getFinestGranularity(leftGranularity, rightGranularity);
        if (finestGranularity != TimeGranularity.NONE) {
            logger.debug("Found temporal granularity {} in arithmetic expression operands", finestGranularity);
        }
        
        return finestGranularity;
    }
    
    /**
     * Handles CAST expressions that might wrap temporal functions.
     * 
     * @param expr The CAST expression to analyze
     * @return The granularity from the wrapped expression
     */
    private TimeGranularity handleCastExpression(Cast expr) {
        TimeGranularity granularity = extractGranularity(expr.getExpression());
        if (granularity != TimeGranularity.NONE) {
            logger.debug("Found temporal granularity {} within CAST expression", granularity);
        }
        return granularity;
    }
    
    /**
     * Handles EXTRACT expressions by analyzing the field being extracted.
     * This is the single, authoritative handler for all EXTRACT expressions (io.trino.sql.tree.Extract).
     * 
     * EXTRACT expressions in Trino are parsed as Extract AST nodes, not FunctionCall nodes,
     * so this method handles all EXTRACT patterns in the system.
     * 
     * @param expr The EXTRACT expression to analyze
     * @return The finest granularity based on the extracted field and source expression
     * 
     * Examples:
     * - EXTRACT(HOUR FROM timestamp) → HOUR
     * - EXTRACT(DAY FROM date_trunc('month', ts)) → DAY (finest of DAY from field and MONTH from source)
     * - EXTRACT(MINUTE FROM date_add('second', 30, ts)) → MINUTE (finest of MINUTE from field and SECOND from source)
     */
    private TimeGranularity handleExtractExpression(Extract expr) {
        TimeGranularity fieldGranularity = TimeGranularity.NONE;
        
        // Extract granularity from the field being extracted
        Extract.Field field = expr.getField();
        if (field != null) {
            String unit = field.toString().toLowerCase();
            fieldGranularity = TimeGranularity.fromUnit(unit);
            if (fieldGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("EXTRACT({}) field requires {} granularity", unit, fieldGranularity);
            } else {
                logger.debug("Unrecognized EXTRACT field: '{}' - treating as UNKNOWN", unit);
                fieldGranularity = TimeGranularity.UNKNOWN;
            }
        } else {
            logger.debug("EXTRACT expression has no field - cannot determine granularity");
        }
        
        // Also check the source expression being extracted from for nested temporal patterns
        TimeGranularity sourceGranularity = extractGranularity(expr.getExpression());
        if (sourceGranularity != TimeGranularity.NONE && sourceGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found nested temporal granularity {} in EXTRACT source expression", sourceGranularity);
        }
        
        // Return the finest granularity between field and source
        TimeGranularity finestGranularity = getFinestGranularity(fieldGranularity, sourceGranularity);
        logger.debug("EXTRACT expression analysis: field={}, source={}, finest={}", fieldGranularity, sourceGranularity, finestGranularity);
        return finestGranularity;
    }
    
    /**
     * Handles searched CASE expressions (CASE WHEN condition THEN result).
     * Returns the finest temporal granularity found across all possible result branches.
     * 
     * @param expr The searched CASE expression to analyze
     * @return The finest granularity from all branches
     */
    private TimeGranularity handleSearchedCaseExpression(SearchedCaseExpression expr) {
        logger.debug("Analyzing searched CASE expression with {} WHEN clauses", expr.getWhenClauses().size());
        
        // Collect granularities from all possible result branches
        TimeGranularity[] granularities = new TimeGranularity[expr.getWhenClauses().size() + 1]; // +1 for ELSE
        int index = 0;
        
        // Check each WHEN clause's result (THEN part) for temporal patterns
        for (WhenClause whenClause : expr.getWhenClauses()) {
            // Check the condition (WHEN part) for temporal patterns in the condition itself
            TimeGranularity conditionGranularity = extractGranularity(whenClause.getOperand());
            if (conditionGranularity != TimeGranularity.NONE && conditionGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in searched CASE WHEN condition", conditionGranularity);
                granularities[index++] = conditionGranularity;
            }
            
            // Check the result (THEN part) - this is a possible output of the CASE expression
            TimeGranularity resultGranularity = extractGranularity(whenClause.getResult());
            if (resultGranularity != TimeGranularity.NONE && resultGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in searched CASE WHEN result", resultGranularity);
                granularities[index++] = resultGranularity;
            }
        }
        
        // Check the ELSE clause if present - this is also a possible output
        if (expr.getDefaultValue().isPresent()) {
            TimeGranularity elseGranularity = extractGranularity(expr.getDefaultValue().get());
            if (elseGranularity != TimeGranularity.NONE && elseGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in searched CASE ELSE clause", elseGranularity);
                granularities[index++] = elseGranularity;
            }
        }
        
        // Return the finest granularity among all collected granularities
        TimeGranularity finestGranularity = getFinestGranularity(granularities);
        logger.debug("Finest temporal granularity from searched CASE expression: {}", finestGranularity);
        return finestGranularity;
    }
    
    /**
     * Handles simple CASE expressions (CASE column WHEN value THEN result).
     * Returns the finest temporal granularity found across all possible result branches.
     * 
     * @param expr The simple CASE expression to analyze
     * @return The finest granularity from all branches and operands
     */
    private TimeGranularity handleSimpleCaseExpression(SimpleCaseExpression expr) {
        logger.debug("Analyzing simple CASE expression with {} WHEN clauses", expr.getWhenClauses().size());
        
        // Collect granularities from all possible result branches and operands
        TimeGranularity[] granularities = new TimeGranularity[expr.getWhenClauses().size() * 2 + 2]; // operand + WHEN values + THEN results + ELSE
        int index = 0;
        
        // Check the operand (the expression being tested)
        TimeGranularity operandGranularity = extractGranularity(expr.getOperand());
        if (operandGranularity != TimeGranularity.NONE && operandGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in simple CASE operand", operandGranularity);
            granularities[index++] = operandGranularity;
        }
        
        // Check each WHEN clause for temporal patterns
        for (WhenClause whenClause : expr.getWhenClauses()) {
            // Check the condition (WHEN part) - the value being compared against
            TimeGranularity conditionGranularity = extractGranularity(whenClause.getOperand());
            if (conditionGranularity != TimeGranularity.NONE && conditionGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in simple CASE WHEN condition", conditionGranularity);
                granularities[index++] = conditionGranularity;
            }
            
            // Check the result (THEN part) - this is a possible output of the CASE expression
            TimeGranularity resultGranularity = extractGranularity(whenClause.getResult());
            if (resultGranularity != TimeGranularity.NONE && resultGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in simple CASE WHEN result", resultGranularity);
                granularities[index++] = resultGranularity;
            }
        }
        
        // Check the ELSE clause if present - this is also a possible output
        if (expr.getDefaultValue().isPresent()) {
            TimeGranularity elseGranularity = extractGranularity(expr.getDefaultValue().get());
            if (elseGranularity != TimeGranularity.NONE && elseGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in simple CASE ELSE clause", elseGranularity);
                granularities[index++] = elseGranularity;
            }
        }
        
        // Return the finest granularity among all collected granularities
        TimeGranularity finestGranularity = getFinestGranularity(granularities);
        logger.debug("Finest temporal granularity from simple CASE expression: {}", finestGranularity);
        return finestGranularity;
    }
    
    /**
     * Handles IF expressions by analyzing condition and result branches.
     * Returns the finest temporal granularity found across all possible result branches.
     * 
     * @param expr The IF expression to analyze
     * @return The finest granularity from all branches
     */
    private TimeGranularity handleIfExpression(IfExpression expr) {
        // Collect granularities from all branches
        TimeGranularity[] granularities = new TimeGranularity[3]; // condition + true + false
        int index = 0;
        
        // Check the condition
        TimeGranularity conditionGranularity = extractGranularity(expr.getCondition());
        if (conditionGranularity != TimeGranularity.NONE && conditionGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in IF condition", conditionGranularity);
            granularities[index++] = conditionGranularity;
        }
        
        // Check the true result - this is a possible output of the IF expression
        TimeGranularity trueResultGranularity = extractGranularity(expr.getTrueValue());
        if (trueResultGranularity != TimeGranularity.NONE && trueResultGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in IF true branch", trueResultGranularity);
            granularities[index++] = trueResultGranularity;
        }
        
        // Check the false result if present - this is also a possible output
        if (expr.getFalseValue().isPresent()) {
            TimeGranularity falseResultGranularity = extractGranularity(expr.getFalseValue().get());
            if (falseResultGranularity != TimeGranularity.NONE && falseResultGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in IF false branch", falseResultGranularity);
                granularities[index++] = falseResultGranularity;
            }
        }
        
        // Return the finest granularity among all collected granularities
        TimeGranularity finestGranularity = getFinestGranularity(granularities);
        logger.debug("Finest temporal granularity from IF expression: {}", finestGranularity);
        return finestGranularity;
    }
    
    /**
     * Handles comparison expressions by analyzing both operands.
     * 
     * @param expr The comparison expression to analyze
     * @return The granularity from either operand
     * 
     * Examples:
     * - DATE_TRUNC('month', ...) = DATE_TRUNC('month', ...) → MONTH
     */
    private TimeGranularity handleComparisonExpression(ComparisonExpression expr) {
        logger.debug("Analyzing comparison expression with operator: {}", expr.getOperator());
        
        // Check both left and right operands for temporal patterns
        TimeGranularity leftGranularity = extractGranularity(expr.getLeft());
        if (leftGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in left operand of comparison", leftGranularity);
            return leftGranularity;
        }
        
        TimeGranularity rightGranularity = extractGranularity(expr.getRight());
        if (rightGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in right operand of comparison", rightGranularity);
            return rightGranularity;
        }
        
        logger.debug("No temporal granularity found in comparison expression");
        return TimeGranularity.NONE;
    }
    
    /**
     * Handles IN predicate expressions by analyzing the value and value list.
     * 
     * @param expr The IN predicate expression to analyze
     * @return The granularity from the value expression or value list
     * 
     * Examples:
     * - MONTH(timestamp) IN (1, 4, 7, 10) → MONTH
     * - date_trunc('day', ts) IN (date1, date2) → DAY
     */
    private TimeGranularity handleInPredicate(InPredicate expr) {
        logger.debug("Analyzing IN predicate expression");
        
        // Check the value expression (left side of IN) for temporal patterns
        TimeGranularity valueGranularity = extractGranularity(expr.getValue());
        if (valueGranularity != TimeGranularity.NONE && valueGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in IN predicate value expression", valueGranularity);
            return valueGranularity;
        }
        
        // Check the value list (right side of IN) for temporal patterns
        TimeGranularity listGranularity = extractGranularity(expr.getValueList());
        if (listGranularity != TimeGranularity.NONE && listGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in IN predicate value list", listGranularity);
            return listGranularity;
        }
        
        logger.debug("No temporal granularity found in IN predicate expression");
        return TimeGranularity.NONE;
    }
    
    // ===== SQL FUNCTION PROCESSORS =====
    
    /**
     * Processes date_trunc function calls to extract granularity from the unit parameter.
     * 
     * @param func The date_trunc function call
     * @return The granularity specified in the unit parameter
     * 
     * Examples:
     * - date_trunc('hour', timestamp) → HOUR
     * - date_trunc('day', timestamp) → DAY
     */
    private TimeGranularity processDateTruncFunction(FunctionCall func) {
        List<Expression> arguments = func.getArguments();
        
        if (arguments.size() < 2) {
            logger.debug("date_trunc function has insufficient arguments: {}", arguments.size());
            return TimeGranularity.NONE;
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
        return TimeGranularity.NONE;
    }
    
    /**
     * Processes DATE_FORMAT function calls by analyzing both the source expression and format string.
     * The effective granularity is constrained by the format string (acts as a ceiling).
     * 
     * @param func The DATE_FORMAT function call
     * @return The effective granularity considering both source and format constraints
     * 
     * Examples:
     * - DATE_FORMAT(date_trunc('hour', timestamp), '%Y-%m-%d') → DAY (format constrains to day)
     * - DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i') → MINUTE (format allows minute precision)
     */
    private TimeGranularity processDateFormatFunction(FunctionCall func) {
        List<Expression> arguments = func.getArguments();
        
        if (arguments.size() < 2) {
            logger.debug("DATE_FORMAT function has insufficient arguments: {}", arguments.size());
            return TimeGranularity.NONE;
        }
        
        // Analyze first argument (date expression) for source granularity
        Expression dateExpression = arguments.get(0);
        TimeGranularity sourceGranularity = extractGranularity(dateExpression);
        
        // Analyze second argument (format string) for output constraint
        Expression formatExpression = arguments.get(1);
        TimeGranularity formatGranularity = TimeGranularity.NONE;
        
        if (formatExpression instanceof StringLiteral) {
            String formatString = ((StringLiteral) formatExpression).getValue();
            formatGranularity = analyzeFormatStringGranularity(formatString);
            logger.debug("Format string '{}' constrains output to {} granularity", formatString, formatGranularity);
        } else {
            logger.debug("DATE_FORMAT second argument is not a string literal: {}", formatExpression.getClass().getSimpleName());
        }
        
        // The effective granularity is the coarser of the two (format string acts as ceiling)
        TimeGranularity effectiveGranularity = getCoarserGranularity(sourceGranularity, formatGranularity);
        
        logger.debug("DATE_FORMAT effective granularity: source={}, format={}, effective={}", 
                    sourceGranularity, formatGranularity, effectiveGranularity);
        
        return effectiveGranularity;
    }
    
    /**
     * Processes DATE_ADD function calls by finding ALL temporal patterns.
     * Uses the unified approach: find all granularities in all arguments, return finest.
     * 
     * @param func The DATE_ADD function call
     * @return The finest granularity from all arguments
     * 
     * Examples:
     * - DATE_ADD('minute', 30, date_trunc('hour', timestamp)) → MINUTE (finest of minute and hour)
     */
    private TimeGranularity processDateAddFunction(FunctionCall func) {
        logger.debug("Analyzing DATE_ADD function for all temporal patterns");
        
        // Use the general recursive approach to find ALL temporal granularities
        // This will find both the unit parameter and any nested temporal functions
        TimeGranularity granularity = searchNestedArguments(func);
        
        if (granularity != TimeGranularity.NONE && granularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} within DATE_ADD function", granularity);
        }
        
        return granularity;
    }
    
    /**
     * Processes standard Trino temporal extraction functions that have fixed granularities.
     * These functions return a specific temporal component and have predictable granularities
     * based on their function name alone, requiring no parameter analysis.
     * 
     * @param functionName The name of the standard temporal function (lowercase)
     * @return The fixed granularity associated with the function
     * 
     * Examples:
     * - day(timestamp) → DAY
     * - hour(timestamp) → HOUR  
     * - minute(timestamp) → MINUTE
     * - day_of_month(timestamp) → DAY
     */
    private TimeGranularity processStandardTemporalFunction(String functionName) {
        switch (functionName) {
            // YEAR granularity functions
            case YEAR:
            case YEAR_OF_WEEK:
            case YOW:
                logger.debug("Standard temporal function '{}' has YEAR granularity", functionName);
                return TimeGranularity.YEAR;
            
            // QUARTER granularity functions
            case QUARTER:
                logger.debug("Standard temporal function '{}' has QUARTER granularity", functionName);
                return TimeGranularity.QUARTER;
            
            // MONTH granularity functions
            case MONTH:
                logger.debug("Standard temporal function '{}' has MONTH granularity", functionName);
                return TimeGranularity.MONTH;
            
            // WEEK granularity functions
            case WEEK:
            case WEEK_OF_YEAR:
                logger.debug("Standard temporal function '{}' has WEEK granularity", functionName);
                return TimeGranularity.WEEK;
            
            // DAY granularity functions
            case DAY:
            case DAY_OF_MONTH:
            case DAY_OF_WEEK:
            case DOW:
            case DAY_OF_YEAR:
            case DOY:
                logger.debug("Standard temporal function '{}' has DAY granularity", functionName);
                return TimeGranularity.DAY;
            
            // HOUR granularity functions
            case HOUR:
            case TIMEZONE_HOUR:
                logger.debug("Standard temporal function '{}' has HOUR granularity", functionName);
                return TimeGranularity.HOUR;
            
            // MINUTE granularity functions
            case MINUTE:
            case TIMEZONE_MINUTE:
                logger.debug("Standard temporal function '{}' has MINUTE granularity", functionName);
                return TimeGranularity.MINUTE;
            
            // SECOND granularity functions
            case SECOND:
                logger.debug("Standard temporal function '{}' has SECOND granularity", functionName);
                return TimeGranularity.SECOND;
            
            // MILLISECOND granularity functions
            case MILLISECOND:
                logger.debug("Standard temporal function '{}' has MILLISECOND granularity", functionName);
                return TimeGranularity.MILLISECOND;
            
            default:
                logger.debug("Unknown standard temporal function: '{}'", functionName);
                return TimeGranularity.UNKNOWN;
        }
    }
    
    
    // ===== LITERAL AND STRING ANALYSIS =====
    
    /**
     * Analyzes DATE_FORMAT format strings to determine output granularity using regex patterns.
     * Checks from finest to coarsest granularity to avoid misclassification.
     * Based on official Trino DATE_FORMAT documentation.
     * 
     * @param formatString The format string to analyze
     * @return The granularity constrained by the format string
     */
    private TimeGranularity analyzeFormatStringGranularity(String formatString) {
        if (formatString == null || formatString.trim().isEmpty()) {
            return TimeGranularity.UNKNOWN;
        }
        
        String format = formatString.trim();
        
        // Check from finest to coarsest granularity to avoid mismatches
        
        // MILLISECOND: Contains fractional second specifiers (finest precision)
        if (format.matches(".*%f.*")) {
            logger.debug("Format '{}' contains fractional seconds - MILLISECOND granularity", format);
            return TimeGranularity.MILLISECOND;
        }
        
        // SECOND: Contains second specifiers
        if (format.matches(".*%s.*") || format.matches(".*%S.*") || 
            format.matches(".*%r.*") || format.matches(".*%T.*")) {
            logger.debug("Format '{}' contains seconds - SECOND granularity", format);
            return TimeGranularity.SECOND;
        }
        
        // MINUTE: Contains minute specifiers
        if (format.matches(".*%i.*")) {
            logger.debug("Format '{}' contains minutes - MINUTE granularity", format);
            return TimeGranularity.MINUTE;
        }
        
        // HOUR: Contains hour specifiers
        if (format.matches(".*%H.*") || format.matches(".*%h.*") || 
            format.matches(".*%I.*") || format.matches(".*%k.*") || 
            format.matches(".*%l.*") || format.matches(".*%p.*")) {
            logger.debug("Format '{}' contains hours - HOUR granularity", format);
            return TimeGranularity.HOUR;
        }
        
        // DAY: Contains day specifiers (including weekday names which represent daily grouping)
        if (format.matches(".*%a.*") || format.matches(".*%d.*") || 
            format.matches(".*%e.*") || format.matches(".*%j.*") || 
            format.matches(".*%W.*")) {
            logger.debug("Format '{}' contains day components - DAY granularity", format);
            return TimeGranularity.DAY;
        }
        
        // WEEK: Contains week specifiers (only %v is supported)
        if (format.matches(".*%v.*")) {
            logger.debug("Format '{}' contains week components - WEEK granularity", format);
            return TimeGranularity.WEEK;
        }
        
        // MONTH: Contains month specifiers
        if (format.matches(".*%b.*") || format.matches(".*%c.*") || 
            format.matches(".*%M.*") || format.matches(".*%m.*")) {
            logger.debug("Format '{}' contains month components - MONTH granularity", format);
            return TimeGranularity.MONTH;
        }
        
        // YEAR: Contains year specifiers
        if (format.matches(".*%Y.*") || format.matches(".*%y.*") || 
            format.matches(".*%x.*")) {
            logger.debug("Format '{}' contains year components - YEAR granularity", format);
            return TimeGranularity.YEAR;
        }
        
        // If no temporal components found, return UNKNOWN
        logger.debug("No temporal components found in format '{}' - UNKNOWN granularity", format);
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Analyzes temporal literal expressions using direct AST analysis.
     * Handles StringLiteral and other literal node types without regex patterns.
     * 
     * @param expr The literal expression to analyze
     * @return The granularity from temporal literals or unit strings
     */
    private TimeGranularity analyzeTemporalLiterals(Expression expr) {
        if (expr instanceof StringLiteral) {
            StringLiteral stringLiteral = (StringLiteral) expr;
            String value = stringLiteral.getValue();
            
            // First check if this is a temporal unit string (minute, hour, day, etc.)
            TimeGranularity unitGranularity = TimeGranularity.fromUnit(value);
            if (unitGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Extracted granularity {} from temporal unit string literal: '{}'", unitGranularity, value);
                return unitGranularity;
            }
            
            // Then analyze the string content for timestamp patterns
            TimeGranularity timestampGranularity = analyzeTimestampString(value);
            if (timestampGranularity != TimeGranularity.NONE && timestampGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Extracted granularity {} from timestamp string literal: '{}'", timestampGranularity, value);
                return timestampGranularity;
            }
        }
        
        // Check for GenericLiteral (typed literals like TIMESTAMP '...', DATE '...', TIME '...')
        if (expr instanceof GenericLiteral) {
            GenericLiteral genericLiteral = (GenericLiteral) expr;
            String literalString = genericLiteral.toString();
            
            // Extract the quoted value from typed literals like TIMESTAMP '2025-01-01 15:00:00'
            String extractedValue = extractValueFromTypedLiteral(literalString);
            if (extractedValue != null) {
                TimeGranularity granularity = analyzeTimestampString(extractedValue);
                if (granularity != TimeGranularity.NONE && granularity != TimeGranularity.UNKNOWN) {
                    logger.debug("Extracted granularity {} from generic literal: '{}'", granularity, literalString);
                    return granularity;
                }
            }
        }
        
        // Check for other literal types that might have temporal significance
        if (expr instanceof LongLiteral) {
            // Long literals might represent Unix timestamps, but without more context
            // we cannot determine granularity - leave as NONE for now
            return TimeGranularity.NONE;
        }
        
        return TimeGranularity.NONE;
    }
    
    /**
     * Analyzes timestamp strings for precision using flexible regex pattern matching.
     * Supports various timestamp formats including ISO 8601, US formats, and more.
     * 
     * @param timestamp The timestamp string to analyze
     * @return The granularity required based on timestamp precision
     * 
     * Examples:
     * - "2025-01-01" → DAY
     * - "2025-01-01T15:00:00Z" → HOUR  
     * - "2025-01-01 15:30:00" → MINUTE
     * - "2025/01/01 15:30:45.123" → MILLISECOND
     */
    private TimeGranularity analyzeTimestampString(String timestamp) {
        if (timestamp == null || timestamp.trim().isEmpty()) {
            return TimeGranularity.NONE;
        }
        
        String normalizedTimestamp = timestamp.trim();
        logger.debug("Analyzing timestamp string for precision: '{}'", normalizedTimestamp);
        
        // Try each pattern in order (more specific patterns first)
        for (TimestampPattern timestampPattern : TIMESTAMP_PATTERNS) {
            Matcher matcher = timestampPattern.getPattern().matcher(normalizedTimestamp);
            if (matcher.matches()) {
                logger.debug("Matched pattern: {}", timestampPattern.getDescription());
                
                if (timestampPattern.getFixedGranularity() != null) {
                    // Fixed granularity pattern
                    TimeGranularity granularity = timestampPattern.getFixedGranularity();
                    logger.debug("Fixed granularity {} from pattern: {}", granularity, timestampPattern.getDescription());
                    return granularity;
                } else {
                    // Analyze captured groups to determine granularity
                    TimeGranularity granularity = analyzeMatchedGroups(matcher);
                    logger.debug("Analyzed granularity {} from matched groups", granularity);
                    return granularity;
                }
            }
        }
        
        // If no pattern matches, return UNKNOWN for safety
        logger.debug("No timestamp pattern matched for '{}' - returning UNKNOWN for safety", normalizedTimestamp);
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Analyzes matched regex groups to determine the finest temporal granularity.
     * 
     * @param matcher The regex matcher with captured groups
     * @return The finest granularity based on captured components
     */
    private TimeGranularity analyzeMatchedGroups(Matcher matcher) {
        // Check for milliseconds first (finest precision)
        String milliseconds = matcher.group("millisecond");
        if (milliseconds != null && !milliseconds.isEmpty()) {
            // Check if milliseconds are non-zero (actual sub-second precision)
            if (!milliseconds.matches("0+")) {
                logger.debug("Found non-zero milliseconds: {} - requires MILLISECOND granularity", milliseconds);
                return TimeGranularity.MILLISECOND;
            }
            // If milliseconds are all zeros, continue to check seconds
        }
        
        // Check seconds (if not at boundary)
        String seconds = matcher.group("second");
        if (seconds != null && !"00".equals(seconds)) {
            logger.debug("Found non-zero seconds: {} - requires SECOND granularity", seconds);
            return TimeGranularity.SECOND;
        }
        
        // Check minutes (if not at boundary)
        String minutes = matcher.group("minute");
        if (minutes != null && !"00".equals(minutes)) {
            logger.debug("Found non-zero minutes: {} - requires MINUTE granularity", minutes);
            return TimeGranularity.MINUTE;
        }
        
        // Has time components but at hour boundary
        if (matcher.group("hour") != null) {
            logger.debug("Found hour component at boundary - requires HOUR granularity");
            return TimeGranularity.HOUR;
        }
        
        // Date only
        logger.debug("Only date components found - requires DAY granularity");
        return TimeGranularity.DAY;
    }
    
    /**
     * Extracts the quoted value from typed literals like TIMESTAMP '2025-01-01 15:00:00'.
     * 
     * @param literalString The full literal string including type prefix
     * @return Just the timestamp string without the type prefix and quotes
     */
    private String extractValueFromTypedLiteral(String literalString) {
        if (literalString == null || literalString.trim().isEmpty()) {
            return null;
        }
        
        // Look for patterns like TIMESTAMP '...', DATE '...', TIME '...'
        // Find the first single quote and the last single quote
        int firstQuote = literalString.indexOf('\'');
        int lastQuote = literalString.lastIndexOf('\'');
        
        if (firstQuote != -1 && lastQuote != -1 && firstQuote < lastQuote) {
            // Extract the content between the quotes
            String extractedValue = literalString.substring(firstQuote + 1, lastQuote);
            logger.debug("Extracted value '{}' from typed literal '{}'", extractedValue, literalString);
            return extractedValue;
        }
        
        logger.debug("Could not extract quoted value from literal: '{}'", literalString);
        return null;
    }
    
    // ===== FILTER-SPECIFIC ANALYSIS =====
    
    /**
     * Analyzes comparison expressions that might contain temporal filtering requirements.
     * 
     * @param expr The comparison expression to analyze
     * @return The finest granularity required for filtering
     * 
     * Examples:
     * - timestamp = '2025-01-01 15:30:00' → MINUTE
     * - timestamp >= start_time → depends on start_time granularity
     */
    private TimeGranularity analyzeComparisonFilter(ComparisonExpression expr) {
        logger.debug("Analyzing comparison filter: {}", expr);
        
        // Check if either operand is a timestamp literal that imposes granularity requirements
        TimeGranularity leftRequirement = analyzeOperandForTemporalRequirement(expr.getLeft());
        TimeGranularity rightRequirement = analyzeOperandForTemporalRequirement(expr.getRight());
        
        return getFinestGranularity(leftRequirement, rightRequirement);
    }
    
    /**
     * Analyzes BETWEEN predicates for temporal range filtering requirements.
     * 
     * @param expr The BETWEEN predicate to analyze
     * @return The finest granularity required for range filtering
     * 
     * Examples:
     * - timestamp BETWEEN '2025-01-01 15:30' AND '2025-01-01 16:30' → MINUTE
     */
    private TimeGranularity analyzeBetweenFilter(BetweenPredicate expr) {
        logger.debug("Analyzing BETWEEN filter: {}", expr);
        
        TimeGranularity minRequirement = analyzeOperandForTemporalRequirement(expr.getMin());
        TimeGranularity maxRequirement = analyzeOperandForTemporalRequirement(expr.getMax());
        
        // For range filters, we need to check boundary alignment
        TimeGranularity rangeRequirement = analyzeRangeAlignment(expr.getMin(), expr.getMax());
        
        return getFinestGranularity(minRequirement, maxRequirement, rangeRequirement);
    }
    
    /**
     * Analyzes logical expressions (AND, OR) by examining all sub-expressions.
     * 
     * @param expr The logical expression to analyze
     * @return The finest granularity required across all terms
     */
    private TimeGranularity analyzeLogicalFilter(LogicalExpression expr) {
        logger.debug("Analyzing logical expression: {} with {} terms", expr.getOperator(), expr.getTerms().size());
        
        TimeGranularity finestRequirement = TimeGranularity.NONE;
        
        for (Expression term : expr.getTerms()) {
            TimeGranularity termRequirement = extractFilterGranularity(term);
            finestRequirement = getFinestGranularity(finestRequirement, termRequirement);
        }
        
        return finestRequirement;
    }
    
    /**
     * Analyzes an operand to determine if it imposes temporal granularity requirements.
     * Uses the unified temporal detection logic.
     * 
     * @param operand The operand to analyze
     * @return The temporal granularity requirement imposed by the operand
     */
    private TimeGranularity analyzeOperandForTemporalRequirement(Expression operand) {
        if (operand == null) {
            return TimeGranularity.NONE;
        }
        
        logger.debug("Analyzing operand type: {} with content: {}", operand.getClass().getSimpleName(), operand);
        
        // Use unified temporal detection
        return analyzeTemporalExpression(operand);
    }
    
    /**
     * Analyzes range alignment for BETWEEN filters to determine granularity requirements.
     * 
     * @param min The minimum value expression
     * @param max The maximum value expression
     * @return The granularity required for proper range alignment
     */
    private TimeGranularity analyzeRangeAlignment(Expression min, Expression max) {
        // For now, return NONE - more sophisticated boundary analysis could be added
        // This would check if the range boundaries align with MV bucket boundaries
        return TimeGranularity.NONE;
    }
    
    // ===== INTERVAL ARITHMETIC =====
    
    /**
     * Extracts granularity from interval arithmetic expressions.
     * Analyzes patterns like: timestamp + interval '3' hour, date_trunc('day', ts) - interval '2' minute
     * Returns the finest granularity between the timestamp operand and the interval unit.
     * 
     * @param expr The arithmetic expression with potential interval operations
     * @return The finest granularity from interval unit and timestamp components
     */
    private TimeGranularity extractIntervalGranularity(ArithmeticBinaryExpression expr) {
        Expression leftOperand = expr.getLeft();
        Expression rightOperand = expr.getRight();
        
        // Check if either operand is an interval literal
        TimeGranularity intervalUnit = extractFromIntervalLiteral(leftOperand);
        TimeGranularity timestampGranularity = extractGranularity(rightOperand);
        
        if (intervalUnit == TimeGranularity.NONE || intervalUnit == TimeGranularity.UNKNOWN) {
            // Try the other way around: right operand might be interval
            intervalUnit = extractFromIntervalLiteral(rightOperand);
            timestampGranularity = extractGranularity(leftOperand);
        }
        
        // If we found both interval and timestamp components, return the finest granularity
        if (intervalUnit != TimeGranularity.NONE && intervalUnit != TimeGranularity.UNKNOWN &&
            timestampGranularity != TimeGranularity.NONE && timestampGranularity != TimeGranularity.UNKNOWN) {
            
            TimeGranularity finestGranularity = getFinestGranularity(intervalUnit, timestampGranularity);
            logger.debug("Interval arithmetic: timestamp granularity={}, interval unit={}, finest={}",
                        timestampGranularity, intervalUnit, finestGranularity);
            return finestGranularity;
        }
        
        // If only interval unit was found, that becomes the effective granularity
        if (intervalUnit != TimeGranularity.NONE && intervalUnit != TimeGranularity.UNKNOWN) {
            logger.debug("Found interval unit {} without explicit timestamp granularity", intervalUnit);
            return intervalUnit;
        }
        
        return TimeGranularity.NONE;
    }
    
    /**
     * Extracts temporal unit from interval literal expressions.
     * 
     * @param expr The expression that might be an interval literal
     * @return The granularity from the interval unit
     * 
     * Examples:
     * - interval '3' hour → HOUR
     * - interval '30' minute → MINUTE
     * - interval '1' day → DAY
     */
    private TimeGranularity extractFromIntervalLiteral(Expression expr) {
        // Check for IntervalLiteral directly
        if (expr instanceof IntervalLiteral) {
            IntervalLiteral interval = (IntervalLiteral) expr;
            
            // Extract the unit from the interval
            IntervalLiteral.IntervalField startField = interval.getStartField();
            if (startField != null) {
                String unit = startField.toString().toLowerCase();
                TimeGranularity granularity = TimeGranularity.fromUnit(unit);
                logger.debug("Extracted granularity {} from interval literal with unit '{}'", granularity, unit);
                return granularity;
            }
        }
        
        // Check for function calls that might represent intervals (like INTERVAL 'value' UNIT)
        if (expr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) expr;
            String funcName = func.getName().toString().toLowerCase();
            
            if ("interval".equals(funcName)) {
                List<Expression> args = func.getArguments();
                if (args.size() >= 2) {
                    // Second argument should be the unit
                    Expression unitExpr = args.get(1);
                    if (unitExpr instanceof StringLiteral) {
                        String unit = ((StringLiteral) unitExpr).getValue().toLowerCase();
                        TimeGranularity granularity = TimeGranularity.fromUnit(unit);
                        logger.debug("Extracted granularity {} from interval function with unit '{}'", granularity, unit);
                        return granularity;
                    }
                }
            }
        }
        
        return TimeGranularity.NONE;
    }
    
    // ===== UNIX TIMESTAMP PATTERN DETECTION =====
    
    /**
     * Detects Unix timestamp patterns with context-aware scale analysis.
     * Handles different scales (seconds, milliseconds, nanoseconds) and validates function pair compatibility.
     * 
     * Pattern: FROM_UNIXTIME(CAST(FLOOR(TO_UNIXTIME(timestamp) / divisor) AS BIGINT) * divisor)
     * 
     * @param fromFunction The outer FROM_* function to analyze
     * @param scale The detected scale context
     * @return The granularity based on the divisor pattern and scale
     */
    private TimeGranularity detectUnixTimestampGranularity(FunctionCall fromFunction, UnixTimeScale scale) {
        List<Expression> args = fromFunction.getArguments();
        if (args.isEmpty()) {
            logger.debug("Empty {} function has no temporal pattern", scale.getFromFunction());
            return TimeGranularity.NONE;
        }
        
        Expression firstArg = args.get(0);
        
        // Look for CAST(... AS BIGINT) * divisor pattern
        if (firstArg instanceof ArithmeticBinaryExpression) {
            ArithmeticBinaryExpression mult = (ArithmeticBinaryExpression) firstArg;
            if (mult.getOperator() == ArithmeticBinaryExpression.Operator.MULTIPLY) {
                // Extract the divisor and validate function pair compatibility
                DivisorExtractionResult result = extractDivisorWithScale(mult, scale);
                if (result.isValid()) {
                    long normalizedSeconds = result.getDivisor() / scale.getScalingFactorToSeconds();
                    logger.debug("Detected {} timestamp pattern: divisor={}, scale={}, normalized_seconds={}", 
                               scale.getDescription(), result.getDivisor(), scale.getScalingFactorToSeconds(), normalizedSeconds);
                    return TimeGranularity.fromSeconds(normalizedSeconds);
                } else if (result.hasIncompatibleFunctions()) {
                    logger.warn("Found incompatible function pair in Unix timestamp pattern: outer={}, inner={}", 
                              scale.getFromFunction(), result.getFoundInnerFunction());
                    return TimeGranularity.UNKNOWN;
                }
                // If we found multiplication but couldn't extract valid divisor, it might be a complex pattern
                logger.debug("Found multiplication in {} but couldn't parse valid divisor - returning UNKNOWN", scale.getFromFunction());
                return TimeGranularity.UNKNOWN;
            }
        }
        
        // Function with non-multiplication argument is likely not a temporal grouping pattern
        return TimeGranularity.NONE;
    }
    
    /**
     * Represents the result of divisor extraction with scale validation.
     */
    private static class DivisorExtractionResult {
        private final Long divisor;
        private final boolean valid;
        private final String foundInnerFunction;
        private final boolean incompatibleFunctions;
        
        public DivisorExtractionResult(Long divisor, boolean valid, String foundInnerFunction, boolean incompatibleFunctions) {
            this.divisor = divisor;
            this.valid = valid;
            this.foundInnerFunction = foundInnerFunction;
            this.incompatibleFunctions = incompatibleFunctions;
        }
        
        public static DivisorExtractionResult valid(long divisor) {
            return new DivisorExtractionResult(divisor, true, null, false);
        }
        
        public static DivisorExtractionResult invalid() {
            return new DivisorExtractionResult(null, false, null, false);
        }
        
        public static DivisorExtractionResult incompatible(String foundInnerFunction) {
            return new DivisorExtractionResult(null, false, foundInnerFunction, true);
        }
        
        public boolean isValid() { return valid; }
        public Long getDivisor() { return divisor; }
        public String getFoundInnerFunction() { return foundInnerFunction; }
        public boolean hasIncompatibleFunctions() { return incompatibleFunctions; }
    }
    
    /**
     * Extracts the divisor from a multiplication expression with scale-aware validation.
     * Validates that the inner TO_* function is compatible with the outer FROM_* function scale.
     * 
     * @param mult The multiplication expression to analyze
     * @param scale The expected scale context
     * @return The extraction result with divisor and validation status
     */
    private DivisorExtractionResult extractDivisorWithScale(ArithmeticBinaryExpression mult, UnixTimeScale scale) {
        Expression rightOperand = mult.getRight();
        
        // The right operand should be the divisor (numeric literal)
        if (rightOperand instanceof LongLiteral) {
            long divisor = Long.parseLong(((LongLiteral) rightOperand).getValue());
            
            // Verify the pattern by checking if the left operand contains the same divisor in a division
            // AND validate that the inner function is compatible with our scale
            DivisionValidationResult divisionResult = validateDivisionWithScale(mult.getLeft(), divisor, scale);
            if (divisionResult.isValid()) {
                return DivisorExtractionResult.valid(divisor);
            } else if (divisionResult.hasIncompatibleFunction()) {
                return DivisorExtractionResult.incompatible(divisionResult.getFoundFunction());
            }
        }
        
        return DivisorExtractionResult.invalid();
    }
    
    /**
     * Represents the result of division validation with function compatibility.
     */
    private static class DivisionValidationResult {
        private final boolean valid;
        private final String foundFunction;
        private final boolean incompatibleFunction;
        
        public DivisionValidationResult(boolean valid, String foundFunction, boolean incompatibleFunction) {
            this.valid = valid;
            this.foundFunction = foundFunction;
            this.incompatibleFunction = incompatibleFunction;
        }
        
        public static DivisionValidationResult valid() {
            return new DivisionValidationResult(true, null, false);
        }
        
        public static DivisionValidationResult invalid() {
            return new DivisionValidationResult(false, null, false);
        }
        
        public static DivisionValidationResult incompatible(String foundFunction) {
            return new DivisionValidationResult(false, foundFunction, true);
        }
        
        public boolean isValid() { return valid; }
        public String getFoundFunction() { return foundFunction; }
        public boolean hasIncompatibleFunction() { return incompatibleFunction; }
    }
    
    /**
     * Recursively validates division patterns and function compatibility.
     * 
     * @param expr The expression to search
     * @param expectedDivisor The divisor value to look for
     * @param scale The expected scale context
     * @return The validation result with compatibility information
     */
    private DivisionValidationResult validateDivisionWithScale(Expression expr, long expectedDivisor, UnixTimeScale scale) {
        if (expr instanceof ArithmeticBinaryExpression) {
            ArithmeticBinaryExpression arith = (ArithmeticBinaryExpression) expr;
            
            if (arith.getOperator() == ArithmeticBinaryExpression.Operator.DIVIDE) {
                Expression rightOperand = arith.getRight();
                if (rightOperand instanceof LongLiteral) {
                    long divisor = Long.parseLong(((LongLiteral) rightOperand).getValue());
                    if (divisor == expectedDivisor) {
                        // Check if the left operand contains compatible TO_* function
                        FunctionCompatibilityResult compatResult = findAndValidateToFunction(arith.getLeft(), scale);
                        if (compatResult.isCompatible()) {
                            return DivisionValidationResult.valid();
                        } else if (compatResult.hasIncompatibleFunction()) {
                            return DivisionValidationResult.incompatible(compatResult.getFoundFunction());
                        }
                    }
                }
            }
            
            // Recursively check both operands
            DivisionValidationResult leftResult = validateDivisionWithScale(arith.getLeft(), expectedDivisor, scale);
            if (leftResult.isValid() || leftResult.hasIncompatibleFunction()) {
                return leftResult;
            }
            
            return validateDivisionWithScale(arith.getRight(), expectedDivisor, scale);
        } else if (expr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) expr;
            
            // Check function arguments
            for (Expression arg : func.getArguments()) {
                DivisionValidationResult result = validateDivisionWithScale(arg, expectedDivisor, scale);
                if (result.isValid() || result.hasIncompatibleFunction()) {
                    return result;
                }
            }
        } else if (expr instanceof Cast) {
            Cast cast = (Cast) expr;
            return validateDivisionWithScale(cast.getExpression(), expectedDivisor, scale);
        }
        
        return DivisionValidationResult.invalid();
    }
    
    /**
     * Represents the result of function compatibility validation.
     */
    private static class FunctionCompatibilityResult {
        private final boolean compatible;
        private final String foundFunction;
        private final boolean hasIncompatibleFunction;
        
        public FunctionCompatibilityResult(boolean compatible, String foundFunction, boolean hasIncompatibleFunction) {
            this.compatible = compatible;
            this.foundFunction = foundFunction;
            this.hasIncompatibleFunction = hasIncompatibleFunction;
        }
        
        public static FunctionCompatibilityResult compatible() {
            return new FunctionCompatibilityResult(true, null, false);
        }
        
        public static FunctionCompatibilityResult notFound() {
            return new FunctionCompatibilityResult(false, null, false);
        }
        
        public static FunctionCompatibilityResult incompatible(String foundFunction) {
            return new FunctionCompatibilityResult(false, foundFunction, true);
        }
        
        public boolean isCompatible() { return compatible; }
        public String getFoundFunction() { return foundFunction; }
        public boolean hasIncompatibleFunction() { return hasIncompatibleFunction; }
    }
    
    /**
     * Recursively searches for TO_* functions and validates compatibility with the expected scale.
     * 
     * @param expr The expression to search
     * @param scale The expected scale context
     * @return The compatibility result
     */
    private FunctionCompatibilityResult findAndValidateToFunction(Expression expr, UnixTimeScale scale) {
        if (expr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) expr;
            String funcName = func.getName().toString().toLowerCase();
            
            // Check if this is a TO_* function
            if (funcName.equals(TO_UNIXTIME) || funcName.equals(TO_MILLISECONDS) || funcName.equals(TO_UNIXTIME_NANOS)) {
                if (scale.isCompatibleInnerFunction(funcName)) {
                    logger.debug("Found compatible function pair: {} with {}", scale.getFromFunction(), funcName);
                    return FunctionCompatibilityResult.compatible();
                } else {
                    logger.debug("Found incompatible function pair: {} with {}", scale.getFromFunction(), funcName);
                    return FunctionCompatibilityResult.incompatible(funcName);
                }
            }
            
            // Recursively check function arguments
            for (Expression arg : func.getArguments()) {
                FunctionCompatibilityResult result = findAndValidateToFunction(arg, scale);
                if (result.isCompatible() || result.hasIncompatibleFunction()) {
                    return result;
                }
            }
        } else if (expr instanceof ArithmeticBinaryExpression) {
            ArithmeticBinaryExpression arith = (ArithmeticBinaryExpression) expr;
            
            FunctionCompatibilityResult leftResult = findAndValidateToFunction(arith.getLeft(), scale);
            if (leftResult.isCompatible() || leftResult.hasIncompatibleFunction()) {
                return leftResult;
            }
            
            return findAndValidateToFunction(arith.getRight(), scale);
        } else if (expr instanceof Cast) {
            Cast cast = (Cast) expr;
            return findAndValidateToFunction(cast.getExpression(), scale);
        }
        
        return FunctionCompatibilityResult.notFound();
    }
    
    // ===== UTILITY METHODS =====
    
    /**
     * Recursively searches function arguments for temporal patterns.
     * Finds ALL temporal granularities in ALL arguments and returns the finest.
     * This handles cases where temporal patterns might be in multiple arguments.
     * 
     * @param func The function call to analyze
     * @return The finest granularity found across all arguments
     */
    private TimeGranularity searchNestedArguments(FunctionCall func) {
        List<Expression> arguments = func.getArguments();
        
        // Collect all granularities found in all arguments
        TimeGranularity[] granularities = new TimeGranularity[arguments.size()];
        int index = 0;
        
        for (Expression arg : arguments) {
            TimeGranularity granularity = extractGranularity(arg);
            if (granularity != TimeGranularity.NONE && granularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in argument {} of function '{}'", 
                           granularity, index, func.getName());
                granularities[index] = granularity;
            }
            index++;
        }
        
        // Return the finest granularity found across all arguments
        TimeGranularity finestGranularity = getFinestGranularity(granularities);
        if (finestGranularity != TimeGranularity.NONE) {
            logger.debug("Finest temporal granularity from function '{}' arguments: {}", func.getName(), finestGranularity);
        } else {
            logger.debug("No temporal granularity found in function '{}' arguments", func.getName());
        }
        
        return finestGranularity;
    }
    
    /**
     * Returns the finest (most precise) granularity from the given options.
     * NONE is treated as "no temporal requirement" and any actual granularity takes precedence.
     * 
     * @param granularities Variable number of granularities to compare
     * @return The finest granularity found, or NONE if no temporal requirements found
     */
    private TimeGranularity getFinestGranularity(TimeGranularity... granularities) {
        TimeGranularity finest = TimeGranularity.NONE;
        
        for (TimeGranularity granularity : granularities) {
            if (granularity == null || granularity.equals(TimeGranularity.UNKNOWN)) {
                continue; // Skip null and UNKNOWN
            }
            
            if (granularity.equals(TimeGranularity.NONE)) {
                continue; // Skip NONE - it represents no temporal requirement
            }
            
            // If we haven't found any temporal requirement yet, this becomes the finest
            if (finest.equals(TimeGranularity.NONE)) {
                finest = granularity;
            } else if (granularity.isFinnerThan(finest)) {
                // Replace with finer granularity
                finest = granularity;
            }
        }
        
        return finest;
    }
    
    /**
     * Returns the coarsest (least precise) granularity from the given options.
     * This is used when format constraints limit precision - the format acts as a ceiling.
     * NONE is treated as "no temporal constraint" and any actual granularity takes precedence.
     * 
     * @param granularities Variable number of granularities to compare
     * @return The coarsest granularity found, or NONE if no temporal constraints found
     */
    private TimeGranularity getCoarserGranularity(TimeGranularity... granularities) {
        TimeGranularity coarsest = TimeGranularity.NONE;
        
        for (TimeGranularity granularity : granularities) {
            if (granularity == null || granularity.equals(TimeGranularity.UNKNOWN)) {
                continue; // Skip null and UNKNOWN
            }
            
            if (granularity.equals(TimeGranularity.NONE)) {
                continue; // Skip NONE - it represents no temporal constraint
            }
            
            // If we haven't found any temporal constraint yet, this becomes the coarsest
            if (coarsest.equals(TimeGranularity.NONE)) {
                coarsest = granularity;
            } else if (granularity.isCoarserThan(coarsest)) {
                // Replace with coarser granularity
                coarsest = granularity;
            }
        }
        
        return coarsest;
    }
}