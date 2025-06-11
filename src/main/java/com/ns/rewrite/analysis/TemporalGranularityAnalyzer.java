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
     * Handles function calls, arithmetic expressions, and nested temporal patterns.
     */
    public TimeGranularity extractGranularity(Expression expr) {
        logger.debug("=== TEMPORAL ANALYSIS START ===");
        logger.debug("Expression type: {}", expr.getClass().getSimpleName());
        logger.debug("Expression toString: {}", expr.toString());
        if (expr == null) {
            return TimeGranularity.UNKNOWN;
        }
        
        logger.debug("Analyzing expression for temporal granularity: {}", expr);
        
        if (expr instanceof FunctionCall) {
            logger.debug("Processing as FunctionCall");
            FunctionCall func = (FunctionCall) expr;
            return extractFromFunctionCall(func);
        }
        
        if (expr instanceof ArithmeticBinaryExpression) {
            // Handle arithmetic expressions that might contain temporal functions
            return extractFromArithmeticExpression((ArithmeticBinaryExpression) expr);
        }
        
        if (expr instanceof Cast) {
            // Handle CAST expressions that might wrap temporal functions
            return extractFromCastExpression((Cast) expr);
        }
        
        if (expr instanceof SearchedCaseExpression) {
            // Handle CASE expressions that contain temporal functions in WHEN/ELSE clauses
            logger.debug("Processing as SearchedCaseExpression");
            return extractFromSearchedCaseExpression((SearchedCaseExpression) expr);
        }
        
        if (expr instanceof SimpleCaseExpression) {
            // Handle simple CASE expressions (CASE column WHEN value THEN result)
            return extractFromSimpleCaseExpression((SimpleCaseExpression) expr);
        }
        
        if (expr instanceof IfExpression) {
            // Handle IF expressions that might contain temporal functions
            return extractFromIfExpression((IfExpression) expr);
        }
        
        if (expr instanceof ComparisonExpression) {
            // Handle comparison expressions that might contain temporal functions in operands
            logger.debug("Processing as ComparisonExpression");
            return extractFromComparisonExpression((ComparisonExpression) expr);
        }
        
        // Future: Could extend to handle other expression types
        logger.debug("Expression type '{}' not recognized for temporal analysis", expr.getClass().getSimpleName());
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Extracts granularity from arithmetic expressions that might contain temporal functions.
     */
    private TimeGranularity extractFromArithmeticExpression(ArithmeticBinaryExpression expr) {
        // Check both left and right operands for temporal patterns
        TimeGranularity leftGranularity = extractGranularity(expr.getLeft());
        if (leftGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in left operand of arithmetic expression", leftGranularity);
            return leftGranularity;
        }
        
        TimeGranularity rightGranularity = extractGranularity(expr.getRight());
        if (rightGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in right operand of arithmetic expression", rightGranularity);
            return rightGranularity;
        }
        
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Extracts granularity from CAST expressions that might wrap temporal functions.
     */
    private TimeGranularity extractFromCastExpression(Cast expr) {
        TimeGranularity granularity = extractGranularity(expr.getExpression());
        if (granularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} within CAST expression", granularity);
        }
        return granularity;
    }
    
    /**
     * Extracts granularity from searched CASE expressions (CASE WHEN condition THEN result).
     * Returns the first temporal granularity found in any clause.
     */
    private TimeGranularity extractFromSearchedCaseExpression(SearchedCaseExpression expr) {
        logger.debug("Analyzing searched CASE expression with {} WHEN clauses", expr.getWhenClauses().size());
        
        // Check each WHEN clause for temporal patterns
        for (WhenClause whenClause : expr.getWhenClauses()) {
            // Check the condition (WHEN part)
            TimeGranularity conditionGranularity = extractGranularity(whenClause.getOperand());
            if (conditionGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in searched CASE WHEN condition", conditionGranularity);
                return conditionGranularity;
            }
            
            // Check the result (THEN part)
            TimeGranularity resultGranularity = extractGranularity(whenClause.getResult());
            if (resultGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in searched CASE WHEN result", resultGranularity);
                return resultGranularity;
            }
        }
        
        // Check the ELSE clause if present
        if (expr.getDefaultValue().isPresent()) {
            TimeGranularity elseGranularity = extractGranularity(expr.getDefaultValue().get());
            if (elseGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in searched CASE ELSE clause", elseGranularity);
                return elseGranularity;
            }
        }
        
        logger.debug("No temporal granularity found in searched CASE expression");
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Extracts granularity from simple CASE expressions (CASE column WHEN value THEN result).
     */
    private TimeGranularity extractFromSimpleCaseExpression(SimpleCaseExpression expr) {
        logger.debug("Analyzing simple CASE expression with {} WHEN clauses", expr.getWhenClauses().size());
        
        // Check the operand (the expression being tested)
        TimeGranularity operandGranularity = extractGranularity(expr.getOperand());
        if (operandGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in simple CASE operand", operandGranularity);
            return operandGranularity;
        }
        
        // Check each WHEN clause for temporal patterns
        for (WhenClause whenClause : expr.getWhenClauses()) {
            // Check the condition (WHEN part)
            TimeGranularity conditionGranularity = extractGranularity(whenClause.getOperand());
            if (conditionGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in simple CASE WHEN condition", conditionGranularity);
                return conditionGranularity;
            }
            
            // Check the result (THEN part)
            TimeGranularity resultGranularity = extractGranularity(whenClause.getResult());
            if (resultGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in simple CASE WHEN result", resultGranularity);
                return resultGranularity;
            }
        }
        
        // Check the ELSE clause if present
        if (expr.getDefaultValue().isPresent()) {
            TimeGranularity elseGranularity = extractGranularity(expr.getDefaultValue().get());
            if (elseGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in simple CASE ELSE clause", elseGranularity);
                return elseGranularity;
            }
        }
        
        logger.debug("No temporal granularity found in simple CASE expression");
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Extracts granularity from IF expressions by analyzing condition and result branches.
     */
    private TimeGranularity extractFromIfExpression(IfExpression expr) {
        // Check the condition
        TimeGranularity conditionGranularity = extractGranularity(expr.getCondition());
        if (conditionGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in IF condition", conditionGranularity);
            return conditionGranularity;
        }
        
        // Check the true result
        TimeGranularity trueResultGranularity = extractGranularity(expr.getTrueValue());
        if (trueResultGranularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} in IF true branch", trueResultGranularity);
            return trueResultGranularity;
        }
        
        // Check the false result if present
        if (expr.getFalseValue().isPresent()) {
            TimeGranularity falseResultGranularity = extractGranularity(expr.getFalseValue().get());
            if (falseResultGranularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in IF false branch", falseResultGranularity);
                return falseResultGranularity;
            }
        }
        
        logger.debug("No temporal granularity found in IF expression");
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Extracts granularity from comparison expressions by analyzing both operands.
     * This handles cases like: DATE_TRUNC('month', ...) = DATE_TRUNC('month', ...)
     */
    private TimeGranularity extractFromComparisonExpression(ComparisonExpression expr) {
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
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Detects semantic temporal patterns that represent higher-level granularities than their literal components.
     * For example: DATE_TRUNC('DAY', DATE_ADD('day', week_offset, timestamp)) represents WEEK granularity.
     */
    private TimeGranularity detectSemanticTemporalPattern(FunctionCall func) {
        String functionName = func.getName().toString().toLowerCase(Locale.ROOT);
        
        // Pattern: DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', week_offset, timestamp)), '%Y-%m-%d')
        // This represents weekly aggregation
        if ("date_format".equals(functionName)) {
            TimeGranularity weekPattern = detectWeekStartPattern(func);
            if (weekPattern != TimeGranularity.UNKNOWN) {
                return weekPattern;
            }
        }
        
        // Pattern: DATE_TRUNC('DAY', DATE_ADD('day', week_offset, timestamp))
        // This also represents weekly aggregation when used with week offset calculations
        if ("date_trunc".equals(functionName)) {
            TimeGranularity weekPattern = detectDateTruncWeekPattern(func);
            if (weekPattern != TimeGranularity.UNKNOWN) {
                return weekPattern;
            }
        }
        
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Detects week-start patterns in DATE_FORMAT functions.
     * Pattern: DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', week_calculation, timestamp)), '%Y-%m-%d')
     */
    private TimeGranularity detectWeekStartPattern(FunctionCall dateFormatFunc) {
        List<Expression> args = dateFormatFunc.getArguments();
        if (args.isEmpty()) {
            return TimeGranularity.UNKNOWN;
        }
        
        Expression firstArg = args.get(0);
        if (!(firstArg instanceof FunctionCall)) {
            return TimeGranularity.UNKNOWN;
        }
        
        FunctionCall innerFunc = (FunctionCall) firstArg;
        return detectDateTruncWeekPattern(innerFunc);
    }
    
    /**
     * Detects week-start patterns in DATE_TRUNC functions.
     * Pattern: DATE_TRUNC('DAY', DATE_ADD('day', week_calculation, timestamp))
     */
    private TimeGranularity detectDateTruncWeekPattern(FunctionCall dateTruncFunc) {
        if (!"date_trunc".equals(dateTruncFunc.getName().toString().toLowerCase())) {
            return TimeGranularity.UNKNOWN;
        }
        
        List<Expression> args = dateTruncFunc.getArguments();
        if (args.size() < 2) {
            return TimeGranularity.UNKNOWN;
        }
        
        // Check if the unit is 'DAY'
        Expression unitExpr = args.get(0);
        if (!(unitExpr instanceof StringLiteral)) {
            return TimeGranularity.UNKNOWN;
        }
        
        String unit = ((StringLiteral) unitExpr).getValue().toLowerCase();
        if (!"day".equals(unit)) {
            return TimeGranularity.UNKNOWN;
        }
        
        // Check if the second argument is a DATE_ADD with week calculation
        Expression secondArg = args.get(1);
        if (!(secondArg instanceof FunctionCall)) {
            return TimeGranularity.UNKNOWN;
        }
        
        FunctionCall innerFunc = (FunctionCall) secondArg;
        if (isWeekCalculationDateAdd(innerFunc)) {
            logger.debug("Detected semantic week-start pattern in DATE_TRUNC('DAY', DATE_ADD(...))");
            return TimeGranularity.WEEK;
        }
        
        return TimeGranularity.UNKNOWN;
    }
    
    /**
     * Checks if a DATE_ADD function contains week boundary calculation logic.
     * Pattern: DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(...) % 7) - 1 + 7, 7)), timestamp)
     */
    private boolean isWeekCalculationDateAdd(FunctionCall dateAddFunc) {
        String funcName = dateAddFunc.getName().toString().toLowerCase();
        if (!"date_add".equals(funcName) && !"dateadd".equals(funcName)) {
            return false;
        }
        
        List<Expression> args = dateAddFunc.getArguments();
        if (args.size() < 3) {
            return false;
        }
        
        // Check if first argument is 'day'
        Expression unitExpr = args.get(0);
        if (!(unitExpr instanceof StringLiteral)) {
            return false;
        }
        
        String unit = ((StringLiteral) unitExpr).getValue().toLowerCase();
        if (!"day".equals(unit)) {
            return false;
        }
        
        // Check if the interval expression contains week calculation patterns
        Expression intervalExpr = args.get(1);
        return containsWeekCalculationLogic(intervalExpr);
    }
    
    /**
     * Recursively checks if an expression contains week calculation logic.
     * Looks for DAY_OF_WEEK functions and MOD operations with 7.
     */
    private boolean containsWeekCalculationLogic(Expression expr) {
        if (expr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) expr;
            String funcName = func.getName().toString().toLowerCase();
            
            // Check for DAY_OF_WEEK function
            if ("day_of_week".equals(funcName)) {
                logger.debug("Found DAY_OF_WEEK function - week calculation detected");
                return true;
            }
            
            // Check for MOD function with value 7
            if ("mod".equals(funcName)) {
                List<Expression> args = func.getArguments();
                if (args.size() >= 2) {
                    Expression secondArg = args.get(1);
                    if (secondArg instanceof LongLiteral) {
                        long value = Long.parseLong(((LongLiteral) secondArg).getValue());
                        if (value == 7) {
                            logger.debug("Found MOD(..., 7) - week calculation detected");
                            return true;
                        }
                    }
                }
            }
            
            // Recursively check function arguments
            for (Expression arg : func.getArguments()) {
                if (containsWeekCalculationLogic(arg)) {
                    return true;
                }
            }
        } else if (expr instanceof ArithmeticBinaryExpression) {
            ArithmeticBinaryExpression arith = (ArithmeticBinaryExpression) expr;
            return containsWeekCalculationLogic(arith.getLeft()) || 
                   containsWeekCalculationLogic(arith.getRight());
        } else if (expr instanceof ArithmeticUnaryExpression) {
            ArithmeticUnaryExpression unary = (ArithmeticUnaryExpression) expr;
            return containsWeekCalculationLogic(unary.getValue());
        }
        
        return false;
    }
    
    /**
     * Extracts granularity from function calls, specifically looking for date_trunc and nested patterns.
     * Also detects semantic temporal patterns that use DATE_TRUNC as a building block.
     */
    private TimeGranularity extractFromFunctionCall(FunctionCall func) {
        String functionName = func.getName().toString().toLowerCase(Locale.ROOT);
        
        // First, check for semantic temporal patterns that might override literal granularity
        TimeGranularity semanticGranularity = detectSemanticTemporalPattern(func);
        if (semanticGranularity != TimeGranularity.UNKNOWN) {
            return semanticGranularity;
        }
        
        if ("date_trunc".equals(functionName)) {
            return extractFromDateTruncFunction(func);
        }
        
        if ("date_format".equals(functionName)) {
            return extractFromDateFormatFunction(func);
        }
        
        if ("date_add".equals(functionName) || "dateadd".equals(functionName)) {
            return extractFromDateAddFunction(func);
        }
        
        // For other functions, recursively search their arguments for temporal patterns
        return extractFromNestedArguments(func);
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
     * Extracts granularity from DATE_FORMAT function calls that often wrap DATE_TRUNC.
     * Expected format: DATE_FORMAT(date_trunc('unit', timestamp), format_string)
     */
    private TimeGranularity extractFromDateFormatFunction(FunctionCall func) {
        List<Expression> arguments = func.getArguments();
        
        if (arguments.isEmpty()) {
            logger.debug("DATE_FORMAT function has no arguments");
            return TimeGranularity.UNKNOWN;
        }
        
        // Analyze the first argument (the date expression) for temporal granularity
        Expression dateExpression = arguments.get(0);
        TimeGranularity granularity = extractGranularity(dateExpression);
        
        if (granularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} within DATE_FORMAT function", granularity);
        }
        
        return granularity;
    }
    
    /**
     * Extracts granularity from DATE_ADD function calls that might contain DATE_TRUNC.
     * Expected format: DATE_ADD('unit', interval, date_trunc('unit', timestamp))
     */
    private TimeGranularity extractFromDateAddFunction(FunctionCall func) {
        List<Expression> arguments = func.getArguments();
        
        if (arguments.size() < 3) {
            logger.debug("DATE_ADD function has insufficient arguments: {}", arguments.size());
            return TimeGranularity.UNKNOWN;
        }
        
        // DATE_ADD typically has format: DATE_ADD(unit, interval, source_date)
        // We're interested in the source_date (3rd argument) which might contain DATE_TRUNC
        Expression sourceDateExpression = arguments.get(2);
        TimeGranularity granularity = extractGranularity(sourceDateExpression);
        
        if (granularity != TimeGranularity.UNKNOWN) {
            logger.debug("Found temporal granularity {} within DATE_ADD function", granularity);
        }
        
        return granularity;
    }
    
    /**
     * Recursively searches function arguments for temporal patterns.
     * This handles cases where DATE_TRUNC might be nested deep in complex expressions.
     */
    private TimeGranularity extractFromNestedArguments(FunctionCall func) {
        List<Expression> arguments = func.getArguments();
        
        for (Expression arg : arguments) {
            TimeGranularity granularity = extractGranularity(arg);
            if (granularity != TimeGranularity.UNKNOWN) {
                logger.debug("Found temporal granularity {} in nested argument of function '{}'", 
                           granularity, func.getName());
                return granularity;
            }
        }
        
        logger.debug("No temporal granularity found in function '{}' arguments", func.getName());
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