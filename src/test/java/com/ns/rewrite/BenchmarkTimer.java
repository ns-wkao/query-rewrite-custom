package com.ns.rewrite;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Simple utility class for timing operations during benchmarking.
 * Provides methods to wrap operations and collect timing data.
 */
public class BenchmarkTimer {
    private static Map<String, Double> timings = new LinkedHashMap<>();
    
    /**
     * Times a supplier operation and stores the result.
     * @param operation Name of the operation being timed
     * @param supplier The operation to time
     * @return The result of the supplier
     */
    public static <T> T timeAndLog(String operation, Supplier<T> supplier) {
        long start = System.nanoTime();
        T result = supplier.get();
        double duration = (System.nanoTime() - start) / 1_000_000.0; // Convert to milliseconds with precision
        timings.put(operation, duration);
        return result;
    }
    
    /**
     * Times a runnable operation (no return value).
     * @param operation Name of the operation being timed
     * @param runnable The operation to time
     */
    public static void timeAndLog(String operation, Runnable runnable) {
        timeAndLog(operation, () -> { 
            runnable.run(); 
            return null; 
        });
    }
    
    /**
     * Gets a copy of all recorded timings.
     * @return Map of operation names to timing in milliseconds
     */
    public static Map<String, Double> getTimings() { 
        return new HashMap<>(timings); 
    }
    
    /**
     * Clears all recorded timings.
     */
    public static void reset() { 
        timings.clear(); 
    }
    
    /**
     * Gets the timing for a specific operation.
     * @param operation The operation name
     * @return Timing in milliseconds, or -1 if not found
     */
    public static double getTiming(String operation) {
        return timings.getOrDefault(operation, -1.0);
    }
    
    /**
     * Calculates the sum of all recorded timings.
     * @return Total time in milliseconds
     */
    public static double getTotalTime() {
        return timings.values().stream().mapToDouble(Double::doubleValue).sum();
    }
    
    /**
     * Directly stores a timing value (for calculated metrics like end-to-end).
     * @param operation The operation name
     * @param timing The timing value in milliseconds
     */
    public static void storeTiming(String operation, double timing) {
        timings.put(operation, timing);
    }
}