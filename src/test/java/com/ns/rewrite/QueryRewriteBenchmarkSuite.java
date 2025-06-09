package com.ns.rewrite;

import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ns.rewrite.analysis.QueryMetadataExtractor;
import com.ns.rewrite.config.TableConfig;
import com.ns.rewrite.model.QueryMetadata;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class QueryRewriteBenchmarkSuite {

    private static final Logger logger = LoggerFactory.getLogger(QueryRewriteBenchmarkSuite.class);
    private static final String TEST_CASES_BASE_RESOURCE_DIR = "test_cases";
    private static final int BENCHMARK_RUNS = 5; // Number of runs to average
    
    private SQLRewriter sqlRewriter;
    private SqlParser sqlParser;

    @BeforeAll
    void initializeBenchmark() {
        logger.info("Initializing benchmark suite...");
        TableConfig config = SQLRewriter.loadConfig("config.yaml");
        sqlRewriter = new SQLRewriter(config);
        sqlParser = new SqlParser();
        logger.info("Benchmark suite initialized.");
    }

    @Test
    void benchmarkAllTestCases() throws IOException, URISyntaxException {
        logger.info("Starting comprehensive benchmark across all test cases...");
        
        List<TestCaseResult> results = new ArrayList<>();
        List<TestCase> testCases = loadTestCases();
        
        logger.info("Found {} test cases to benchmark", testCases.size());
        
        for (TestCase testCase : testCases) {
            logger.info("Benchmarking test case: {} ({} runs)", testCase.getName(), BENCHMARK_RUNS);
            TestCaseResult avgResult = benchmarkTestCase(testCase, BENCHMARK_RUNS);
            results.add(avgResult);
            logger.info("Completed benchmark for: {} (avg end-to-end: {}ms)", 
                       testCase.getName(), String.format("%.3f", avgResult.getEndToEndMs()));
        }
        
        // Export results to CSV
        exportToCsv(results);
        logSummaryStatistics(results);
        
        logger.info("Benchmark suite completed successfully!");
    }

    private TestCaseResult benchmarkTestCase(TestCase testCase, int runs) {
        List<Map<String, Double>> allTimings = new ArrayList<>();
        
        for (int i = 0; i < runs; i++) {
            BenchmarkTimer.reset();
            try {
                runSingleBenchmark(testCase);
                allTimings.add(BenchmarkTimer.getTimings());
            } catch (Exception e) {
                logger.warn("Error in benchmark run {} for test case {}: {}", 
                           i + 1, testCase.getName(), e.getMessage());
            }
        }
        
        return calculateAverage(testCase, allTimings);
    }

    private void runSingleBenchmark(TestCase testCase) {
        String sql = testCase.getQuerySql();
        
        // Phase A: SQL Parsing
        Statement stmt = BenchmarkTimer.timeAndLog("sql_parsing", 
            () -> sqlParser.createStatement(sql));
        
        // Phase B: Metadata Extraction  
        BenchmarkTimer.timeAndLog("metadata_extraction",
            () -> QueryMetadataExtractor.extractMetadataFromQuery(stmt));
        
        // Phase C: Full Query Rewriting (includes MV matching + AST rewriting)
        BenchmarkTimer.timeAndLog("full_rewriting",
            () -> sqlRewriter.processUserQuery(sql));
        
        // Phase D: No-op Baseline (just parse + format)
        BenchmarkTimer.timeAndLog("noop_baseline", 
            () -> SqlFormatter.formatSql(stmt));
        
        // Calculate end-to-end (sum of parsing + metadata + rewriting)
        double parsing = BenchmarkTimer.getTiming("sql_parsing");
        double metadata = BenchmarkTimer.getTiming("metadata_extraction");
        double rewriting = BenchmarkTimer.getTiming("full_rewriting");
        double endToEnd = parsing + metadata + rewriting;
        
        // Debug logging
        logger.debug("Timing breakdown - Parsing: {}ms, Metadata: {}ms, Rewriting: {}ms, End-to-end: {}ms", 
                    String.format("%.3f", parsing), String.format("%.3f", metadata), 
                    String.format("%.3f", rewriting), String.format("%.3f", endToEnd));
        
        // Store the calculated end-to-end time in the original map
        BenchmarkTimer.storeTiming("end_to_end", endToEnd);
    }

    private TestCaseResult calculateAverage(TestCase testCase, List<Map<String, Double>> allTimings) {
        if (allTimings.isEmpty()) {
            logger.warn("No successful timing runs for test case: {}", testCase.getName());
            return new TestCaseResult(testCase.getName(), 0, 0, 0, 0, 0);
        }
        
        Map<String, Double> averages = new HashMap<>();
        Set<String> operations = allTimings.get(0).keySet();
        
        logger.debug("Calculating averages for test case: {} with operations: {}", testCase.getName(), operations);
        
        for (String operation : operations) {
            double avg = allTimings.stream()
                .mapToDouble(timings -> timings.getOrDefault(operation, 0.0))
                .average()
                .orElse(0.0);
            averages.put(operation, avg);
            logger.debug("Operation '{}' average: {}ms", operation, String.format("%.3f", avg));
        }
        
        return new TestCaseResult(
            testCase.getName(),
            averages.getOrDefault("sql_parsing", 0.0),
            averages.getOrDefault("metadata_extraction", 0.0),
            averages.getOrDefault("full_rewriting", 0.0),
            averages.getOrDefault("end_to_end", 0.0),
            averages.getOrDefault("noop_baseline", 0.0)
        );
    }

    private List<TestCase> loadTestCases() throws IOException, URISyntaxException {
        URL resourceUrl = getClass().getClassLoader().getResource(TEST_CASES_BASE_RESOURCE_DIR);
        Path baseDirPath;

        if (resourceUrl == null) {
            Path directPath = Paths.get("src", "test", "resources", TEST_CASES_BASE_RESOURCE_DIR);
            if (Files.exists(directPath) && Files.isDirectory(directPath)) {
                baseDirPath = directPath;
            } else {
                throw new RuntimeException("Test cases directory not found: " + TEST_CASES_BASE_RESOURCE_DIR);
            }
        } else {
            baseDirPath = Paths.get(resourceUrl.toURI());
        }

        List<TestCase> testCases = new ArrayList<>();
        try (Stream<Path> dirs = Files.list(baseDirPath).filter(Files::isDirectory)) {
            dirs.forEach(testCaseDir -> {
                String name = testCaseDir.getFileName().toString();
                Path queryFile = testCaseDir.resolve("query.sql");
                if (Files.exists(queryFile)) {
                    try {
                        String sql = new String(Files.readAllBytes(queryFile), StandardCharsets.UTF_8)
                                .replace("\uFEFF", "").trim();
                        if (!sql.isEmpty()) {
                            testCases.add(new TestCase(name, sql));
                        }
                    } catch (IOException e) {
                        logger.warn("Failed to read query file for test case: {}", name);
                    }
                }
            });
        }
        
        testCases.sort(Comparator.comparing(TestCase::getName));
        return testCases;
    }

    private void exportToCsv(List<TestCaseResult> results) {
        try {
            Path outputFile = Paths.get("benchmark_results.csv");
            try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8))) {
                // CSV Header
                writer.println("test_case,sql_parsing_ms,metadata_extraction_ms,full_rewriting_ms,end_to_end_ms,noop_baseline_ms");
                
                // CSV Data
                for (TestCaseResult result : results) {
                    writer.printf("%s,%.3f,%.3f,%.3f,%.3f,%.3f%n",
                        result.getTestCaseName(),
                        result.getSqlParsingMs(),
                        result.getMetadataExtractionMs(),
                        result.getFullRewritingMs(),
                        result.getEndToEndMs(),
                        result.getNoopBaselineMs()
                    );
                }
            }
            logger.info("Benchmark results exported to: {}", outputFile.toAbsolutePath());
        } catch (IOException e) {
            logger.error("Failed to export CSV results: {}", e.getMessage());
        }
    }

    private void logSummaryStatistics(List<TestCaseResult> results) {
        if (results.isEmpty()) return;
        
        DoubleSummaryStatistics endToEndStats = results.stream()
            .mapToDouble(TestCaseResult::getEndToEndMs)
            .summaryStatistics();
            
        DoubleSummaryStatistics metadataStats = results.stream()
            .mapToDouble(TestCaseResult::getMetadataExtractionMs)
            .summaryStatistics();
        
        logger.info("=== BENCHMARK SUMMARY ===");
        logger.info("Total test cases: {}", results.size());
        logger.info("End-to-end timing - Min: {}ms, Max: {}ms, Avg: {}ms", 
                   String.format("%.3f", endToEndStats.getMin()), 
                   String.format("%.3f", endToEndStats.getMax()), 
                   String.format("%.3f", endToEndStats.getAverage()));
        logger.info("Metadata extraction - Min: {}ms, Max: {}ms, Avg: {}ms", 
                   String.format("%.3f", metadataStats.getMin()),
                   String.format("%.3f", metadataStats.getMax()),
                   String.format("%.3f", metadataStats.getAverage()));
        
        // Find slowest test cases
        TestCaseResult slowest = results.stream()
            .max(Comparator.comparing(TestCaseResult::getEndToEndMs))
            .orElse(null);
        if (slowest != null) {
            logger.info("Slowest test case: {} ({}ms)", 
                       slowest.getTestCaseName(), String.format("%.3f", slowest.getEndToEndMs()));
        }
    }

    // Helper classes
    private static class TestCase {
        private final String name;
        private final String querySql;

        public TestCase(String name, String querySql) {
            this.name = name;
            this.querySql = querySql;
        }

        public String getName() { return name; }
        public String getQuerySql() { return querySql; }
    }

    private static class TestCaseResult {
        private final String testCaseName;
        private final double sqlParsingMs;
        private final double metadataExtractionMs;
        private final double fullRewritingMs;
        private final double endToEndMs;
        private final double noopBaselineMs;

        public TestCaseResult(String testCaseName, double sqlParsingMs, double metadataExtractionMs, 
                             double fullRewritingMs, double endToEndMs, double noopBaselineMs) {
            this.testCaseName = testCaseName;
            this.sqlParsingMs = sqlParsingMs;
            this.metadataExtractionMs = metadataExtractionMs;
            this.fullRewritingMs = fullRewritingMs;
            this.endToEndMs = endToEndMs;
            this.noopBaselineMs = noopBaselineMs;
        }

        public String getTestCaseName() { return testCaseName; }
        public double getSqlParsingMs() { return sqlParsingMs; }
        public double getMetadataExtractionMs() { return metadataExtractionMs; }
        public double getFullRewritingMs() { return fullRewritingMs; }
        public double getEndToEndMs() { return endToEndMs; }
        public double getNoopBaselineMs() { return noopBaselineMs; }
    }
}