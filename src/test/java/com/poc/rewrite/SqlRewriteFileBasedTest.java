package com.poc.rewrite;

import com.poc.rewrite.config.PocConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments; // Required for custom arguments
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList; // Required for new list
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail; // Explicit import for fail
import static org.junit.jupiter.api.Assumptions.assumeTrue;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SqlRewriteFileBasedTest {

    private static final Logger logger = LoggerFactory.getLogger(SqlRewriteFileBasedTest.class);
    // Base directory within resources for all test case folders
    private static final String TEST_CASES_BASE_RESOURCE_DIR = "test_cases"; 

    private SQLRewritePoc pocInstance;

    @BeforeAll
    void initializePoc() {
        logger.info("Initializing SQLRewritePoc for tests...");
        PocConfig config = SQLRewritePoc.loadConfig("config.yaml");
        assertNotNull(config, "Failed to load PocConfig. Ensure config.yaml is in classpath.");
        pocInstance = new SQLRewritePoc(config);
        logger.info("SQLRewritePoc instance initialized.");
    }

    static Stream<Arguments> sqlFileProvider() throws IOException, URISyntaxException {
        URL resourceUrl = SqlRewriteFileBasedTest.class.getClassLoader().getResource(TEST_CASES_BASE_RESOURCE_DIR);
        Path baseDirPath;

        if (resourceUrl == null) {
            Path directPath = Paths.get("src", "test", "resources", TEST_CASES_BASE_RESOURCE_DIR);
            if(Files.exists(directPath) && Files.isDirectory(directPath)) {
                logger.warn("Test cases base resource directory not found directly in classpath. Using fallback path: {}", directPath.toAbsolutePath());
                baseDirPath = directPath;
            } else {
                logger.error("Test cases base resource directory not found in classpath or at fallback path: {}", TEST_CASES_BASE_RESOURCE_DIR);
                return Stream.empty();
            }
        } else {
             baseDirPath = Paths.get(resourceUrl.toURI());
             logger.info("Located test cases base directory for provider: {}", baseDirPath.toAbsolutePath());
        }

        List<Arguments> testArguments = new ArrayList<>();
        if (Files.isDirectory(baseDirPath)) {
            try (Stream<Path> testCaseDirs = Files.list(baseDirPath).filter(Files::isDirectory)) {
                testCaseDirs.forEach(testCaseDir -> {
                    Path querySqlFile = testCaseDir.resolve("query.sql");
                    Path expectedSqlFile = testCaseDir.resolve("expected.sql");
                    // Each test case directory must contain both query.sql and expected.sql
                    if (Files.exists(querySqlFile) && Files.isRegularFile(querySqlFile) &&
                        Files.exists(expectedSqlFile) && Files.isRegularFile(expectedSqlFile)) {
                        testArguments.add(Arguments.of(querySqlFile, testCaseDir.getFileName().toString()));
                    } else {
                        logger.warn("Skipping test case directory '{}': missing 'query.sql' and/or 'expected.sql'.", 
                                    testCaseDir.getFileName().toString());
                    }
                });
            }
        } else {
            logger.warn("Base directory for test cases is not a directory: {}", baseDirPath.toAbsolutePath());
        }
        
        if (testArguments.isEmpty()){
            logger.warn("No valid test cases (folders with query.sql and expected.sql) found in {}", baseDirPath.toAbsolutePath());
        }
        return testArguments.stream();
    }

    @DisplayName("SQL Rewrite Test Case:")
    @ParameterizedTest(name = "[{1}]") // Display test case name (directory name)
    @MethodSource("sqlFileProvider")
    void processSqlFile(Path querySqlFilePath, String testCaseName) {
    assertNotNull(pocInstance, "SQLRewritePoc instance was not initialized.");

    logger.info("--- Processing Test Case: {} ---", testCaseName);

    String originalSql = "";
    String rewrittenSql = "";
    String expectedSql = "";

    try {
        // Read original query
        originalSql = new String(Files.readAllBytes(querySqlFilePath), StandardCharsets.UTF_8)
                          .replace("\uFEFF", "").trim();
        logger.info("Original SQL from 'query.sql' for [{}]:\n{}", testCaseName, originalSql);

        if (originalSql.isEmpty()) {
            logger.warn("Input 'query.sql' in test case [{}] is empty. Test considered vacuously passed.", testCaseName);
            return;
        }

        // Perform rewrite
        rewrittenSql = pocInstance.processUserQuery(originalSql).trim();
        logger.info("Rewritten SQL for [{}]:\n{}", testCaseName, rewrittenSql);

        // Read expected query
        Path expectedSqlFilePath = querySqlFilePath.resolveSibling("expected.sql");
        expectedSql = new String(Files.readAllBytes(expectedSqlFilePath), StandardCharsets.UTF_8)
                                 .replace("\uFEFF", "").trim();
        logger.info("Expected SQL from 'expected.sql' for [{}]:\n{}", testCaseName, expectedSql);

        // Normalize SQL strings for comparison
        String normalizedRewrittenSql = normalizeSql(rewrittenSql);
        String normalizedExpectedSql = normalizeSql(expectedSql);

        // Compare
        assertEquals(normalizedExpectedSql, normalizedRewrittenSql,
                     "Rewritten SQL does not match expected.sql for test case: " + testCaseName);

    } catch (IOException e) {
        logger.error("Error reading files for test case [{}]: {}", testCaseName, e.getMessage(), e);
        fail("Failed to read files for test case: " + testCaseName, e);
    } catch (Exception e) {
        logger.error("Error processing query for test case [{}]: {}", testCaseName, e.getMessage(), e);
        fail("Failed to process query for test case: " + testCaseName, e);
    }
    logger.info("--- Finished Processing Test Case: {} ---", testCaseName);
}

// Helper method to normalize SQL
private String normalizeSql(String sql) {
    // Replace multiple whitespace characters with a single space
    String normalized = sql.replaceAll("\\s+", " ").trim();

    // Use regex to find and replace SQL keywords and functions case-insensitively
    String[] keywordsAndFunctions = {
        "select", "from", "where", "join", "on", "and", "or", "group by",
        "order by", "with", "as", "case", "when", "then", "else", "end",
        "distinct", "union", "all", "having", "limit", "offset", "insert",
        "into", "values", "update", "set", "delete", "timestamp", "split", "concat"
    };
    
    for (String item : keywordsAndFunctions) {
        normalized = normalized.replaceAll("(?i)\\b" + item + "\\b", item.toUpperCase());
    }

    // Normalize spacing around commas and parentheses
    normalized = normalized.replaceAll("\\s*,\\s*", ", ");
    normalized = normalized.replaceAll("\\s*\\(\\s*", " (").replaceAll("\\s*\\)\\s*", ") ");

    return normalized.trim();
}


}