# SQL Rewriter POC

## 🚀 Overview

This project is a Proof of Concept (POC) for a **Simple SQL Query Rewriter**. The primary goal is to demonstrate how certain user queries can be automatically rewritten to leverage pre-calculated **Materialized Views (MVs)** instead of querying the original base tables. This can significantly improve query performance, especially in analytical workloads.

The core functionality involves:

1.  **Parsing** incoming SQL queries and pre-defined MV definitions.
2.  **Analyzing** both the user query and MVs to understand their structure, including base tables, projections, filters, and aggregations.
3.  **Matching** user queries against available MVs to determine if an MV can satisfy the query's requirements.
4.  **Rewriting** the user query's Abstract Syntax Tree (AST) to substitute the base table with the appropriate MV target table when a match is found.

This POC uses the **Trino SQL Parser** for robust SQL parsing and AST manipulation.

## ⚙️ How It Works

The rewriting process follows these steps:

1.  **Initialization**:
    * Loads configuration (`config.yaml`) containing base table schemas and MV definitions.
    * Parses each MV definition, extracts its metadata (base table, columns, aggregations, etc.) using `QueryMetadataExtractor`, and stores it. It validates that MVs do not use `*`.

2.  **User Query Processing**:
    * Parses the incoming user SQL query into an AST.
    * Extracts initial metadata from the user query.

3.  **Candidate Selection**:
    * Uses `RewriteMatcher` to compare the *refined* user query metadata against all loaded MV metadata.
    * The `RewriteMatcher` checks if an MV:
        * Is based on the same table.
        * Provides all the necessary columns (for projections and filters).
        * Provides all required aggregations.
        * Has compatible `GROUP BY` clauses.

4.  **Rewriting**:
    * If a suitable MV candidate is found, `TableReplacerVisitor` traverses the user query's AST.
    * It replaces occurrences of the original base table name with the MV's target table name.
    * The modified AST is formatted back into a SQL string.

5.  **Output**:
    * If a rewrite occurred, the new SQL string is returned.
    * If no suitable MV was found, or if the query isn't a candidate for rewriting (e.g., not a SELECT, or requires `*` from base), the original SQL string is returned.

## 🧩 Key Components

* **`SQLRewriter.java`**: The main orchestrator class.
* **`config/`**: Classes for loading and holding configuration data (`PocConfig`, `MaterializedViewDefinition`, `TableDefinition`).
* **`model/`**: Data classes representing query metadata (`QueryMetadata`, `AggregationInfo`).
* **`analysis/`**:
    * `QueryMetadataExtractor.java`: Parses SQL and extracts high-level metadata.
* **`rewriting/`**:
    * `RewriteMatcher.java`: Decides if an MV can satisfy a query.
    * `TableReplacerVisitor.java`: Modifies the SQL AST to perform the table swap.

## 🛠️ Configuration (`config.yaml`)

The `config.yaml` file is crucial for defining the environment. It specifies the schemas of the base tables and the definitions (SQL and target table) for all available Materialized Views that the rewriter can consider.

## ⚠️ Assumptions & Limitations

This is a POC and has several simplifying assumptions:

* **Single Base Table**: Focuses primarily on queries and MVs built upon a single base table. Join handling is basic and might prevent rewrites.
* **Simple Substitution**: Only performs table name replacement. It doesn't attempt complex query restructuring.
* **No `*` in MVs**: MVs *must* specify their columns explicitly; `SELECT *` is not allowed in MV definitions.
* **No `*` from Base**: Queries that ultimately require *all* columns (`*`) from a base table *cannot* be rewritten.
* **Filter/Aggregation Coverage**: MVs must contain *at least* the columns/aggregations/groupings needed by the query. It doesn't (yet) support rolling up aggregations or deriving columns if they aren't directly present.
* **Limited Filter Pushdown Logic**: It primarily checks if the *columns* used in filters are present; it doesn't analyze if the MV's filters are compatible or sufficient.

## 🧪 How to Run Tests

The project includes JUnit 5 tests that process SQL files from specific directories.

1.  **Test Structure**: Test cases are located under `src/test/resources/test_cases/`. Each test case is a directory containing:
    * `query.sql`: The original user query.
    * `expected.sql`: The expected SQL output after processing (either rewritten or original).
2.  **Running Tests**: Use a build tool like Maven or Gradle:
    * **Maven**: `mvn test`
    * **Gradle**: `gradle test`
3.  **Filtering Tests**: You can run a specific test case by setting the `testFilter` system property. For example, to run only tests whose directory name contains "aggregation":
    * `mvn test -DtestFilter=aggregation`
4.  **Logging**: The tests use `logback.xml` for logging, providing detailed (DEBUG level) output during processing, which is helpful for understanding the rewriting decisions.