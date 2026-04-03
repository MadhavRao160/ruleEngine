# Low-Level Design (LLD): Spark Batch Processing Layer

## 1. Architectural Philosophy
The Spark Batch Layer operates on a **Config-Driven (DSL) Architecture**. To achieve both "Zero Logic Drift" and maximum big-data performance, the engine abandons row-by-row Python evaluation (`mapPartitions`). Instead, it translates shared JSON policy rules into **Native PySpark SQL Expressions**, leveraging the Catalyst Optimizer and Tungsten Execution Engine for raw speed.

---

## 2. The Policy DSL (Abstract Syntax Tree)
Business rules are stored in DynamoDB as a JSON-based Domain Specific Language (DSL). This allows both the FastAPI speed layer and the Spark batch layer to read from a Single Source of Truth.

* **Structure:** Rules are defined as recursive JSON objects (Abstract Syntax Trees) supporting logical operators (`AND`, `OR`), target fields, and condition thresholds.
* **Separation of Concerns:** The DSL focuses strictly on evaluation. It assumes that all necessary context (e.g., employee department, rolling totals) has already been hydrated into the dataset prior to evaluation.

---

## 3. The Native PySpark Compiler (AST Translator)
This component runs entirely on the **Spark Driver** before distributed data processing begins. It acts as a compiler, parsing the JSON DSL and generating a highly optimized execution plan.

* **Recursive Parsing:** A `RuleCompiler` service reads the JSON tree and recursively maps conditions to PySpark native column functions.
    * *Pseudocode:* Translates `{field: amount, operator: >, value: 100}` into `F.col("amount") > 100`.
* **Column Chaining:** A `BatchEvaluationService` dynamically chains these compiled expressions using `F.when().otherwise()`, writing the outcomes to an `evaluation_result` column without breaking the Catalyst execution plan.

---

## 4. Stateful Aggregations (Native Windowing)
The system calculates running totals (e.g., "Daily Caps") natively without relying on external databases or in-memory Python dictionaries, preventing `OutOfMemory` errors across the 4TB payload.

* **Pre-computation:** An Aggregation Service uses PySpark `Window.partitionBy()` (e.g., partitioned by `employee_id` and ordered by `transaction_timestamp`) to calculate and append running totals to the DataFrame *before* the DSL evaluates the row.
* **State Management:** Spark natively manages the state using Tungsten off-heap memory, gracefully spilling to local executor NVMe SSDs if a specific partition exceeds available RAM.

---

## 5. Data Skew Management
To handle uneven data distribution, the architecture employs a hybrid skew-mitigation strategy.

* **Join Skew:** Addressed natively by enabling Spark's Adaptive Query Execution (`spark.sql.adaptive.enabled = true`). AQE dynamically splits skewed partitions during upstream data hydration.
* **Aggregation Skew:** Addressed via a **Two-Pass Salting Pattern**. If a global window aggregation funnels massive amounts of data to a single node, a random `salt` column is temporarily added to distribute the initial sum across all executors, followed by a final global sum.

---

## 6. Medallion Storage & Idempotent Upserts
The physical storage layer guarantees strict financial data integrity, ensuring that Airflow pipeline retries never result in duplicate expenses.

* **Medallion Tiers:** * **Bronze:** Append-only raw Amex/Chase CSV dumps.
    * **Silver:** Cleaned, schema-enforced, and tokenized data.
    * **Gold:** The finalized, reconciled dataset containing `approved_amount` and `excess_amount`.
* **ACID Transactions:** The Gold table utilizes an open-table format (Delta Lake / Apache Iceberg). Writes are executed using the `MERGE INTO` pattern matched on `expense_id`. This guarantees idempotency—if a job crashes and restarts, existing records are safely updated rather than duplicated.