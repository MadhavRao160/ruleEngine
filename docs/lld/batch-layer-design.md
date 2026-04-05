# Low-Level Design (LLD): Spark Batch Processing Layer

**Component:** Financial Reconciliation & State Correction Engine  
**Compute:** Apache Spark (PySpark) on AWS EMR / Glue / Databricks  
**Storage:** Amazon S3 (Medallion Architecture)  
**Table Format:** Apache Iceberg  
**Orchestration:** Apache Airflow (MWAA)

---

## 1. Architectural Philosophy: The "Financial Truth" Engine

The Batch Layer is the system's "Ground Truth." While the Speed Layer is optimized for **Latency** (sub-50ms provisional decisions), the Batch Layer is optimized for **Integrity** (high-throughput final audits).

### Core Responsibilities:
* **Audit & Reconciliation:** Matching real-time "Provisional Holds" (from Kafka) against "Final Settlements" (from Bank CSVs).
* **Zero Logic Drift:** Utilizing the exact same **JSON AST** policies as the Speed Layer, compiled into native Spark expressions.
* **State Correction (The True-Up):** Identifying "State Drift"—discrepancies between holds and settlements—and pushing corrections back to the Speed Layer’s DynamoDB ledger.
* **Compliance & Reporting:** Providing an immutable, time-travel-enabled dataset for Finance and SOC2 audits.

---

## 2. Medallion Data Lifecycle (S3 + Apache Iceberg)

We utilize a strictly zoned architecture to transform raw ingest into high-integrity financial aggregates.

### 2.1 Bronze Layer (Raw & Immutable)
* **API Decision Archive:** S3-buffered Kafka events containing every decision made by the FastAPI workers.
* **Bank Settlement Files:** Raw 3–4TB CSV/ISO-20022 files ingested from partner banks via the "Claim Check" pattern.
* **Constraint:** Data is append-only and immutable. It serves as the system’s "Flight Data Recorder."

### 2.2 Silver Layer (Standardized & Matched)
* **Schema Enforcement:** Spark standardizes messy, multi-source bank data into a unified schema.
* **The Heavy Join:** Spark joins the API Decision records with the Bank Settlement records using a **Left Outer Join** to capture "Orphaned Decisions" (holds that haven't yet settled).
* **Match Keys:** A composite key consisting of `Network_TX_ID`, `Employee_ID`, and `Normalized_Amount`.

### 2.3 Gold Layer (The Reconciled Truth)
* **Logic Application:** Spark executes the **AST Evaluator** over the matched records.
* **Delta Calculation:** Spark calculates the `True_Up_Delta`.
    * *Scenario:* If a $200 hotel hold was settled for $150, the Delta is +$50.
* **Audit Snapshot:** The final, reconciled record is saved as an Iceberg table, ready for Athena querying.

---

## 3. The AST Compiler (Native Spark Expressions)

To process terabytes of data, the engine avoids row-by-row Python `map` operations or UDFs, which incur high serialization overhead.

* **Compiler Logic:** The Spark Driver reads the JSON AST and recursively translates logical nodes into **Native Spark SQL Expressions** (`pyspark.sql.functions`).
* **Vectorized Execution:** By chaining `F.when().otherwise()` expressions, the logic runs inside Spark’s **Tungsten** engine. This ensures the evaluation stays within JVM memory, achieving maximum throughput.
* **Parity:** Because the compiler uses the same JSON source as the Speed Layer, we guarantee 100% logic parity between a card swipe and a nightly audit.

---

## 4. The "True-Up" Loop (Reverse ETL to DynamoDB)

This is the system's "Self-Healing" mechanism. Once Spark identifies a discrepancy, it must correct the Speed Layer’s "Exposure Cache."

### The Sync Workflow:
1.  **Detection:** Spark filters for records where `Settled_Amount != Provisional_Amount`.
2.  **Aggregation:** Spark groups deltas by `Employee_ID` to minimize the number of write IOPS to DynamoDB.
3.  **Atomic Correction:** Using the **Spark-to-DynamoDB Connector**, the job issues an `UpdateItem` request with the `ADD` operation.
4.  **Result:** The employee’s real-time spending power is restored (or deducted) to match the bank's finalized clearing.

---

## 5. Handling Data Skew & Scalability

Processing global corporate spend involves massive data skews (e.g., Black Friday spikes).

* **Adaptive Query Execution (AQE):** Enabled via `spark.sql.adaptive.enabled`. Spark dynamically re-partitions data and optimizes join strategies (e.g., switching from Sort-Merge to Broadcast Hash) based on runtime statistics.
* **Salting Technique:** To prevent "Hot Partitions" when aggregating by `Department_ID`, a random "salt" column is added. This distributes the aggregation load across all executors before a final global merge, preventing Out-Of-Memory (OOM) errors.

---

## 6. Auditability & Time-Travel

By utilizing **Apache Iceberg**, the system provides features essential for financial compliance:

* **Snapshot Isolation:** Users can query the Gold layer as it existed at any point in time (e.g., `FOR SYSTEM_TIME AS OF '2026-04-01'`).
* **ACID Transactions:** Ensures that Airflow pipeline retries do not result in duplicate records or partial writes.
* **Amazon Athena Integration:** Finance teams can run standard SQL queries for monthly tax reporting and spend-trend analysis without touching the production compute environment.