# Expense Policy Rules Engine - Unified Architecture & HLD

## 👨‍💻 Author
#### Madhav Rao
#### Principal Architect | Distributed Data Systems

---

## 🎯 Project Objective
The **Expense Policy Rules Engine** is an enterprise-grade, distributed data platform designed to validate corporate travel and business expenses against a dynamic set of company policies. 

It solves three critical business challenges across two entirely different compute extremes:
1. **Real-Time Synchronous Validation:** Providing sub-20ms validations for individual employees submitting single-trip expenses via the mobile app.
2. **Bulk Partner Ingestion:** Processing massive 3-4TB daily data dumps from corporate credit card partners (e.g., Amex, Chase) to reconcile system-driven transactions.
3. **Historical Backfill Auditing:** Re-evaluating billions of historical records in the Data Lakehouse whenever corporate policies change to retroactively flag compliance violations.

To achieve this without duplicating business logic, the system utilizes a **Lambda Architecture** built around a **Shared Core** using strict **Domain-Driven Design (DDD)** and **Clean Architecture** principles.

---

## 🛠️ Technology Stack
* **Language:** Python 3.x
* **Web Framework:** FastAPI, Pydantic
* **Big Data Compute:** Apache Spark (PySpark), Databricks / AWS EMR
* **Event Streaming:** Apache Kafka
* **Cloud Infrastructure:** AWS (API Gateway, S3, DynamoDB)
* **Storage Format:** Apache Parquet, Delta Lake / Apache Iceberg
* **Orchestration:** Apache Airflow

---

## 1. System Overview & Architectural Patterns
This monorepo houses multiple execution environments that all import from a single, framework-agnostic core domain to process the "Corporate Spend Validation" Bounded Context.

* **Domain-Driven Design (DDD):** The system strictly models the business domain, utilizing the `Trip` as the Aggregate Root.
* **Config-Driven Architecture (The Shared Core):** To achieve "Zero Logic Drift," business rules are not hardcoded. They are defined as a Domain-Specific Language (DSL) stored as JSON Abstract Syntax Trees. Both the FastAPI web servers and the Spark Big Data clusters read from this exact same JSON source, acting as diverse execution engines for a unified policy set.
* **Extensibility:** Designed so adding new policy rules requires zero modifications to the core evaluation execution loop.

---

## 2. Entrypoints & The Intelligent Router (Ingestion)
To protect the real-time web servers from massive data warehousing workloads, traffic is split at the edge based on payload size and source intent.

* **Micro-Payloads (Mobile/Web Apps):** Standard HTTP requests route through the **AWS API Gateway** directly to the FastAPI Speed Layer.
* **Macro-Payloads (The Claim Check Pattern):** Corporate partners (e.g., Amex, Chase) sending 1TB+ CSV files cannot use the API Gateway. Instead:
  1. The partner requests an upload ticket via a lightweight FastAPI endpoint.
  2. FastAPI generates a temporary, secure **S3 Pre-Signed URL**.
  3. The partner uploads the massive file directly into the S3 Bronze bucket, bypassing the web servers entirely.

---

## 3. The Speed Layer (Real-Time Synchronous Path)
Handles synchronous, low-latency (`< 20ms`) requests for individual employees submitting expenses.

### 3.1 Component Flow
1. **Controller Layer (FastAPI):** Intercepts HTTP POST. Utilizes Pydantic schemas to strictly validate incoming JSON payloads.
2. **State Hydration:** Queries DynamoDB to fetch current trip totals and applicable department rules.
3. **Domain Evaluation:** Invokes a lightweight DSLEvaluator that parses the JSON rules dynamically against the hydrated Trip state to generate an EvaluationResult.
4. **Event Broadcasting (Apache Kafka):** Asynchronously publishes the evaluation event to a Kafka topic (`realtime-expense-audits`) for Lakehouse ingestion and continuous compliance monitoring.
5. **Response:** Translates the pure Python result back to JSON and returns HTTP 200.

### 3.2 API Contract
**Endpoint:** `POST /v1/trips/evaluate`

**Request Payload:**
```json
{
  "trip_id": "trip_123",
  "employee_id": "emp_999",
  "department": "Engineering",
  "currency": "USD",
  "expenses": [
    {
      "expense_id": "001",
      "amount": 80.00,
      "expense_type": "restaurant",
      "merchant_name": "Starbucks",
      "merchant_category_code": "5814",
      "transaction_date": "2026-04-01T12:30:00Z"
    }
  ]
}
```

**Response Payload:**
```json
{
  "trip_id": "trip_123",
  "overall_status": "PROVISIONALLY_ACCEPTED",
  "total_submitted": 2100.00,
  "total_approved": 2000.00,
  "overall_violations": [
    "Total Trip Budget of $2,000.00 exceeded. Final reimbursement capped."
  ],
  "expenses": [
    {
      "expense_id": "001",
      "submitted_amount": 1100.00,
      "approved_amount": 1100.00, 
      "status": "APPROVED",
      "violations": []
    },
    {
      "expense_id": "002",
      "submitted_amount": 1000.00,
      "approved_amount": 900.00,
      "status": "APPROVED_WITH_LIMIT",
      "violations": ["Reduced by $100.00 to stay within the $2,000.00 total trip cap."]
    }
  ]
}
```

## 4. The Batch Layer (Asynchronous Heavy Path)
Handles massive 4TB daily partner uploads and historical backfill audits.

### 4.1 Pipeline Orchestration
* **Apache Airflow:** A daily scheduled DAG utilizes S3 Sensors to detect when partner bulk files have successfully landed in the Bronze S3 bucket.
* **Transient Compute:** Airflow dynamically provisions an ephemeral **Apache Spark** cluster (Databricks/EMR) to process the workload, shutting it down upon completion to control cloud costs.

### 4.2 Distributed Execution
* **The AST Translator (Native PySpark):** The Spark Driver reads the JSON DSL rules and acts as a compiler. It parses the abstract syntax trees and translates them into highly optimized, native PySpark SQL column expressions (e.g., chained F.when().otherwise() statements). This allows Spark's Catalyst Optimizer and Tungsten Execution Engine to process the policy logic natively at C++ speeds.
* **Stateful Aggregations (Native Windowing):** Running totals and budget caps are calculated dynamically across the 4TB payload using PySpark Window Functions (Window.partitionBy("employee_id")). Spark manages this massive financial state natively using off-heap memory and graceful local SSD spilling, completely avoiding external database I/O bottlenecks during the batch run.
* **Data Skew Management:** The pipeline natively mitigates massive enterprise data skew. It utilizes Adaptive Query Execution (AQE) to dynamically balance join operations, and implements a custom Two-Pass Salting strategy for global window aggregations to ensure even data distribution and prevent single-node executor crashes.
* **Fault Tolerance:** Malformed incoming records (e.g., corrupted Amex CSV rows) are safely caught and routed to a Dead Letter Queue (DLQ) in S3. This ensures that isolated data quality issues do not fail the multi-hour enterprise batch job.

---

## 5. The Serving & Storage Layer (State Management)
To manage state across both real-time and big data workloads, the system utilizes a hybrid database approach with Eventual Consistency.

### 5.1 Operational Database (Amazon DynamoDB)
Provides single-digit millisecond reads for the FastAPI Speed Layer.
* **Table 1: Rule Repository (ExpenseEngine-Rules):** PK: `DEPT#{department_name}`, SK: `RULE#{rule_id}`.
* **Table 2: State Repository (ExpenseEngine-State):** PK: `EMP#{employee_id}`, SK: `BUDGET#{metric_type}#{date}`.

### 5.2 The Data Lakehouse (S3 + Apache Iceberg / Delta Lake)
Utilizes the Medallion Architecture, providing ACID transactions on top of object storage to support simultaneous writes from Kafka and Spark.
* **Bronze (Raw):** Immutable landing zone for S3 partner bulk files and raw Kafka event logs.
* **Silver (Cleaned):** Deduplicated, schema-enforced data ready for Spark processing.
* **Gold (Business Ready):** The final `EvaluationResult` output tables. Highly aggregated and optimized for the Finance Team's BI dashboards and SQL query engines (e.g., Amazon Athena).
* **State Reconciliation:** A nightly downstream task syncs the heavily verified Gold Layer aggregates back into the DynamoDB state tables to resolve any eventual consistency drift.

---

## 6. Enterprise Resilience & Safety Nets
To ensure the system is production-grade, fault-tolerant, and financially secure, the architecture implements several layers of infrastructure safety nets, strictly separating cloud-level resilience from code-level logic.

### 6.1 Reliability & Disaster Recovery (DR)
The system is designed to survive a complete AWS region failure (e.g., `us-east-1` going offline) utilizing an **Active-Passive (Pilot Light)** strategy to balance high availability with cost efficiency.
* **Target SLAs:** RTO `< 2 minutes` for the real-time API, RTO `< 4 hours` for the Batch Layer. RPO is strictly **Near-Zero**.
* **Automated DNS Failover:** AWS Route53 health checks monitor the primary API Gateway. Upon failure, traffic is automatically routed to a scaled-down "Pilot Light" cluster in the secondary region (`us-west-2`), which rapidly auto-scales to meet demand.
* **State & Storage Replication:** * **DynamoDB Global Tables** continuously replicate the transaction state and policy rules across regions with sub-second latency.
  * **S3 Cross-Region Replication (CRR)** ensures the 4TB partner data dumps are securely backed up to the secondary region upon upload.

### 6.2 Data Integrity & Concurrency
Financial systems cannot tolerate duplicate transactions or lost updates due to network retries or concurrent usage.
* **Idempotency (The "Double-Submit" Problem):** All API requests must include a client-generated UUID (`X-Idempotency-Key`). FastAPI utilizes DynamoDB Conditional Writes (`attribute_not_exists`) to guarantee a delayed network retry does not result in a double-charge.
* **Race Conditions (The "Simultaneous Swipe"):** To prevent a user from overdrawing a budget via simultaneous requests from multiple devices, the DynamoDB state table utilizes **Optimistic Locking**. A `version` attribute is checked and incremented upon every write; conflicting parallel writes are rejected and re-evaluated.
* **Batch Upserts:** The Spark layer strictly prohibits raw `INSERT` operations into the Silver and Gold layers. All data is written using Delta/Iceberg `MERGE INTO` syntax keyed on `transaction_id`, allowing the Airflow DAG to be safely re-run without duplicating the 4TB dataset.

### 6.3 Observability & Operational Workflows
Monitoring focuses on business symptoms and data lifecycle management rather than purely tracking server CPU.
* **Distributed Tracing:** An `X-Correlation-ID` is injected at the API Gateway and passed continuously through FastAPI, Kafka, and into the S3 Parquet files, allowing engineers to trace a single dropped expense across the entire distributed system via a centralized logging tool (e.g., ELK/Splunk).
* **The DLQ Lifecycle:** Bad records isolated in the Dead Letter Queue trigger symptom-based alerts. Internal pipeline parser bugs trigger engineering code fixes and "Replay Jobs." External partner schema violations automatically generate and email error reports to the source partner for correction.
* **Symptom Alerting:** PagerDuty is triggered based on SLA breaches, such as the FastAPI p99 latency exceeding 50ms, or DLQ volume exceeding standard thresholds.

### 6.4 Security & Data Governance
Financial data and Personally Identifiable Information (PII) are secured at both the perimeter and the storage level.
* **Authentication:** All entry points are secured via OAuth2/OIDC protocols. AWS API Gateway validates the JSON Web Tokens (JWT) before traffic touches the internal VPC.
* **Encryption:** TLS 1.2+ enforces encryption in transit. AWS KMS (Customer-Managed Keys) enforces encryption at rest across S3, DynamoDB, and Kafka EBS volumes.
* **Catalog-Level RBAC & Masking:** PII protection is decoupled from compute code. Using **AWS Lake Formation** and the **Glue Data Catalog**, row-level and column-level access controls are applied to the data lake. For example, if a business analyst queries the Gold tables via Amazon Athena, sensitive columns (like `bank_account_number`) are dynamically masked, whereas the internal Spark execution role can read the raw values.

### 6.5 Infrastructure Scalability
To handle the extreme data volume disparities (API vs. 4TB Batch) efficiently:
* **Compute Auto-Scaling:** Both the FastAPI container orchestration layer (ECS/EKS) and the transient Spark clusters utilize dynamic auto-scaling rules to handle peak loads without paying for idle resources during off-hours.
* **Storage Partitioning:** S3 Bronze, Silver, and Gold data files are physically partitioned by `event_date` (and heavily skewed banks where necessary). This prevents downstream systems (like Athena or Airflow sensors) from executing full 4TB table scans. *(Note: Code-level big data scaling, such as Adaptive Query Execution and Salting, are documented in the Low-Level Design).*