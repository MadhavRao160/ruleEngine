# Expense Policy Rules Engine

## 👨‍💻 Author
**Madhav Rao**
*Principal Architect | Distributed Data Systems*

---

## 🎯 Business Purpose
The **Expense Policy Rules Engine** is an enterprise-grade compliance platform designed for extreme scale. It automates the validation of corporate spend by unifying real-time user experience with massive backend financial reconciliation.

### Core Value Propositions:
* **Zero Logic Drift:** Uses a **JSON-based Domain Specific Language (DSL)** to ensure the exact same policy is enforced across the Mobile API and the 4TB Batch Auditor.
* **Provisional Guardrails:** Provides sub-20ms feedback to employees via **Provisional Acceptance**, reducing friction while maintaining financial control.
* **Budget Integrity:** Automatically reconciles real-time spend against final **Bank Settlements** using a nightly "Waterfall" reconciliation process.

---

## 🏗️ Technical Strategy: Config-Driven Lambda
The system utilizes a **Lambda Architecture** where the "Shared Core" is a version-controlled **Policy DSL** rather than hardcoded logic.

* **Speed Layer (FastAPI):** An **AST (Abstract Syntax Tree) Evaluator** that parses JSON rules in real-time to provide immediate, low-latency feedback.
* **Batch Layer (Apache Spark):** A **DSL-to-SQL Translator** that converts JSON rules into native PySpark Window functions and optimized Columnar expressions for 4TB+ processing.
* **Unified State:** All layers sync to a **Budget Utilization Ledger** (DynamoDB), ensuring a single source of truth for an employee's remaining balance.

---

## 📂 Project Structure
* **/core**: DSL Schema definitions, AST Parser logic, and Shared Policy JSONs (The "Source of Truth").
* **/speed**: FastAPI application, Pydantic response models, and the real-time "Provisional" evaluation logic.
* **/batch**: PySpark jobs for massive scale auditing, Airflow DAGs, and the Nightly Reconciliation (Reverse-ETL) logic.
* **/infra**: Infrastructure as Code for DynamoDB Global Tables, S3 Medallion buckets, and MSK/Kafka configurations.
* **/docs**: Detailed **Unified HLD**, Symmetrical Mermaid diagrams, and API specifications.

---

## 🛠️ Tech Stack Highlights
* **Logic Engine:** Config-Driven JSON DSL (No Logic Duplication).
* **Compute:** FastAPI (Async/Uvicorn), Apache Spark (Native SQL & Windowing).
* **Storage:** DynamoDB (Budget Utilization Ledger), S3 + Delta Lake/Iceberg (Analytical Lakehouse).
* **Observability:** Distributed Tracing with `X-Correlation-ID` across API and Spark.

---

## 📘 Deep Dive Documentation
For a detailed technical breakdown of the architecture, including disaster recovery, idempotency strategies, and data integrity safety nets, please refer to the main technical blueprint:

👉 **[Unified Architecture & HLD](README/HLD-README.md)**