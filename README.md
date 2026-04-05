# Real-Time Spend Authorization Gateway

## 👨‍💻 Author
**Madhav Rao** *Principal Architect | Distributed Data Systems*

---

## 🎯 Business Purpose
The Real-Time Spend Authorization Gateway is an automated financial decision engine that evaluates employee expenses at the exact moment they occur. When an employee swipes a corporate card or submits a manual receipt, this system instantly checks the requested amount against the company’s policies and the employee’s remaining budget.

Its primary business purpose is to protect company funds by strictly enforcing spending limits before any money leaves the account. By making these decisions instantly, the system guarantees that the company never exceeds its allocated budgets, provides employees with immediate clarity on what is approved, and eliminates the need for the finance team to manually review routine expenses.

---

## 🏗️ Technical Strategy & Architecture
This system is built using a modern **Lambda Architecture** to separate synchronous point-of-sale decision-making from asynchronous financial reconciliation. The core strategy relies on three isolated pillars:

**1. The Speed Layer (Real-Time Authorization)**
To support sub-50ms responses to credit card network webhooks and manual API requests, the system utilizes a **FastAPI** microservice backed by **Amazon DynamoDB**. 
* **Stateless Logic:** Rules are defined as JSON-based Abstract Syntax Trees (AST) allowing dynamic policy updates without code deployments. 
* **Stateful Guardrails:** DynamoDB acts as a high-speed "Exposure Cache," utilizing Optimistic Locking (Conditional Writes) to track budget utilization in real-time and physically prevent concurrent double-spending.

**2. The Event Bridge (Decoupling)**
The Speed Layer does not communicate directly with downstream financial systems. Once a transaction is approved, partially approved (Pay & Chase), or declined, the FastAPI service fires the decision payload into an **Apache Kafka** event stream (fire-and-forget), ensuring the real-time API remains unblocked.

**3. The Batch Layer (Reconciliation & Settlement)**
The ultimate financial truth is established asynchronously. An **Apache Spark** batch process reads from the data lake (ingesting both Kafka events and nightly Bank CSV settlements) to execute the final audit. This layer handles delayed transactions, refunds, and overage calculations, eventually performing a Reverse-ETL sync back to DynamoDB to correct any state drift.

---

## 📂 Project Structure
* **`/core`**: DSL Schema definitions, pure-Python AST Evaluator logic (incorporating Pay & Chase financial math), and Shared Policy JSONs.
* **`/speed`**: FastAPI application handling physical card webhooks and manual API uploads, Pydantic response models, and real-time DynamoDB state management.
* **`/batch`**: PySpark jobs for massive-scale auditing, Airflow DAGs, and the nightly Reverse-ETL true-up logic.
* **`/infra`**: Infrastructure as Code for DynamoDB tables, S3 Medallion buckets, and MSK/Kafka configurations.
* **`/docs`**: Detailed High-Level Design (HLD), Low-Level Design (LLD), system design diagrams, and API specifications.

---

## 🛠️ Tech Stack Highlights
* **Logic Engine:** Config-Driven JSON DSL (AST Parsed).
* **Compute:** FastAPI (Async/Uvicorn), Apache Spark (Native SQL & Windowing).
* **Storage:** DynamoDB (Real-Time Ledger & Guardrails), S3 + Iceberg/Delta Lake (Analytical Lakehouse).
* **Event Stream:** Apache Kafka (Decoupling synchronous API from asynchronous batch).
* **Observability:** Distributed Tracing with `X-Correlation-ID` across API and Spark pipelines.

---

## 📘 Deep Dive Documentation
For a detailed technical breakdown of the architecture, including database schemas, concurrency controls, and the AST Rule Evaluator logic, please refer to the main technical blueprints:

👉 **[High-Level Design (HLD)](docs/hld/high-level-design.md)**