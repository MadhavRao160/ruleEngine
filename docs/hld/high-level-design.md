# High-Level Design: Real-Time Spend Authorization Gateway

**Author:** Madhav Rao  
**Role:** Principal Architect | Distributed Data Systems

---

## 🎯 Project Objective
The **Real-Time Spend Authorization Gateway** is a mission-critical financial decision engine designed to evaluate and control corporate spend at the exact moment of transaction. By shifting from a reactive auditing model to a proactive authorization model, the platform solves three distinct engineering and financial challenges across two compute extremes:

1. **Real-Time Point-of-Sale Authorization (The Speed Layer):** Providing sub-50ms cryptographic decisions for global credit card network webhooks (e.g., Visa, Stripe) and manual mobile API requests, physically preventing budget overruns before money leaves the account.
2. **Asynchronous Financial True-Up (The Batch Layer):** Processing terabyte-scale nightly settlement files from partner banks to reconcile provisional real-time holds against finalized financial reality. This layer automatically corrects "state drift" caused by hotel holds, refunds, and calculates employee liabilities for "Pay & Chase" overages.
3. **Zero Logic Drift (The Shared Core):** Ensuring that the exact same financial policies—defined as JSON-based Abstract Syntax Trees (AST)—are executed by a millisecond-latency API and a massive PySpark cluster without duplicating code.

To achieve this without race conditions or double-spending, the system utilizes a modern **Lambda Architecture**, relying on strict conditional locking mechanisms within a highly available Distributed Ledger (DynamoDB).

---

## 🛠️ Technology Stack
The platform is built on a strictly segregated technology stack, optimizing for millisecond latency at the edge and massive parallel processing in the core.

### The Logic Core (Shared)
* **Domain Specific Language:** JSON-based Abstract Syntax Tree (AST).
* **Evaluator Engine:** Pure Python (Stateless, zero-dependency engine capable of running in both Uvicorn workers and PySpark UDFs).

### The Speed Layer (Real-Time Authorization)
* **Web Framework:** FastAPI (Async execution) with Pydantic (Strict payload validation).
* **Stateful Ledger:** Amazon DynamoDB (Utilized specifically for single-digit millisecond reads and Conditional Writes/Optimistic Locking).
* **API Gateway:** AWS API Gateway (Handling webhook routing and payload throttling).

### The Event Bridge (Decoupling)
* **Message Broker:** Apache Kafka / Amazon MSK (High-throughput, persistent event streaming for fire-and-forget API offloading).

### The Batch Layer (Reconciliation & True-Up)
* **Big Data Compute:** Apache Spark / PySpark (Running on AWS EMR or AWS Glue for terabyte-scale CSV ingestion and state drift calculation).
* **Data Lake Storage:** Amazon S3 (Bronze/Silver/Gold Medallion Architecture).
* **Table Format:** Apache Iceberg or Delta Lake (For ACID compliance and time-travel querying on the Data Lake).
* **Orchestration:** Apache Airflow / MWAA (Managing the nightly settlement and reverse-ETL DAGs).
* **Analytics:** Amazon Athena (Serverless SQL querying for Finance BI dashboards).

---

## 1. System Overview & Architectural Patterns

### 1.1 System Overview
The Real-Time Spend Authorization Gateway is a distributed, high-availability financial engine designed to evaluate corporate spend policies at the point of sale. It operates at the intersection of microsecond Online Transaction Processing (OLTP) and terabyte-scale Online Analytical Processing (OLAP). The system serves as the definitive gatekeeper between external financial networks (e.g., Visa, Stripe, mobile clients) and internal corporate ledgers. It is designed around a dual-lifecycle model:
* **Synchronous Authorization:** Providing strictly bound, sub-50ms REST/JSON webhook responses to external card networks to approve or decline real-time transactions.
* **Asynchronous Settlement:** Processing massive delayed clearing files from partner banks to reconcile provisional holds, process refunds, and correct long-term state drift.

### 1.2 Core Architectural Patterns

**A. Modified Lambda Architecture**
Traditional Lambda architectures run identical logic across both real-time and batch layers to produce a unified view. This system modifies that pattern into a **Complementary Lambda Architecture**:
* **The Speed Layer:** Acts as an "Exposure Cache." It makes provisional, real-time decisions based on immediate state and strictly manages risk (the "Hold").
* **The Batch Layer:** Acts as the "Financial Truth." It ingests delayed settlement data to perform a final audit and executes a Reverse-ETL sync to correct any discrepancies (the "True-Up").

**B. Stateless Compute with a Stateful Distributed Ledger**
The compute nodes evaluating the financial rules (FastAPI workers) are strictly stateless. They can be scaled horizontally infinitely and killed without warning. All financial state—specifically the employee's remaining budget—is centralized in a highly available NoSQL ledger (Amazon DynamoDB). 
* **Concurrency Control:** To prevent "double-spending" race conditions during simultaneous card swipes, the system relies entirely on DynamoDB’s **Optimistic Locking (Conditional Writes)** rather than application-level memory locks.

**C. Event-Driven Decoupling (The "Fire-and-Forget" Bridge)**
Because the external REST/JSON webhook expects an HTTP response within milliseconds, the Speed Layer cannot afford to communicate directly with downstream analytical or HR systems. Once FastAPI successfully commits the budget deduction to DynamoDB, it emits a standardized JSON decision payload to an Apache Kafka topic and immediately closes the HTTP request. Kafka acts as an asynchronous shock absorber, decoupling the fast, synchronous API world from the heavy, asynchronous Big Data world.

**D. Configuration-Driven Logic (AST Rule Engine)**
To prevent "logic drift" between the API microservices and the Big Data clusters, financial policies are not hardcoded into Python. Instead, business rules are defined as **JSON-based Abstract Syntax Trees (AST)**. Both the FastAPI Speed Layer and the PySpark Batch Layer pull from the exact same JSON policy definitions. This allows the business to deploy new financial rules instantly across the entire platform without requiring a code deployment.

**E. Medallion Lakehouse Architecture**
The Batch Layer utilizes a strictly zoned Data Lakehouse pattern (Bronze, Silver, Gold) resting on Amazon S3.
* **Bronze (Raw):** Ingests both the real-time API Kafka events and the raw CSV bank settlement files.
* **Silver (Cleaned & Matched):** Standardizes schemas and matches the provisional API holds against the finalized bank settlements.
* **Gold (Aggregated):** Calculates the final financial true-ups (refunds, overages, dropped holds) which are then fed into BI tools (Athena) and the automated Reverse-ETL pipeline back to DynamoDB.

---

## 2. Entrypoints & The Intelligent Router (Ingestion)

### 2.1. Unified RESTful Gateway (AWS API Gateway)
All external traffic enters the system through **AWS API Gateway**. This layer provides the necessary security and traffic management:
* **TLS Termination & WAF:** Ensures all incoming webhooks and mobile requests are encrypted and filtered for common web exploits.
* **Source-Based Throttling:** Applies distinct rate-limiting tiers. Card network webhooks are prioritized with high-burst allowances, while manual mobile uploads are throttled to prevent "noisy neighbor" impacts on the Speed Layer.
* **Stateless Routing:** Directs traffic to the FastAPI cluster via a Private Link, ensuring the internal microservices are never exposed directly to the public internet.

### 2.2. The Fast-Path: Real-Time Transaction Ingestion
The most critical entry point is the **Synchronous Webhook Endpoint** (`POST /v1/webhooks/card-auth`).
* **Payload Validation:** FastAPI uses **Pydantic** to perform strict, zero-copy schema validation. If the payload does not match the banking standard (e.g., ISO 20022 JSON mapping), it is rejected at the edge with an HTTP 400.
* **Context Hydration:** The router immediately extracts the `Employee_ID` and `Merchant_Category` to prepare the context for the AST Evaluator.
* **Synchronous Response:** The connection is held open until a decision is reached (Approve/Decline). The router is designed to return a response within a 50ms window to prevent timeouts at the physical Point-of-Sale (POS) terminal.

### 2.3. The Slow-Path: Settlement Ingestion (Claim Check Pattern)
To handle massive nightly data dumps (3–4TB) from partner banks without overwhelming the API's memory or bandwidth, the system implements the **Claim Check Pattern**:
* **Metadata Request:** The Partner Bank sends a lightweight "Upload Request" containing file metadata (size, checksum, partner ID).
* **Pre-Signed URL Generation:** Instead of accepting the file, the FastAPI router generates an **AWS S3 Pre-signed URL**. This URL is a time-limited, cryptographically signed token that grants the bank's system permission to upload directly to a specific "Bronze" S3 prefix.
* **Direct-to-S3 Upload:** The bank's system uploads the multi-terabyte CSV directly to S3. This bypasses the API Gateway and FastAPI entirely, ensuring that the heavy ingestion of historical data never competes with real-time card authorizations.

### 2.4. Intelligent Load Shedding
The router acts as a circuit breaker. If the **DynamoDB** latency exceeds a pre-defined threshold (e.g., >100ms), the Intelligent Router can trigger a "Failsafe" mode:
* **For Card Swipes:** It can be configured to default to "Stand-In Processing" (STIP), allowing small-value transactions to pass through while logging the event for later reconciliation.
* **For Manual Claims:** It returns an HTTP 202 (Accepted), telling the user the claim will be processed shortly, moving the task from a synchronous wait to an asynchronous background job.

---

## 3. The Speed Layer (Real-Time Synchronous Path)

### 3.1 Component Flow
1.  **Request Ingestion & Validation:** FastAPI receives the REST/JSON payload. **Pydantic** models enforce strict schema validation.
2.  **State Hydration (The Exposure Check):** The system performs a single-digit millisecond read from **Amazon DynamoDB** to fetch the employee’s current state, including remaining budget balances and active policy metadata.
3.  **AST Policy Evaluation:** The stateless **Python AST Evaluator** executes the JSON-defined rules against the hydrated state. It calculates the decision based on **Strategy 2 (Pay and Chase)**:
    * **Approved:** Within the base limit.
    * **Approved with Overage:** Exceeds base limit but stays within the "Company Float" threshold.
    * **Hard Decline:** Exceeds the maximum allowable float.
4.  **Atomic State Persistence:** If the transaction is approved (fully or partially), FastAPI attempts to update the DynamoDB ledger. This uses **Optimistic Locking (Conditional Writes)** to ensure the balance is only deducted if it has not changed since the hydration step.
5.  **Event Emission (Kafka Bridge):** The final decision—including the breakdown of company vs. employee liability—is published asynchronously to the **Apache Kafka** `transaction-decisions` topic.
6.  **Synchronous Response:** The system returns the final status to the caller. For card webhooks, this is a binary Approve/Decline; for mobile users, this includes a detailed breakdown of the decision.

### 3.2 API Contracts
**A. Card Network Webhook (`POST /v1/webhooks/card-auth`)**
* **Request Payload:** Contains `network_tx_id`, `employee_id`, `amount`, `currency`, `mcc`, `merchant_name`.
* **Response Payload (Status 200):** Contains `auth_decision: "APPROVE"`, `transaction_id`, `timestamp`.

**B. Manual Claim Submission (`POST /v1/claims/manual`)**
* **Request Payload:** Contains `claim_id`, `employee_id`, `amount`, `category`, `receipt_url`.
* **Response Payload (Status 200):** Contains `status: "APPROVED_WITH_OVERAGE"`, `breakdown` (company_covered vs employee_liability), and a user-friendly `message`.

### 3.3 Concurrency Guardrails
The Speed Layer utilizes **DynamoDB Conditional Expressions** (`SET balance = balance - :val WHERE balance >= :min_required`) as the primary defense against race conditions. This ensures that even if two FastAPI workers attempt to authorize two different $60 swipes simultaneously against a $100 budget, only one will succeed.

---

## 4. The Batch Layer (Asynchronous Heavy Path)

### 4.1. Role and Responsibilities
The Batch Layer operates on a nightly schedule to perform deep financial reconciliation. Its primary responsibilities include matching real-time "Provisional Holds" (Kafka) against "Final Settlements" (Bank CSVs) and identifying "State Drift" (e.g., hotel hold releases or refunds).

### 4.2. The Medallion Lakehouse Lifecycle (S3 + Spark)
* **Bronze (Raw Data):** Ingests two primary streams: **Kafka Event Archives** and **Bank Settlement Files**. Data is stored in original formats to allow for complete "Time-Travel" re-processing.
* **Silver (Standardized & Matched):** Spark performs schema enforcement and joins API events to Bank records using a composite key (`Employee_ID` + `Merchant_ID` + `Date` + `Normalized_Amount`).
* **Gold (Aggregated):** Calculates the final **True-Up Delta** (difference between provisional hold and actual bank charge) and flags "Pay & Chase" overages for payroll.

### 4.3. The Reconciliation Logic (Match & Audit)
Spark executes a "Fuzzy Matching" algorithm:
1.  **Exact Match:** `Network_TX_ID` matches perfectly.
2.  **Inferred Match:** Matches based on merchant, timestamp window, and employee ID.
3.  **Exception Handling:** Unmatched transactions move to a **Manual Review Table** (Dead Letter Queue) for investigation.

### 4.4. The State True-Up (Reverse ETL)
Once the "Gold" truth is established, a dedicated Spark job performs an atomic increment/decrement on the employee's budget in **DynamoDB**, effectively "refunding" released holds back to the employee’s real-time limit.

### 4.5. Analytical Access (Amazon Athena)
The Gold layer is exposed via **Amazon Athena** for standard SQL queries for monthly closing, tax reporting, and spend-trend analysis.

---

## 5. The Serving & Storage Layer (State Management)

### 5.1. Operational State: The Active Ledger (Amazon DynamoDB)
* **Data Model:** Optimized for Key-Value access, using `Employee_ID` as the Partition Key (PK).
* **Double-Spending Guard:** Employs **Optimistic Locking** on all updates.
* **TTL (Time-to-Live):** Provisional records are assigned a TTL of 7 days to keep the table lean.

### 5.2. Analytical State: The Medallion Lakehouse (Amazon S3)
Implements Medallion Architecture using **Apache Iceberg** to provide ACID transactions, preventing "partial writes" during heavy Spark jobs.

### 5.3. Configuration State: The Rule Repository
Financial policies are treated as **State-as-Code**. JSON policy files are stored in versioned S3 buckets and cached locally by FastAPI and Spark nodes, ensuring global updates take effect within minutes.

### 5.4. Managing State Drift (The Reconciliation Loop)
1.  **Detection:** Spark identifies discrepancies (e.g., a $200 hold vs $150 final charge).
2.  **Correction (Reverse ETL):** An atomic "Credit" update is fired back to the DynamoDB Ledger.
3.  **Result:** The employee’s real-time spending power is restored.

---

## 6. Enterprise Resilience & Safety Nets

### 6.1 Reliability & Disaster Recovery (DR)
* **Multi-AZ Deployment:** FastAPI nodes deployed across three AZs.
* **Data Persistence:** DynamoDB Point-in-Time Recovery (PITR); S3 Cross-Region Replication (CRR).
* **Metrics:** RTO < 15 minutes; RPO < 5 minutes.

### 6.2 Data Integrity & Concurrency
* **Idempotency Key:** `Network_TX_ID` prevents duplicate deductions during network retries.
* **Optimistic Locking:** Ensures state consistency in distributed environments.
* **ACID Lakehouse:** Apache Iceberg prevents "partial writes" during large batch updates.

### 6.3 Observability & Operational Workflows
* **Distributed Tracing:** `X-Correlation-ID` traces a single swipe through the entire 24-hour Lambda lifecycle.
* **Real-Time Monitoring:** Alarms for p99 latency > 100ms or Kafka lag > 5 minutes.
* **Drift Alerting:** Alerts fired if the gap between provisional approval and bank settlement exceeds 5%.

### 6.4 Security & Data Governance
* **Encryption:** AES-256 at rest (KMS); TLS 1.3 in transit.
* **Least Privilege:** FastAPI compute nodes have "Write-Only" access to Kafka and "Scoped-Read/Write" access to DynamoDB.
* **PII Masking:** Redacts sensitive employee info in the Gold layer.
* **Audit Logging:** Tamper-proof logs for every AST Policy change.

### 6.5 Infrastructure Scalability
* **FastAPI Scaling:** Horizontal Pod Autoscaling (HPA) based on CPU and RPS.
* **Spark Dynamic Allocation:** Scales executors based on the size of the nightly bank file (4TB vs 100GB).
* **DynamoDB Auto-Scaling:** Uses On-Demand mode for the baseline period, moving to Provisioned with Auto-Scaling for cost optimization.