# Low-Level Design (LLD): FastAPI Speed Layer

**Component:** Real-Time Spend Authorization API | **Framework:** FastAPI | **State:** DynamoDB | **Event Bus:** Kafka | **Latency:** < 50ms (p99)

---

## 1. Architectural Philosophy
The Speed Layer is a **Stateless Decision Engine** with a **Config-Driven Core** designed to ensure horizontal scalability and zero logic drift between real-time and batch environments.

* **Rules as Data:** Logic is externalized as JSON **ASTs**. Both FastAPI and Spark evaluate the same JSON "source of truth," preventing logic drift.
* **Stateless Compute:** Worker nodes maintain no local session state; all financial memory is externalized to **DynamoDB**.
* **Pay and Chase (Strategy 2):** The engine calculates liability splits (Company vs. Employee) to allow approvals within "Company Float" thresholds, rather than binary blocks.
* **Idempotent Execution:** Uses `network_tx_id` as a unique key to prevent duplicate budget deductions during network retries.
* **Atomic Reliability:** Employs **Optimistic Locking** and **Atomic Transactions** to manage high-concurrency swipes and dual-table writes.

---

## 2. Domain Entities
* **AuthorizationRequest:** `network_tx_id`, `employee_id`, `amount`, `currency`, `mcc`, `timestamp`.
* **PolicyAST:** `department_id`, `version`, `nodes`, `required_metadata` (pre-calculated state keys for hydration).
* **LedgerState:** `daily_meal_consumed`, `monthly_travel_consumed`, `company_float_limit`, `is_active`.
* **DecisionResult:** `status` (APPROVED, OVERAGE, HARD_DECLINE), `company_covered`, `employee_liability`, `reason_codes`.

---

## 3. Core Design Patterns
* **AST Evaluator:** A recursive engine that traverses JSON nodes to resolve operators (AND/OR) and comparisons (>, ==) between the Request and the hydrated LedgerState.
* **Hydration Pattern:** The service reads `required_metadata` from the active Policy, performs a targeted fetch of only the necessary attributes from DynamoDB, and injects them into the `LedgerState` entity.
* **Repository Pattern:** Isolates I/O logic. `RuleRepository` (LRU cache with 5-min TTL) and `StateRepository` (DynamoDB I/O with Transactional Write support).

---

## 4. The Execution Flow
1. **Ingestion:** API validates schema and checks the **Transaction Journal** for the `network_tx_id` to ensure idempotency.
2. **Rule Retrieval:** Retrieves the Policy AST from the local cache (preferred) or the Rule Store.
3. **Hydration:** Fetches required state metrics via single-digit millisecond DynamoDB reads.
4. **Evaluation:** The engine calculates the split between company and employee liability based on the AST logic.
5. **Atomic Persistence:** Executes `TransactWriteItems` to update the employee balance (Table 1) and record the transaction in the Journal (Table 3) simultaneously.
6. **Broadcasting:** Asynchronous Kafka event dispatch for the Batch Layer; DynamoDB Streams serve as a delivery fallback for 100% audit integrity.
7. **Final Response:** Synchronous HTTP response returned to the card network or mobile client.

---

## 5. DynamoDB Data Model

The schema is optimized for **O(1) lookups** using a Single-Table Design philosophy across three specific tables to isolate state, rules, and audit logs.

### Table 1: State Ledger (ExpenseEngine-State)
**PK:** `EMP#{id}` | **SK:** `BUDGET#{type}#{granularity}`

| PK | SK | Allocated | Consumed | Version | TTL |
| :--- | :--- | :--- | :--- | :--- | :--- |
| EMP#101 | BUDGET#MEAL#2026-04-05 | 100.00 | 45.00 | 2 | 1712361600 |
| EMP#101 | BUDGET#TRAVEL#2026-04 | 5000.00 | 1200.00 | 1 | 1714521600 |

### Table 2: Rule Store (ExpenseEngine-Rules)
**PK:** `DEPT#{id}` | **SK:** `VERSION#{ts}`

| PK | SK | policy_ast (JSON) | required_metadata | is_active |
| :--- | :--- | :--- | :--- | :--- |
| DEPT#SALES | VERSION#1712330000 | {...ast_json...} | ["meal_daily"] | true |

### Table 3: Transaction Journal (ExpenseEngine-Journal)
**PK:** `TX#{network_tx_id}` | **SK:** `METADATA`

| PK | SK | status | decision_payload | TTL |
| :--- | :--- | :--- | :--- | :--- |
| TX#auth_987 | METADATA | PROCESSED | {...json...} | 1712448000 |

**Consistency Note:** Table 1 and Table 3 are updated atomically via `TransactWriteItems` to guarantee that every budget deduction is tied to a unique transaction record.