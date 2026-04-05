# Expense Policy Rules Engine - Visual Architecture Flows

This document contains detailed architectural diagrams for the Unified Platform.

## 1. The Master Data Map (Lambda Architecture)
This shows the macro-level split between the Speed Layer and the Batch Layer.

```mermaid
graph TD
    %% ==========================================
    %% 1. DEFINE COLOR & STYLE CLASSES
    %% ==========================================
    classDef compute fill:#a8dadc,stroke:#1d3557,stroke-width:1px,rx:5,ry:5,color:#000;
    classDef database fill:#f1faee,stroke:#1d3557,stroke-width:1px,rx:10,ry:10,color:#000;
    classDef s3 fill:#fefae0,stroke:#bc6c25,stroke-width:1px,rx:15,ry:15,color:#000;
    classDef core fill:#2a9d8f,stroke:#264653,stroke-width:2.5px,color:#fff;
    classDef external fill:#f8f9fa,stroke:#adb5bd,stroke-width:1px,color:#333;
    classDef stream fill:#e6e6fa,stroke:#9370DB,stroke-width:1px,color:#000;
    classDef router fill:#d1e7dd,stroke:#198754,stroke-width:1px,color:#000;

    %% ==========================================
    %% 2. DEFINE COMPONENTS
    %% ==========================================
    
    subgraph Clients [External Entities & Sources]
        Mobile[Mobile / Web App <br> Manual Claims]
        CardNet[Card Network Webhooks <br> Real-Time Swipes]
        Bank["Partner Bank <br> Nightly Settlement CSV"]
    end
    class Mobile,CardNet,Bank external;

    subgraph Edge [Edge & Ingestion]
        API_GW[AWS API Gateway]
        Pre_Signed[S3 Pre-Signed URL Controller]
    end
    class API_GW,Pre_Signed router;

    subgraph Speed_Layer [Speed Layer: Real-Time Gatekeeper]
        FastAPI[FastAPI Service <br> AST Rule Evaluator]
        Kafka[Apache Kafka <br> Event Bridge]
    end
    class FastAPI compute;
    class Kafka stream;

    subgraph DynamoDB [Amazon DynamoDB: The Active State]
        State[(Budget Utilization Ledger <br> Exposure Cache)]
        DSL[(Rules Table <br> JSON AST Policies)]
    end
    class State database;
    class DSL core; 

    subgraph Batch_Layer [Batch Layer: Reconciliation]
        Airflow[Apache Airflow Orchestration]
        Spark[Apache Spark Cluster <br> ETL & Auditing]
    end
    class Airflow,Spark compute;

    subgraph Storage [Analytical Lakehouse]
        Bronze[(Bronze S3 Raw Data)]
        Silver[(Silver S3 Clean Data)]
        Gold[(Gold S3 / Iceberg Aggregates)]
        Athena[Amazon Athena <br> Finance Dashboard]
    end
    class Bronze,Silver,Gold s3;
    class Athena external;

    %% ==========================================
    %% 3. DEFINE CONNECTIONS
    %% ==========================================
    %% Real-Time Paths
    Mobile -->|JSON Request| API_GW
    CardNet -->|Synchronous Auth Webhook| API_GW
    API_GW --> FastAPI
    
    %% Speed Layer Operations
    FastAPI <-->|Check/Update Balance <br> Conditional Write| State
    DSL -.->|Load JSON Policy| FastAPI
    
    %% Event Decoupling
    FastAPI -->|Fire & Forget Decision| Kafka
    Kafka -->|Stream Load| Bronze

    %% Asynchronous Batch Path
    Bank -->|Direct PUT| Bronze
    Bank -.->|Request Upload| Pre_Signed
    
    %% Batch Operations
    Airflow -->|Triggers Nightly| Spark
    Bronze -->|Reads Combined Data| Spark
    DSL -.->|Batch Policy Sync| Spark
    
    Spark -->|Clean & Map| Silver
    Silver -->|Aggregate Math| Gold
    Gold --> Athena
    
    %% The True-Up Loop
    Gold -.->|Reverse ETL <br> Settlement True-Up| State
```

## 2: The Ingestion "Claim Check" Pattern.
This sequence diagram illustrates how we prevent 1TB+ partner files from crashing our synchronous web servers.

```mermaid
sequenceDiagram
    autonumber
    
    %% Define Participants
    actor Bank as Partner Bank (Settlement)
    participant API as AWS API Gateway
    participant FA as FastAPI (Ingestion Controller)
    participant S3 as AWS S3 (Bronze Lakehouse)

    Note over Bank, FA: Step 1: Requesting the "Ticket" (Micro-Payload)
    Bank->>API: POST /v1/settlement/upload-request {size: "1TB", partner: "Visa_Settlement"}
    API->>FA: Route Request
    FA-->>FA: Authenticate Identity & Authorize Upload
    
    Note over FA, S3: FastAPI acts as the Controller, asking AWS for a temporary, secure URL
    FA->>S3: Request Pre-Signed URL (Action: PUT, Expiry: 15m)
    S3-->>FA: Return Cryptographic URL string
    FA-->>API: HTTP 200 OK {upload_url: "https://s3.aws.com/..."}
    API-->>Bank: HTTP 200 OK {upload_url: "https://s3.aws.com/..."}

    Note over Bank, S3: Step 2: The Actual Upload (Macro-Payload)<br/>FastAPI and API Gateway are bypassed to prevent memory exhaustion.
    
    Bank->>S3: HTTP PUT /1TB_Settlement_Data.csv (using upload_url)
    S3-->>S3: Validate Cryptographic Signature & Expiry
    S3-->>Bank: HTTP 200 OK (Settlement File Safely Landed)
```

## 3: The Speed Layer (Real-Time)
This component flow proves how we meet the strict < 20ms latency SLA for mobile app users. Notice how the Kafka publish is safely outside the user's waiting path.

```mermaid
graph LR
    %% ==========================================
    %% 1. STYLE DEFINITIONS
    %% ==========================================
    classDef compute fill:#a8dadc,stroke:#1d3557,stroke-width:1px,color:#000;
    classDef database fill:#f1faee,stroke:#1d3557,stroke-width:1px,color:#000;
    classDef core fill:#2a9d8f,stroke:#264653,stroke-width:2.5px,color:#fff;
    classDef stream fill:#e6e6fa,stroke:#9370DB,stroke-width:1px,color:#000;
    classDef external fill:#f8f9fa,stroke:#adb5bd,stroke-width:1px,color:#333;

    %% ==========================================
    %% 2. SYMMETRICAL PIPELINE (Left to Right)
    %% ==========================================
    
    subgraph Clients_In [Synchronous Inputs]
        Mobile_In((Mobile App <br> Manual Upload))
        CardNet_In((Card Network <br> POS Webhook))
    end
    class Mobile_In,CardNet_In external;

    API[AWS API Gateway]
    class API compute;

    FA[FastAPI Service <br> AST Evaluator <br> 'Pay & Chase' Logic]
    class FA compute;

    subgraph Clients_Out [Synchronous Responses]
        Mobile_Out((Mobile App <br> UI Update))
        CardNet_Out((POS Terminal <br> Approve/Decline))
    end
    class Mobile_Out,CardNet_Out external;

    %% Main Request-Response Spine
    Mobile_In -->|1a. POST JSON| API
    CardNet_In -->|1b. Auth Request| API
    API -->|2. Route| FA
    
    FA -->|"5a. HTTP 200 (Status)"| Mobile_Out
    FA -->|5b. HTTP 200/403| CardNet_Out

    %% ==========================================
    %% 3. THE SHARED CORE (Vertical Axis)
    %% ==========================================
    
    subgraph DynamoDB [Amazon DynamoDB: The Active State]
        direction TB
        DSL[(Rules Table <br> JSON AST Policies)]
        State[(Budget Utilization Ledger <br> Exposure Cache)]
    end
    class State database;
    class DSL core;

    %% Database Interactions (Hanging vertically off FastAPI)
    DSL -.->|3. Load Context| FA
    FA <-->|"4. Conditional Write <br> (Optimistic Lock)"| State

    %% ==========================================
    %% 4. ASYNC OUTPUT (Off-axis)
    %% ==========================================
    
    Kafka[Apache Kafka <br> Event Stream]
    class Kafka stream;

    FA -.->|"6. Fire Async Event <br> (Decision Payload)"| Kafka

    %% ==========================================
    %% 5. LAYOUT TWEAKS (Invisible Symmetry)
    %% ==========================================
    %% Force Kafka to stay to the right of the DB block
    DynamoDB ~~~ Kafka
```

## 4. The Batch Layer (Data Lakehouse ETL)
This diagram shows the asynchronous orchestration of massive historical data through the Medallion architecture (Bronze, Silver, Gold).

```mermaid
graph LR
    %% ==========================================
    %% 1. STYLE DEFINITIONS
    %% ==========================================
    classDef compute fill:#a8dadc,stroke:#1d3557,stroke-width:1px,color:#000;
    classDef s3 fill:#fefae0,stroke:#bc6c25,stroke-width:1px,color:#000;
    classDef core fill:#2a9d8f,stroke:#264653,stroke-width:2.5px,color:#fff;
    classDef database fill:#f1faee,stroke:#1d3557,stroke-width:1px,color:#000;
    classDef airflow fill:#e1f5fe,stroke:#01579b,stroke-width:1px,color:#000;

    %% ==========================================
    %% 2. PIPELINE ZONES (Left to Right)
    %% ==========================================

    subgraph Inputs [1. Inputs & Control]
        Airflow[Apache Airflow <br> Orchestrator]
        DSL[(Rules Table <br> JSON Policies)]
        Bronze_Events[(Bronze S3 <br> Kafka API Holds)]
        Bronze_Bank[(Bronze S3 <br> Bank CSV Settlements)]
    end
    class Airflow airflow;
    class DSL core;
    class Bronze_Events,Bronze_Bank s3;

    subgraph Processing [2. Compute Engine]
        Spark[Spark Cluster <br> Match, Audit & True-Up]
        DLQ[(S3 Dead Letter <br> Bad Rows)]
    end
    class Spark compute;
    class DLQ database;

    subgraph Storage [3. Refined Lakehouse]
        Silver[(Silver S3 <br> Cleaned Parquet)]
        Gold[(Gold S3 <br> True-Up Deltas)]
    end
    class Silver,Gold s3;

    subgraph Outputs [4. Downstream Actions]
        Athena[Amazon Athena <br> Finance BI]
        State[(DynamoDB Ledger <br> Update State)]
    end
    class Athena compute;
    class State database;

    %% ==========================================
    %% 3. THE DATA FLOW CONNECTIONS
    %% ==========================================
    
    %% Ingestion to Compute
    Airflow -.->|1. Trigger Nightly| Spark
    DSL -.->|2. Load Logic| Spark
    Bronze_Events -->|3a. Read Provisional| Spark
    Bronze_Bank -->|3b. Read Finalized| Spark

    %% Compute to Storage
    Spark -.->|Parse Errors| DLQ
    Spark -->|4. Clean & Match| Silver
    Silver -->|5. Aggregate| Gold

    %% Storage to Outputs
    Gold -->|6. Query Data| Athena
    Gold -.->|7. Reverse ETL Sync| State
```