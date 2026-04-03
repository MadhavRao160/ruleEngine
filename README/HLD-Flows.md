# Expense Policy Rules Engine - Visual Architecture Flows

This document contains detailed architectural diagrams for the Unified Platform.

## 1. The Master Data Map (Lambda Architecture)
This shows the macro-level split between the Speed Layer and the Batch Layer, unified by the Shared Core.

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
    
    subgraph Clients [Clients & Sources]
        Mobile[Mobile / Web App]
        Partner["Partner Banks data (in TB, in CSVformat)"]
    end
    class Mobile,Partner external;

    subgraph Edge [Edge & Ingestion]
        API_GW[AWS API Gateway]
        Pre_Signed[S3 Pre-Signed URL Controller]
    end
    class API_GW,Pre_Signed router;

    subgraph Speed_Layer [Speed Layer: Real-Time Synchronous]
        FastAPI[FastAPI Service <br> AST Evaluator]
        Kafka[Apache Kafka Event Stream]
    end
    class FastAPI compute;
    class Kafka stream;

    subgraph DynamoDB [Amazon DynamoDB: The Physical Shared Core]
        State[(Budget Utilization Ledger <br> Real-Time Balances)]
        DSL[(Rules Table <br> JSON AST Policies)]
    end
    class State database;
    class DSL core; 

    subgraph Batch_Layer [Batch Layer: Asynchronous Heavy]
        Airflow[Apache Airflow Orchestration]
        Spark[Apache Spark Cluster <br> Native PySpark SQL & Windowing]
        DLQ[(S3 Dead Letter Queue)]
    end
    class Airflow,Spark compute;
    class DLQ database;

    subgraph Storage [Lakehouse Storage]
        Bronze[(Bronze S3 Raw Data)]
        Silver[(Silver S3 Clean Data)]
        Gold[(Gold S3 / Iceberg Aggregates)]
        Athena[Amazon Athena Finance Dashboard]
    end
    class Bronze,Silver,Gold s3;
    class Athena external;

    %% ==========================================
    %% 3. DEFINE CONNECTIONS
    %% ==========================================
    Mobile -->|JSON| API_GW
    API_GW --> FastAPI
    
    Partner -->|Request Upload| Pre_Signed
    Partner -->|Direct PUT| Bronze
    
    %% Speed Layer State & Logic Flow
    FastAPI <-->|Fetch/Update Totals| State
    DSL -.->|JSON Policy Data Flow| FastAPI
    
    FastAPI -->|Fire & Forget| Kafka
    Kafka -->|Stream Load| Bronze

    Airflow -->|Triggers Daily| Spark
    Bronze -->|Reads 4TB| Spark
    
    %% Batch Layer State & Logic Flow
    DSL -.->|JSON Policy Data Flow| Spark
    
    Spark -->|Bad Rows| DLQ
    Spark -->|Clean Results| Silver
    Silver --> Gold
    Gold --> Athena
    Gold -.->|Nightly State Sync| State
```

## 2: The Ingestion "Claim Check" Pattern.
This sequence diagram illustrates how we prevent 1TB+ partner files from crashing our synchronous web servers.

```mermaid
sequenceDiagram
    autonumber
    
    %% Define Participants
    actor Partner as Partner Bank System
    participant API as AWS API Gateway
    participant FA as FastAPI (Intelligent Router)
    participant S3 as AWS S3 (Bronze Bucket)

    Note over Partner, FA: Step 1: Requesting the "Ticket" (Micro-Payload)
    Partner->>API: POST /v1/upload-request {size: few TB, partner: "Amex"}
    API->>FA: Route Request
    FA-->>FA: Authenticate & Validate Payload Size
    
    Note over FA, S3: FastAPI asks AWS for a secure, temporary upload URL
    FA->>S3: Request Pre-Signed URL (Action: PUT, Expiry: 15m)
    S3-->>FA: Return cryptographic URL string
    FA-->>API: HTTP 200 OK {upload_url: "https://s3.aws.com/..."}
    API-->>Partner: HTTP 200 OK {upload_url: "https://s3.aws.com/..."}

    Note over Partner, S3: Step 2: The Actual Upload (Macro-Payload)<br/>Notice that FastAPI and API Gateway are no longer involved.
    
    Partner->>S3: HTTP PUT /1TB_Amex_Data.csv (using upload_url)
    S3-->>S3: Validate Cryptographic Signature & Expiry
    S3-->>Partner: HTTP 200 OK (File safely stored)
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
    
    User_In((Mobile App <br> Request))
    class User_In external;

    API[AWS API Gateway]
    class API compute;

    FA[FastAPI Service <br> AST Evaluator]
    class FA compute;

    User_Out((Mobile App <br> Response))
    class User_Out external;

    %% Main Request-Response Spine
    User_In -->|1. POST| API
    API -->|2. Route| FA
    FA -->|5. HTTP 200| User_Out

    %% ==========================================
    %% 3. THE SHARED CORE (Vertical Axis)
    %% ==========================================
    
    subgraph DynamoDB [Amazon DynamoDB: The Physical Shared Core]
        direction TB
        DSL[(Rules Table <br> JSON AST Policies)]
        State[(Budget Utilization Ledger <br> Real-Time Balances)]
    end
    class State database;
    class DSL core;

    %% Database Interactions (Hanging vertically off FastAPI)
    DSL -.->|4. Fetch Rules| FA
    FA <-->|3. Hydrate & Update| State

    %% ==========================================
    %% 4. ASYNC OUTPUT (Off-axis)
    %% ==========================================
    
    Kafka[Apache Kafka <br> Event Stream]
    class Kafka stream;

    FA -.->|6. Async Event| Kafka

    %% ==========================================
    %% 5. LAYOUT TWEAKS (Invisible Symmetry)
    %% ==========================================
    %% Force Kafka to stay to the right of the DB block
    DynamoDB ~~~ Kafka
```

## 4. The Batch Layer (Data Lakehouse ETL)
This diagram shows the asynchronous orchestration of massive historical data through the Medallion architecture (Bronze, Silver, Gold).

```mermaid
graph TD
    %% ==========================================
    %% 1. STYLE DEFINITIONS
    %% ==========================================
    classDef compute fill:#a8dadc,stroke:#1d3557,stroke-width:1px,color:#000;
    classDef s3 fill:#fefae0,stroke:#bc6c25,stroke-width:1px,color:#000;
    classDef core fill:#2a9d8f,stroke:#264653,stroke-width:2.5px,color:#fff;
    classDef database fill:#f1faee,stroke:#1d3557,stroke-width:1px,color:#000;
    classDef airflow fill:#e1f5fe,stroke:#01579b,stroke-width:1px,color:#000;

    %% ==========================================
    %% 2. TOP LEVEL: INGESTION & CONTROL
    %% ==========================================
    subgraph Control_Zone ["1. Control Plane"]
        Airflow[Apache Airflow <br> Orchestrator]
    end
    class Airflow airflow;

    Bronze[(Bronze S3 <br> 4TB Raw CSV)]
    class Bronze s3;

    %% ==========================================
    %% 3. MIDDLE LEVEL: COMPUTE & RULES
    %% ==========================================
    subgraph Core_Zone ["2. Shared Core Logic"]
        DSL[(Rules Table <br> JSON AST Policies)]
    end
    class DSL core;

    Spark[Spark Cluster <br> Native SQL & Windowing]
    class Spark compute;

    DLQ[(S3 Dead Letter <br> Bad Rows)]
    class DLQ database;

    %% ==========================================
    %% 4. BOTTOM LEVEL: REFINEMENT & STATE
    %% ==========================================
    Silver[(Silver S3 <br> Cleaned Parquet)]
    class Silver s3;

    Gold[(Gold S3 <br> Budget Aggregates)]
    class Gold s3;

    subgraph State_Zone ["3. Ledger Persistence"]
        State[(Budget Utilization Ledger <br> Real-Time Balances)]
    end
    class State database;

    Athena[Amazon Athena <br> BI & Finance]
    class Athena compute;

    %% ==========================================
    %% 5. THE VERTICAL CONNECTIONS (THE SPINE)
    %% ==========================================
    
    %% Main Data Downward Flow
    Bronze -->|3. Load| Spark
    Spark -->|5. Transform| Silver
    Silver -->|6. Aggregate| Gold
    Gold -->|7. Query| Athena

    %% Orchestration (Inward from Left)
    Airflow -->|1. Sensor/Scheduled| Bronze
    Airflow -->|2. Provision| Spark

    %% Logic Ingestion (Inward from Right)
    DSL -.->|4. Fetch Rules| Spark

    %% Error Routing (Outward to Left)
    Spark -.->|DLQ| DLQ

    %% Reconciliation (Inward from Bottom-Right)
    Gold -.->|8. Nightly State Sync| State
```