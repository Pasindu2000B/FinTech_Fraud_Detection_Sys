# FinTech_Fraud_Detection_Sys

An end-to-end, production-grade data pipeline built on the **Lambda Architecture** to process financial transactions, detect fraudulent activities in real-time, and orchestrate batch reconciliation reporting.

## 📌 Architecture Overview

This project implements a robust **Lambda Architecture** with two distinct processing paths:
1. **Speed Layer (Real-time):** Utilizes **Apache Kafka** and **Spark Structured Streaming** to ingest transaction streams, evaluate fraud rules (e.g., High-Value transactions, Impossible Travel), and instantly write alerts to a PostgreSQL database.
2. **Batch Layer (Historical/Reconciliation):** Utilizes **Apache Airflow** to trigger periodic ETL jobs that join raw transaction logs with fraud alerts, archive validated (safe) transactions into a Data Warehouse (Parquet format), and generate automated PDF reconciliation reports.

## 🛠️ Tech Stack

* **Message Broker:** Apache Kafka (KRaft mode)
* **Stream & Batch Processing:** Apache Spark (PySpark)
* **Orchestration:** Apache Airflow
* **Database:** PostgreSQL
* **Storage:** Local Data Warehouse (Parquet)
* **Containerization:** Docker & Docker Compose
* **Reporting:** Python (`fpdf`)

## 💡 Key Features & Fraud Rules

* **Real-time Ingestion:** Continuous consumption of JSON transaction data from Kafka topics.
* **Complex Event Processing:** * **High Value Fraud:** Flags transactions exceeding $5,000.
  * **Impossible Travel:** Detects transactions from the same user in different countries within a 10-minute window using Spark Watermarking and Windowing.
* **Data Warehousing:** Separates validated safe transactions and stores them in optimized Parquet format for downstream analytics.
* **Automated Reconciliation:** An Airflow DAG scheduled every 6 hours generates a comprehensive PDF report detailing total ingress volume vs. validated volume, along with a breakdown of fraud attempts by merchant category.

## ⚙️ Getting Started

### Prerequisites
* Docker Desktop
* Docker Compose
* Python 3.8+ (for local scripts)

### Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Pasindu2000B/FinTech_Fraud_Detection_Sys.git
   cd FinTech-Fraud-Detection-Pipeline

2. **Docker Compose**
   ```bash
   docker-compose up -d

3. **Start Streaming Job**
   ```bash
   docker exec -it spark-master /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 /opt/spark-apps/fraud_streaming.py


 4. **Generate Data**
   ```bash

 5 **Access Airflow & Generate Report**

    *Navigate to http://localhost:8082 (Login: airflow / airflow).
    *Unpause and trigger the Reconciliation_batch DAG.
    *Once the task is green, your PDF report will be waiting in the /scripts folder.  python Transaction.py




