from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from fpdf import FPDF
from datetime import datetime
from pdf_generator import generate_pdf_report
import os


spark = SparkSession.builder.appName("FinTech_Batch_Final_ETL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("location", StringType(), True)
])



raw_kafka_df = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe", "transactions") \
    .option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
    
parsed_raw_df = raw_kafka_df.select( F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")

fraud_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://postgres:5432/airflow").option("dbtable", "fraud_alerts") \
    .option("user", "airflow").option("password", "airflow").option("driver", "org.postgresql.Driver").load()


high_value = (fraud_df.fraud_type == "High Value (> 5000)") & (parsed_raw_df.timestamp == fraud_df.timestamp)
time_window_start = fraud_df.timestamp - F.expr("INTERVAL 10 MINUTES")
impossible_travel = (fraud_df.fraud_type == "Impossible Travel") & (parsed_raw_df.timestamp <= fraud_df.timestamp) & (parsed_raw_df.timestamp >= time_window_start)
fraud_type_match = (high_value | impossible_travel)

join_conditions = (parsed_raw_df.user_id == fraud_df.user_id) & fraud_type_match

validated_df = parsed_raw_df.join(fraud_df, on=join_conditions, how="left_anti")
fraud_transactions_df = parsed_raw_df.join(fraud_df, on=join_conditions, how="inner") \
    .select(parsed_raw_df["*"], fraud_df["fraud_type"])



validated_df.write.mode("append").parquet("/tmp/data_warehouse/validated_transactions")
fraud_transactions_df.write.mode("append").parquet("/tmp/data_warehouse/fraud_transactions")

total_stats = parsed_raw_df.select(
    F.count("*").alias("count"),
    F.sum("amount").alias("amount")
).collect()[0]

valid_stats = validated_df.select(
    F.count("*").alias("count"),
    F.sum("amount").alias("amount")
).collect()[0]


merchant_fraud_summary = fraud_transactions_df.groupBy("merchant_category") .agg( F.count("*").alias("fraud_count"),F.sum("amount").alias("total_fraud_amount")) \
    .orderBy(F.col("total_fraud_amount").desc())
    

total_amt = total_stats['amount'] or 0.0
valid_amt = valid_stats['amount'] or 0.0

final_stats = {
    'total_count': total_stats['count'],
    'total_amount': total_amt,
    'valid_amount': valid_amt,
    'fraud_amount': total_amt - valid_amt
}


category_stats_rows = merchant_fraud_summary.collect()
merchant_fraud_data = []
for row in category_stats_rows:
    merchant_fraud_data.append({
        "category": row["merchant_category"],
        "count": row["fraud_count"],
        "amount": row["total_fraud_amount"]
    })
    

generate_pdf_report(final_stats,merchant_fraud_data, "/opt/airflow/scripts/reconciliation_report.pdf")

spark.stop()


