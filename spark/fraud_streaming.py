from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit,col, window,from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("FinTechFraudDetection").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("location", StringType(), True)
])

kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .load()
    
string_df = kafka_df.select(F.col("value").cast("string"))

value_column = F.col("value")
parsed_column = F.from_json(value_column, schema)
dataa = parsed_column.alias("data")

data_frame_struct = string_df.select(dataa)
parsed_df = data_frame_struct.select("data.*")
                           
aboveFiveThousand_df = parsed_df.filter("amount > 5000")
fraud_type_column_add_df = aboveFiveThousand_df.withColumn("fraud_type", lit("High Value (> 5000)"))
high_value_alerts_df = fraud_type_column_add_df.select("user_id", "amount", "location", "timestamp", "fraud_type")
       
watermarked_df = parsed_df.withWatermark("timestamp", "10 minutes")
grouped_data = watermarked_df.groupBy(window(col("timestamp"), "10 minutes"),col("user_id"))
aggregated_df = grouped_data.agg(F.approx_count_distinct("location").alias("location_count"),F.sum("amount").alias("total_amount"))
fraudulent_users_df = aggregated_df.filter(col("location_count") > 1)

travel_fraud = fraudulent_users_df.select(
    F.col("user_id"),
    F.col("total_amount").alias("amount"),
    F.lit("Multiple Locations").alias("location"),
    F.col("window.end").alias("timestamp"),
    F.lit("Impossible Travel").alias("fraud_type")
)

def write_to_postgres(df, batch_id):
    
    try:
        if df.storageLevel.useMemory == False:
            df.persist()
        if not df.isEmpty():
            print(f"Batch: {batch_id} | Records: {df.count()}")
            df.write.format("jdbc").option("url", "jdbc:postgresql://postgres:5432/airflow").option("dbtable", "fraud_alerts").option("user", "airflow").option("password", "airflow") \
                .option("driver", "org.postgresql.Driver").mode("append").save()
        df.unpersist()
    except Exception as e:
        print(f"Error saving to Postgres: {e}")
            
query1 = high_value_alerts_df.writeStream.foreachBatch(write_to_postgres).trigger(processingTime='60 seconds').option("checkpointLocation", "/tmp/checkpoint_high_value").start()

query2 = travel_fraud.writeStream.foreachBatch(write_to_postgres).trigger(processingTime='60 seconds').option("checkpointLocation", "/tmp/checkpoint_travel").start()

print("FinTech Fraud Detection Start")
spark.streams.awaitAnyTermination()