from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Transaction',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    
    'Reconciliation_batch',
    default_args=default_args,
    schedule_interval='0 */6 * * *',
    catchup=False,
)


run_etl_job = SparkSubmitOperator(
    task_id='run_reconciliation_and_pdf_generation',
    application='/opt/airflow/scripts/ETL.py',
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2',
    conn_id='spark_default',
    name='FintechBatchETL',
    verbose=True,
    conf={
        
        "spark.master": "spark://spark-master:7077",
        "spark.executor.memory": "1g",
        "spark.driver.memory": "1g"
    },
    dag=dag,
)


run_etl_job