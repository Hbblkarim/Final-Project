from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, month, max as spark_max
from datetime import datetime
import psycopg2
import pandas as pd
import logging
from pyspark.sql.functions import sum as spark_sum, avg as spark_avg, count as spark_count

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SalesPipeline")

def create_spark_session():
    """Create a PySpark session with necessary configurations."""
    logger.info("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("SalesPipeline") \
        .config("spark.jars", "/home/hadoop/data/postgresql-42.3.1.jar") \
        .config("spark.sql.shuffle.partitions", 8) \
        .getOrCreate()
    return spark

def fetch_data_directly():
    """Extract data from PostgreSQL database."""
    logger.info("Fetching data from PostgreSQL...")
    try:
        conn_params = {
            "dbname": "awal_oltp",
            "user": "postgres",
            "password": "210304",
            "host": "localhost",
            "port": "5432"
        }

        conn = psycopg2.connect(**conn_params)
        query = """
        SELECT invoice_id, branch, city, customer_type, gender, product_line,
               unit_price, quantity, tax_5_percent, total, date, time, payment_
        FROM hasil_transformasi
        """
        source_df = pd.read_sql(query, conn)
        conn.close()

        logger.info(f"Fetched {len(source_df)} rows from PostgreSQL.")
        return source_df

    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise

def process_and_save_data():
    """Transform and load data into the destination database."""
    logger.info("Starting data processing...")
    try:
        # Step 1: Extract
        source_data = fetch_data_directly()
        if source_data.empty:
            logger.warning("No data fetched from PostgreSQL.")
            return

        # Step 2: Transform
        logger.info("Starting data transformation...")
        spark = create_spark_session()
        source_df_spark = spark.createDataFrame(source_data)
        source_df_spark = source_df_spark.cache()  # Cache for performance

        logger.info("Normalizing column names...")
        source_df_spark = source_df_spark.select([
            col(c).alias(c.strip().lower().replace(" ", "_")) for c in source_df_spark.columns
        ])

        logger.info("Adding 'month' column...")
        source_df_spark = source_df_spark.withColumn("month", month(col("date")))

        logger.info("Calculating monthly highest sales...")
        # Group by month to find the highest sales in each month
        monthly_sales = (
            source_df_spark.groupBy("month")
            .agg(spark_max("total").alias("highest_sales"))
            .orderBy(col("month"))
        )

        logger.info("Grouping by product_line for additional insights...")
        product_line_stats = (
            source_df_spark.groupBy("product_line")
            .agg(
                spark_sum("total").alias("total_sales"),
                spark_sum("quantity").alias("total_quantity_sold"),
                spark_avg("unit_price").alias("average_unit_price"),
                spark_count("product_line").alias("transactions_count")
            )
        )

        logger.info("Joining grouped product_line data back to the main dataset...")
        enriched_df = source_df_spark.join(
            product_line_stats,
            on="product_line",
            how="left"
        ).join(
            monthly_sales,
            on="month",
            how="left"
        ).select(
            # Original columns
            col("product_line"),
            col("branch"),
            col("city"),
            col("customer_type"),
            col("gender"),
            col("unit_price"),
            col("quantity"),
            col("tax_5_percent"),
            col("total"),
            col("payment_"),
            col("date"),
            col("time"),
            col("month"),

            # Monthly sales insights
            col("highest_sales"),

            # Product line insights
            col("total_sales"),
            col("total_quantity_sold"),
            col("average_unit_price"),
            col("transactions_count"),

            # Additional metadata
            lit("Generated").alias("report_status"),
            lit(datetime.now().date()).alias("processed_date"),
            lit(datetime.now().strftime("%H:%M:%S")).alias("processed_time")
        )

        logger.info("Saving data to destination database...")
        pg_hook_target = PostgresHook(postgres_conn_id="akhir_conn")
        connection = pg_hook_target.get_connection("akhir_conn")
        db_user = connection.login
        db_password = connection.password
        db_host = connection.host
        db_port = connection.port
        db_name = connection.schema

        connection_uri = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
        enriched_df.write.jdbc(
            url=connection_uri,
            table="hasil_akhir_",
            mode="overwrite",
            properties={
                "user": db_user,
                "password": db_password,
                "driver": "org.postgresql.Driver"
            }
        )

        logger.info("Data successfully saved to the destination database.")
        enriched_df.show(truncate=False)

        spark.stop()
        logger.info("Spark session stopped.")

    except Exception as e:
        logger.error(f"Error during data processing: {e}")
        raise

# Airflow DAG configuration
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 1),
    "retries": 2,
}

dag = DAG(
    "sales_pipeline_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

process_and_save_task = PythonOperator(
    task_id="process_and_save_task",
    python_callable=process_and_save_data,
    dag=dag,
)

process_and_save_task
