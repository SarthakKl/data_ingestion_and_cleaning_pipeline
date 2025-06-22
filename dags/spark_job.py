import os, boto3
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def import_csv_to_staging():
    try:
        logger.info("Starting Spark job: import_csv_to_staging")

        access_key = os.getenv("AWS_ACCESS_KEY")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASS')

        if not access_key or not secret_key:
            raise ValueError("Missing AWS credentials in environment variables.")

        spark = SparkSession.builder \
            .appName("import_to_staging") \
            .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.4.1.jar,"
                                  "/opt/spark/jars/bundle-2.31.65.jar,"
                                  "/opt/spark/jars/postgresql-42.6.0.jar") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000") \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.strict", "true") \
            .getOrCreate()

        logger.info("Spark session created successfully.")

        # Define schema
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("item", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("price_per_unit", StringType(), True),
            StructField("total_spent", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("location", StringType(), True),
            StructField("transaction_date", StringType(), True)
        ])

        # Load data from S3 (MinIO)
        logger.info("Reading data from S3 bucket...")
        df = spark.read.format('csv') \
            .option('header', 'true') \
            .option('delimiter', ',') \
            .schema(schema) \
            .load('s3a://daily-data/input.csv')

        logger.info("Data loaded successfully. Showing schema and preview:")
        df.printSchema()
        df.show(5)

        # JDBC configuration
        jdbc_url = "jdbc:postgresql://host.docker.internal:32768/postgres"
        properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }

        # Write to PostgreSQL
        logger.info("Writing data to PostgreSQL staging table...")
        df.write.jdbc(
            url=jdbc_url,
            table="staging_transactions",
            mode="overwrite",
            properties=properties
        )
        logger.info("Deleting the input file from daily-data bucket")
        delete_file_from_bucket()

        logger.info("Data written to PostgreSQL successfully.")

    except Exception as e:
        logger.exception("An error occurred during the Spark job.")
        raise

    finally:
        try:
            spark.stop()
            logger.info("Spark session stopped.")
        except Exception:
            logger.warning("Spark session could not be stopped cleanly.")

def delete_file_from_bucket():
    access_key=os.getenv('MINIO_USER')
    secret_key=os.getenv('MINIO_PASS')
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio1:9000',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    try:
        s3.delete_object(Bucket='daily-data', Key='input.csv')
    except Exception:
        logging.error('Error while deleting input.csv from bucket')
        
if __name__ == '__main__':
    import_csv_to_staging()
