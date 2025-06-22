import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs
import sys,os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def staging_to_transactions():
    try:
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASS')
        logger.info("Starting Spark job: staging_to_transactions")

        spark = SparkSession.builder \
                .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
                .appName('Data Quality check and Transformation') \
                .getOrCreate()

        logger.info("Spark session created.")

        jdbc_url = "jdbc:postgresql://host.docker.internal:32768/postgres"
        properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }

        logger.info("Reading data from 'staging_transactions' table.")
        df = spark.read.jdbc(
            url=jdbc_url,
            properties=properties,
            table='staging_transactions'
        )

        logger.info("Filtering valid records.")
        clean_df = df.filter(
            (col('item').isNotNull() & (~col('item').isin('ERROR', 'UNKNOWN'))) &
            (col('quantity').isNotNull() & (~col('quantity').isin('ERROR', 'UNKNOWN'))) &
            (col('price_per_unit').isNotNull() & (~col('price_per_unit').isin('ERROR', 'UNKNOWN'))) &
            (col('total_spent').isNotNull() & (~col('total_spent').isin('ERROR', 'UNKNOWN'))) &
            (col('payment_method').isin(["Cash", "Credit Card", "Digital Wallet"])) &
            (col('location').isNotNull() & (~col('location').isin('ERROR', 'UNKNOWN'))) &
            (col('transaction_date').isNotNull() & (~col('transaction_date').isin('ERROR', 'UNKNOWN')))
        )
        typed_df = clean_df.withColumn('quantity', col('quantity').cast('int')) \
                           .withColumn('price_per_unit', col('price_per_unit').cast('float')) \
                           .withColumn('total_spent', col('total_spent').cast('float')) \
                           .withColumn('transaction_date', col('transaction_date').cast('date'))
        
        logger.info("Filtering and writing valid records to 'final_transactions'.")
        final_df = typed_df.filter(
                (col('quantity') > 0) & 
                (abs(col('total_spent') - 
                    (col('price_per_unit') * col('quantity'))) < 0.01)
            )
        final_df.write.jdbc(
            url=jdbc_url,
            table='final_transactions',
            mode='overwrite',
            properties=properties
        )
        # Subtract method will return all the rows that are present in df but not in valid_df
        # It can be used for small to medium dataset but is not optimal for large datasets because
        # it will compare each row, so shuffle will be happen as not necessary both the rows from 
        # df and valid_df will be on same node. Also we compare entire two rows which also a performance
        # bottleneck

        #errorneous_df = df.subtract(valid_df)

        # left_anti join gives all the data present in left df and not present in the right one
        logger.info("Identifying erroneous records using left anti join.")
        erroneous_df = df.join(final_df, on='transaction_id', how='left_anti')

        logger.info("Writing erroneous records to 'erroneous_transactions' table.")
        erroneous_df.write.jdbc(
            url=jdbc_url,
            properties=properties,
            table='erroneous_transactions',
            mode='append'
        )

        logger.info("Clearing 'staging_transactions' table.")
        empty_df = spark.createDataFrame([], schema=df.schema)
        empty_df.write.jdbc(
            url=jdbc_url,
            table='staging_transactions',
            mode='overwrite',
            properties=properties
        )

        logger.info("Spark job completed successfully.")

    except Exception as e:
        logger.exception("An error occurred during staging_to_transactions execution.")
        sys.exit(1)

    finally:
        if spark:
            try:
                spark.stop()
                logger.info("Spark session stopped.")
            except Exception:
                logger.warning("Failed to stop Spark session cleanly.")


        
if __name__ == '__main__':
    staging_to_transactions()
