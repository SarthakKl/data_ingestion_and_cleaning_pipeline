from airflow.sensors.base import BaseSensorOperator
import boto3,os

class MinioFileSensor(BaseSensorOperator):
    def __init__(self, bucket, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.key = key

    def poke(self, context):
        access_key=os.getenv('MINIO_USER')
        secret_key=os.getenv('MINIO_PASS')
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio1:9000',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        try:
            self.log.info(f"Checking for s3://{self.bucket}/{self.key}")
            s3.head_object(Bucket=self.bucket, Key=self.key)
            self.log.info('File exists!')
            return True 
        except Exception as e:
            self.log.warning(f"File not found or access denied: {e}")
            return False
