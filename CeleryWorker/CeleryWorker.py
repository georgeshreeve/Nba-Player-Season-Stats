from celery import Celery
import redis
import boto3
from datetime import datetime, timedelta
import pytz
import os

access_key = os.environ.get('AWS_ACCESS_KEY_ID')
secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
region = os.environ.get('REGION_NAME')
bucket = os.environ.get('AWS_BUCKET')
bucket_prefix = os.environ.get('BUCKET_PREFIX')

app = Celery('CeleryWorker', broker='redis://redis:6379/0', backend='redis://redis:6379/0')

utc = pytz.UTC

class Helpers:
    def S3Client():
        client = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret_access_key, region_name = region)
        return client

@app.task()
def DeleteOldQueries():
    delta = utc.localize(datetime.utcnow() + timedelta(hours = - 24))
    client = Helpers.S3Client()
    keys = []
    response = client.list_objects(Bucket = bucket, Prefix = bucket_prefix)
    for obj in response['Contents']:
        if obj['LastModified'] < delta:
            keys.append(obj['Key'])
    for key in keys:
        try:
            client.delete_object(Bucket = bucket, Key = key)
        except Exception as e:
            pass


