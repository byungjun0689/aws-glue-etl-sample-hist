import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json

import boto3
from botocore.exceptions import ClientError

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

hadoop_conf = glueContext._jsc.hadoopConfiguration()
hadoop_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")  # SUCCESS 폴더 생성 방지
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # $folder$ 폴더  생성 방지 

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def get_secret():

    secret_name = "de-enhancement-postgresql-secretsmanager"
    region_name = "ap-northeast-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        return None

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return secret

secret_json_str = get_secret()
secret_json = json.loads(secret_json_str)

db_host = secret_json['host']
db_username = secret_json['username']
db_password = secret_json['password']
db_port = secret_json['port']

db_url = f'jdbc:postgresql://{db_host}:{db_port}/postgres'

db_schema = 'retail'

dimension_table_list = ['customer', 'products_info', 'zipcode']

for table_name in dimension_table_list:
    customer_df = spark.read.format("jdbc") \
        .option("url",db_url) \
        .option("dbtable",  f"{db_schema}.{table_name}") \
        .option("user",db_username) \
        .option("password",db_password) \
        .load()
    
    output_path = f's3://blee-lab/glue/data/dimension/{table_name}/'
    customer_df.write.mode('overwrite').parquet(output_path) # customer_df.write.mode('overwrite').partitionBy(['sex','age_group']).parquet(output_path)

job.commit()