# # 계열사별, 지역별, 매출

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, substring, count, sum

import json
import boto3
from botocore.exceptions import ClientError

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --
# -- Overwrite setting
# --
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")  #  없으면 전체 Partition이 overwrite 된다 

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

hadoop_conf = glueContext._jsc.hadoopConfiguration()
hadoop_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")  # SUCCESS 폴더 생성 방지
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # $folder$ 폴더  생성 방지 

glue_database_name = "de-enhancement-db"

# --
# -- FACT : purchase_all_info
# --

purchase_year = '2014'
purchase_from_month = '01'
purchase_to_month = '12'

push_down_predicate = f"purchase_year={purchase_year} and purchase_month between {purchase_from_month} and {purchase_to_month}"

purchase_all_info_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database = glue_database_name, 
                                                                    table_name = 'purchase_all_info',
                                                                    push_down_predicate = f"({push_down_predicate})")

purchase_all_info_df = purchase_all_info_dynamic_frame.toDF()
purchase_all_info_df.cache()

purchase_all_info_df.show(3)

residence_grouped_purchase_df = purchase_all_info_df.groupBy(['affiliate', 'purchase_year', 'purchase_month','province_city', 'city_county'])\
                                            .agg(sum("amount").alias("total_purchase_amount"), count("amount").alias("count_of_purchase"))\
                                            .sort(col("affiliate"), col('purchase_year'), col('purchase_month'), col('total_purchase_amount').desc(), col('count_of_purchase').desc())

residence_grouped_purchase_df.show(3)

# month 까지 partition을 나누게 되면 1개 파일이 2.4k 로 되서 오히려 Read/Write Overhead가 발생
# 사실상 현재 파일 크기로 봤을떈 그냥 1개 파일로 만들어도되나 추후 데이터가 늘어난다는 전제하에 Partition Split
output_path = output_path = f's3://blee-lab/glue/data/fact/gold/mart_salesbyresidence/'
residence_grouped_purchase_df.coalesce(1).write.mode('overwrite').partitionBy(['affiliate','purchase_year','purchase_month']).parquet(output_path)

job.commit()