# # 4. 계열사별 + 연령별 + 제품(카테고리)별 매출
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
# -- FACT : purchase
# --

purchase_year = '2014'
purchase_from_month = '01'
purchase_to_month = '12'

push_down_predicate = f"purchase_year={purchase_year} and purchase_month between {purchase_from_month} and {purchase_to_month}"

purchase_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database = glue_database_name, 
                                                                    table_name = 'purchase',
                                                                    push_down_predicate = f"({push_down_predicate})")

purchase_df = purchase_dynamic_frame.toDF()

purchase_df.cache()
purchase_df.show(3)

grouped_purchase_df = purchase_df.groupBy(['customer_id', 'affiliate', 'purchase_year','purchase_month'])\
            .agg(sum("amount").alias("total_purchase_amount"), count("amount").alias("count_of_purchase"))

grouped_purchase_df.show(3)

# ## Customer + Zipcode Info Join

# glue catalog에서 glue dynamic frame으로 데이터를 읽은 후 Spark DataFrame으로 변환
customer_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database=glue_database_name
                                                            , table_name = 'customer')
customer_df = customer_dynamic_frame.toDF()

zipcode_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database=glue_database_name
                                                            , table_name = 'zipcode')
zipcode_df = zipcode_dynamic_frame.toDF()

# Customer + Zipcode Jon

customer_with_zipcode_df = customer_df.join(zipcode_df, customer_df['residence'] == zipcode_df['short_zipcode'], "left")\
                                        .drop(zipcode_df.short_zipcode)

grouped_purchase_full_df = grouped_purchase_df.join(customer_with_zipcode_df
                                        , grouped_purchase_df['customer_id'] == customer_with_zipcode_df['customer_id']
                                        , "left")\
                                        .drop(customer_with_zipcode_df.customer_id)

grouped_purchase_full_df.show(3)

output_path = output_path = f's3://blee-lab/glue/data/fact/gold/mart_salesbycustomer/'
grouped_purchase_full_df.coalesce(1).write.mode('overwrite').partitionBy(['affiliate','purchase_year','purchase_month']).parquet(output_path)

job.commit()