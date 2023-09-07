import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, substring

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
# -- DIMENSION : ZIPCODE, CUSTOMER, PRODUCTS_INFO
# --

# glue catalog에서 glue dynamic frame으로 데이터를 읽은 후 Spark DataFrame으로 변환
customer_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database=glue_database_name
                                                            , table_name = 'customer')
customer_df = customer_dynamic_frame.toDF()

zipcode_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database=glue_database_name
                                                            , table_name = 'zipcode')
zipcode_df = zipcode_dynamic_frame.toDF()

products_info_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database=glue_database_name
                                                            , table_name = 'products_info')
products_info_df = products_info_dynamic_frame.toDF()

# Customer + Zipcode Jon

customer_with_zipcode_df = customer_df.join(zipcode_df, customer_df['residence'] == zipcode_df['short_zipcode'], "left")\
                                        .drop(zipcode_df.short_zipcode)
#customer_with_zipcode_df.show(3)

# --
# -- FACT : PURCHASE
# --

purchase_year = '2014'
purchase_from_month = '01'
purchase_to_month = '12'

push_down_predicate = f"purchase_year={purchase_year} and purchase_month between {purchase_from_month} and {purchase_to_month}"

purchase_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database = glue_database_name, 
                                                                    table_name = 'purchase',
                                                                    push_down_predicate = f"({push_down_predicate})")
purchase_df = purchase_dynamic_frame.toDF()

purchase_product_df = purchase_df.join(products_info_df
                                , (purchase_df['affiliate'] == products_info_df['affiliate']) &
                                    (purchase_df['division_cd'] == products_info_df['division_cd']) & 
                                    (purchase_df['main_category_cd'] == products_info_df['main_category_cd']) &
                                    (purchase_df['sub_category_cd'] == products_info_df['sub_category_cd'])
                                , "left")\
                                    .drop(products_info_df.affiliate)\
                                    .drop(products_info_df.division_cd)\
                                    .drop(products_info_df.main_category_cd)\
                                    .drop(products_info_df.sub_category_cd)

purchase_full_df = purchase_product_df.join(customer_with_zipcode_df
                                        , purchase_product_df['customer_id'] == customer_with_zipcode_df['customer_id']
                                        , "left")\
                                        .drop(customer_with_zipcode_df.customer_id)

#purchase_full_df.repartition(2)

output_path = output_path = f's3://blee-lab/glue/data/fact/silver/purchase_all_info/'
purchase_full_df.coalesce(1).write.mode('overwrite').partitionBy(['affiliate','purchase_year', 'purchase_month']).parquet(output_path)

job.commit()