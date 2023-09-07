# # 1. 계열사별 + 년 + 월 + 일 + 시간 + 요일 + 매출 건수 + 매출 금액
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, substring, count, sum, udf

from datetime import datetime
import json
import boto3
from botocore.exceptions import ClientError

# @params: [JOB_NAME]
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
purchase_all_info_df = purchase_all_info_df.withColumn("purchase_day", substring(col("purchase_date"),7,2))

purchase_all_info_df.cache()

purchase_all_info_df.show(3)

## 계열사, 년, 월, 일, 시간 + 매출 건수, 매출 금액 

grouped_purchase_df = purchase_all_info_df.groupBy(['affiliate', 'purchase_year', 'purchase_month', 'purchase_day', 'purchase_time'])\
                                            .agg(sum("amount").alias("total_purchase_amount"), count("amount").alias("count_of_purchase"))

## Group By  결과물에 요일 추가 
def change_day_of_week(year: str, month: str, day: str):
    try:
        date_string = f"{year}{month}{day}"
        # Convert the date string to a datetime object
        date_object = datetime.strptime(date_string, '%Y%m%d')
        # Get the day of the week as an integer (0 = Monday, 6 = Sunday)
        day_of_week = date_object.weekday()
        # Convert the integer to the corresponding day name
        day_name = date_object.strftime('%A')
        return day_name
    except ValueError:
        return "Invalid date format. Please"

#print(change_day_of_week('2014','11','12') == 'Wednesday')

change_day_of_week_udf = udf(change_day_of_week)

grouped_purchase_df = grouped_purchase_df.withColumn("day_of_week", change_day_of_week_udf(col("purchase_year"), col("purchase_month"), col("purchase_day")))

grouped_purchase_df = grouped_purchase_df.select('affiliate','purchase_year','purchase_month','purchase_day','day_of_week','purchase_time','total_purchase_amount','count_of_purchase')

output_path = output_path = f's3://blee-lab/glue/data/fact/gold/mart_salesbydatetime/'
grouped_purchase_df.coalesce(1).write.mode('overwrite').partitionBy(['affiliate','purchase_year','purchase_month']).parquet(output_path)

job.commit()