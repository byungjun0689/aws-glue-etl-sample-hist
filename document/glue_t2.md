# 5. Glue

💡 [AWS Glue란?](https://www.notion.so/AWS-Glue-21a4e620cac84c54a1960d5f7d801697?pvs=21)

💡 `공통`
IAM Role : `DE-CF-GlueExecute-Role`
- SecretsManager Read/Write
- S3 Full Access
- Glue Service Role

AWS Glue 에서 Job, Crawler를 실행 시킬 때 필요한 Role 을 사전 생성.
        
## 5.6 [Glue Job] T2, Gold Data


💡 대부분의 Gold Data의 경우 Summary된 형태의 데이터를 많이 생성한다. 
GroupBy, Sum, Avg 등 BI 또는 리포트에 맞는 형태로 데이터를 변환하여 적재 수행
해당 실습은 데이터 분석을 목적으로 하는게 아니라 엔지니어링을 목적으로 하기에 간단한 지표만 생성할 예정

### 5.6.1 주제

> 해당 주제는 임의로 정의한 주제로, 추후 개별적으로 실습하실때는 다르게 주제를 잡아서 수행해도 됩니다.
> 
1. 계열사
    1. 년, 월, 일 매출, 건수
        - `SQL`
            
            ```sql
            SELECT 
                "affiliate"
                , "purchase_year"
                , "purchase_month"
                , round(sum(amount) / 1000000,2) as sum_of_amount
                , count(amount) as cnt_purchase 
                FROM "AwsDataCatalog"."de-enhancement-db"."purchase_all_info"
            group by "affiliate", "purchase_year", "purchase_month"
            order by affiliate, purchase_year, purchase_month
            ```
            
    2. 요일, 시간 매출, 건수
        - `SQL`
            
            ```sql
            -- Monday 1, Tuesday 2, Wednesday 3, Thursday 4, Friday 5, Saturday 6, Sunday 7
            SELECT 
                "affiliate"
                , date_format(date_parse(purchase_date, '%Y%m%d'), '%W') as day_of_week
                , day_of_week(date_parse(purchase_date, '%Y%m%d')) as day_of_week_num
                , "purchase_time"
                , round(sum(amount) / 1000000,2) as sum_of_amount
                , count(amount) as cnt_purchase 
                FROM "AwsDataCatalog"."de-enhancement-db"."purchase_all_info"
            group by 1,2,3,4
            order by 1,3,4
            ```
            
    3. 제품 (카테고리) 별 매출
        - `SQL`
            
            ```sql
            SELECT 
                "affiliate"
                , "division_cd"
                , "main_category_desc"
                , "sub_category_desc"
                , round(sum(amount) / 1000000,2) as sum_of_amount
                , count(amount) as cnt_purchase 
            FROM "AwsDataCatalog"."de-enhancement-db"."purchase_all_info"
            Group by 1,2,3,4
            order by 1,2,3,4
            ```
            
    4. 연령대별, 제품(카테고리)별 매출
        - `SQL`
            
            ```sql
            SELECT 
                "affiliate"
                ,"age_group"
                , "division_cd"
                , "main_category_desc"
                , "sub_category_desc"
                , round(sum(amount) / 1000000,2) as sum_of_amount
                , count(amount) as cnt_purchase 
            FROM "AwsDataCatalog"."de-enhancement-db"."purchase_all_info"
            Group by 1,2,3,4,5
            order by 1,2,6 desc
            ```
            
    5. 지역별 매출
        - `SQL`
            
            ```sql
            SELECT 
                "affiliate"
                ,"province_city"
                ,"city_county"
                , round(sum(amount) / 1000000,2) as sum_of_amount
                , count(amount) as cnt_purchase 
            FROM "AwsDataCatalog"."de-enhancement-db"."purchase_all_info"
            Group by 1,2,3
            order by 1,4 desc
            ```
            
2. 고객
    1. 계열사, 전체 구매 건수, 고객 수, 평균 구매 금액, 평균 구매 횟수
        - `SQL`
            
            ```sql
            SELECT 
                "affiliate"
                , count(distinct "customer_id") as number_of_customer
                , count("customer_id") as cnt_of_purchase
                , count("customer_id") / count(distinct "customer_id") as avg_number_of_purchase
                , round(sum("amount") / count("customer_id") / 10000,2) as avg_purchase_amount
                , round(sum("amount") / count(distinct "customer_id") / 10000,2) as avg_purchase_amount_per_customer
            FROM "AwsDataCatalog"."de-enhancement-db"."purchase_all_info"
            Group by 1
            Order by 1
            ```
            
    2. 계열사, 년, 월 별 구매 건수, 구매 금액
        - `SQL`
            
            ```sql
            SELECT 
                "affiliate"
                , "customer_id"
                , "purchase_year"
                , "purchase_month"
                , count("customer_id") as cnt_of_purchase
                , round(sum("amount") / count("customer_id") / 10000,2) as avg_purchase_amount
            FROM "AwsDataCatalog"."de-enhancement-db"."purchase_all_info"
            Group by 1, 2, 3, 4
            Order by 1, 2, 3, 4
            ```
            

### 5.6.2 Glue Job(4개)


💡 Glue Job 생성하고 Detail 속성들은 위에 작업했었던 Glue와 동일하거나 Worker 수만 조절하여 진행 
따로 작성하지 않겠습니다. 

`코드 변경`
→ Output Path



1. 계열사별 + 년 + 월 + 일 + 시간 + 요일 + 매출 건수 + 매출 금액
    - 이름 : {blee}_jb_de_enhancement_t2_salesbydatetime_s2s
    - `변경이 필요한 부분`
        - Glue Database 명
        - Output Path
    - `Script`
        
        ```python
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
        ```
        
    
    ![Untitled]( ../img/Untitled%2056.png)
    
    ![Untitled]( ../img/Untitled%2057.png)
    
2. 계열사별 + 연령별 + 제품(카테고리)별 매출
    - 이름 : {blee}_jb_de_enhancement_t2_salesbyageproducts_s2s
    - `변경이 필요한 부분`
        - Glue Database 명
        - Output Path
    - `Script`
        
        ```python
        # # 2. 계열사별 + 연령별 + 제품(카테고리)별 매출
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
        
        ## 계열사별 + 연령별 + 제품(카테고리)별 + 매출 건수, 매출 금액 
        grouped_purchase_df = purchase_all_info_df.groupBy(['affiliate', 'purchase_year', 'purchase_month', 'age_group', 'division_cd', 'main_category_desc', 'sub_category_desc'])\
                                                    .agg(sum("amount").alias("total_purchase_amount"), count("amount").alias("count_of_purchase"))
        
        grouped_purchase_df.show(3)
        
        output_path = output_path = f's3://blee-lab/glue/data/fact/gold/mart_salesbyageproducts/'
        grouped_purchase_df.coalesce(1).write.mode('overwrite').partitionBy(['affiliate','purchase_year','purchase_month']).parquet(output_path)
        
        job.commit()
        ```
        
    
    ![Untitled]( ../img/Untitled%2058.png)
    
    ![Untitled]( ../img/Untitled%2059.png)
    
3. 계열사별 + 지역별 + 매출
    - 이름 : {blee}_jb_de_enhancement_t2_salesbyresidence_s2s
    - `변경이 필요한 부분`
        - Glue Database 명
        - Output Path
    - `Script`
        
        ```python
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
        ```
        
    
    ![Untitled]( ../img/Untitled%2060.png)
    
    ![Untitled]( ../img/Untitled%2061.png)
    
4. 고객별 + 각 계열사 + 년 + 월 + 구매 건수 + 구매 금액
    - 이름 :  {blee}_jb_de_enhancement_t2_salesbycustomer_s2s
    - `변경이 필요한 부분`
        - Glue Database 명
        - Output Path
    - `Script`
        
        ```python
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
        ```
        
    
    ![Untitled]( ../img/Untitled%2062.png)
    
    ![Untitled]( ../img/Untitled%2063.png)
    

### 5.6.3 Crawler(4개)

위 각 주제에 맞게 S3 Path만 지정하여 4개 크롤러 생성 

1. 계열사별 + 년 + 월 + 일 + 시간 + 요일 + 매출 건수 + 매출 금액
    - 이름 : {blee}_cr_de_enhancement_t2_salesbydatetime
2. 계열사별 + 연령별 + 제품(카테고리)별 매출
    1. 이름 : {blee}_cr_de_enhancement_t2_salesbyageproducts
3. 계열사별 + 지역별 + 매출
    1. 이름 : {blee}_cr_de_enhancement_t2_salesbyresidence
4. 고객별 + 각 계열사 + 년 + 월 + 구매 건수 + 구매 금액
    1. 이름 : {blee}_cr_de_enhancement_t2_salesbycustomer

![Untitled]( ../img/Untitled%2064.png)

![Untitled]( ../img/Untitled%2065.png)

![Untitled]( ../img/Untitled%2066.png)