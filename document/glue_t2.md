# 5. Glue

💡 [AWS Glue란?](https://www.notion.so/AWS-Glue-21a4e620cac84c54a1960d5f7d801697?pvs=21)

💡 `공통`
IAM Role : `DE-CF-GlueExecute-Role`
- SecretsManager Read/Write
- S3 Full Access
- Glue Service Role

AWS Glue 에서 Job, Crawler를 실행 시킬 때 필요한 Role 을 사전 생성.
        
## 5.6 [Glue Job] T2, Gold Data

> 💡 대부분의 Gold Data의 경우 Summary된 형태의 데이터를 많이 생성한다.   
GroupBy, Sum, Avg 등 BI 또는 리포트에 맞는 형태로 데이터를 변환하여 적재 수행  
해당 실습은 데이터 분석을 목적으로 하는게 아니라 엔지니어링을 목적으로 하기에 간단한 지표만 생성할 예정

### 5.6.1 주제

> 해당 주제는 임의로 정의한 주제로, 추후 개별적으로 실습하실때는 다르게 주제를 잡아서 수행해도 됩니다.

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

> 💡 Glue Job 생성하고 Detail 속성들은 위에 작업했었던 Glue와 동일하거나 Worker 수만 조절하여 진행 따로 작성하지 않겠습니다. 

`코드 변경`
→ Output Path

1. 계열사별 + 년 + 월 + 일 + 시간 + 요일 + 매출 건수 + 매출 금액
    - 이름 : {blee}_jb_de_enhancement_t2_salesbydatetime_s2s
    - `변경이 필요한 부분`
        - Glue Database 명
        - Output Path
    - [`T2 Glue Job Script (sales by datetime)`](../scripts/jb_de_enhancement_t2_salesbydatetime_s2s.py)
    
    ![Untitled]( ../img/Untitled%2056.png)
    ![Untitled]( ../img/Untitled%2057.png)
    
2. 계열사별 + 연령별 + 제품(카테고리)별 매출
    - 이름 : {blee}_jb_de_enhancement_t2_salesbyageproducts_s2s
    - `변경이 필요한 부분`
        - Glue Database 명
        - Output Path
    - [`T2 Glue Job Script(sales by products)`](../scripts/jb_de_enhancement_t2_salesbyageproducts_s2s.py)
        
    ![Untitled]( ../img/Untitled%2058.png)
    ![Untitled]( ../img/Untitled%2059.png)
    
3. 계열사별 + 지역별 + 매출
    - 이름 : {blee}_jb_de_enhancement_t2_salesbyresidence_s2s
    - `변경이 필요한 부분`
        - Glue Database 명
        - Output Path
    - [`T2 Glue Job Script(sales by residence)`](../scripts/jb_de_enhancement_t2_salesbyresidence_s2s.py.py)
    
    ![Untitled]( ../img/Untitled%2060.png)
    ![Untitled]( ../img/Untitled%2061.png)
    
4. 고객별 + 각 계열사 + 년 + 월 + 구매 건수 + 구매 금액
    - 이름 :  {blee}_jb_de_enhancement_t2_salesbycustomer_s2s
    - `변경이 필요한 부분`
        - Glue Database 명
        - Output Path
    - [`T2 Glue Job Script(sales by customer)`](../scripts/jb_de_enhancement_t2_salesbycustomer_s2s.py)
    
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

#### 결과
![Untitled]( ../img/Untitled%2064.png)
![Untitled]( ../img/Untitled%2065.png)
![Untitled]( ../img/Untitled%2066.png)