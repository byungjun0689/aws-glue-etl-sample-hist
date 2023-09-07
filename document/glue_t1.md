# 5. Glue

💡 [AWS Glue란?](https://www.notion.so/AWS-Glue-21a4e620cac84c54a1960d5f7d801697?pvs=21)

💡 `공통`
IAM Role : `DE-CF-GlueExecute-Role`
- SecretsManager Read/Write
- S3 Full Access
- Glue Service Role

AWS Glue 에서 Job, Crawler를 실행 시킬 때 필요한 Role 을 사전 생성. 

## 5.5 [Glue Job] T1, Silver Data

T0에서는 Bronze(Raw Data) 형태로 추후 언제든지 사용될 수 있는 원천 데이터 형태로 데이터를 적재 했다면 Silver Data의 경우 BI 툴 또는 데이터 분석가들이 바로 사용할 수 있도록 비정형화된 데이터 모습으로 생성하려고한다.

즉, 주가 되는 거래 데이터에 나머지 Dimension Data를 Join 해놓은 형태로 적재

사용자가 데이터를 사용할때 마다 Join을 수행해서 사용해도되지만 해당 빈도마다 오버헤드가 발생하며, 컴퓨팅 리소스가 사용된다. 

### 5.5.1 Ad-hoc 분석

> Ad-hoc 분석이란?
라틴어로 ‘특별한 목적을 위해서’라는 뜻으로, 즉각적인 질문(목적)에 데이터로 답을 할 수 있는 일을 의미한다. 
정형화된 결과인 대시보드를 통하지 않고 분석가들이 EDA(탐색적분석), 시각화 등을 통해 인사이트를 도출하고자하는 분석 방법을 의미함.
> 

![Untitled]( ../img/Untitled%2047.png)

- `Athena`
    - SQL
        
        ```sql
        WITH customer_info AS (
            SELECT *
              FROM "customer" c
              LEFT JOIN "zipcode" z
                on c."residence" = z."short_zipcode"
             WHERE 1=1
        )
        select * from (select * from "purchase"
        where "affiliate" = 'A'
        and purchase_year = '2014'
        and purchase_month = '01'
        order by "purchase_date", "purchase_time") p201401
        left join customer_info c
        on p201401."customer_id" = c."customer_id"
        left join "products_info" pi
        on (p201401."affiliate" = pi."affiliate"
            and p201401."division_cd" = pi."division_cd" 
            and p201401."main_category_cd" = pi."main_category_cd" 
            and p201401."sub_category_cd" = pi."sub_category_cd" )
        limit 20
        ```
        
    
    ![Untitled]( ../img/Untitled%2048.png)
    ![Untitled]( ../img/Untitled%2049.png)
    
    위와 같은 쿼리를 이용하여 GroupBy 와 같은 집계 연산을 수행하려고한다면 SQL의 복잡도가 더욱 높아 질 것입니다. 그리하여 우리는 해당 결과물을 Glue Data Catalog로 테이블화 시킬 것입니다.
    

### 5.5.2 Glue Job
💡 Glue Job, Crawler 생성

Fact 테이블(purchase)를 중심으로 customer + zipcode, products_info를 left join을 수행하는 ETL 작업 수행
S3 (parquet file) → S3(parquet file)

`코드 변경`
→ Output Path

1. JobName : {blee,mail_id}_jb_de_enhancement_t1_fulljoin_s2s
2. Glue Version : 3.0
3. Worker Type : G 1X
4. NumberOfWorkers : 4
5. Script Path : 위에서 지정한 Glue ScriptPath
6. Maximum concurrency : 1
7. Temporary path : 위에서 지정한 Glue Temp Path 
8. ~~Connections : **5.1** 에서 생성한 Connection 선택~~
    1. Glue Catalog를 활용할때는 Connection이 필요 없음
9. Script 아래 내용 붙여 넣기
    - Glue 작업 진행 중 DataFrame끼리 Join하다 보니 최종 Partition의 수가 40개로 늘어남에 따라 개별 파일의 용량이 줄고 수가 늘어나게되면 추후 분석에 IO 가 늘어남에 따라 Repartition을 수행하여 파티션수를 줄이는 작업도 코드에 포함.
    - `변경이 필요한 부분`
        - Glue Database 명
        - Output Path
    - [`T1 Glue Job Script`](../scripts/jb_de_enhancement_t1_fulljoin_s2s.py)
        
10. 결과물

    ![Untitled]( ../img/Untitled%2050.png)    
    ![Untitled]( ../img/Untitled%2051.png)
        

### 5.5.3 Crawler

- 이름 : {blee}_cr_de_enhancement_t1_purchase_all
- S3 Path : 본인이 생성한 S3 Bucket 내 Fact Data → Silver 폴더 내 테이블명으로 지정
    - s3://blee-lab/glue/data/fact/silver/purchase_all_info/
- IAM Role : 위에서 생성해놓은 IAM Role
- 순서 : 5.3.1 과 동일하며, S3 Path 지정만 다르다.
- 생성 이후 Run 수행

    ![Untitled]( ../img/Untitled%2052.png)    
    ![Untitled]( ../img/Untitled%2053.png)
    ![Untitled]( ../img/Untitled%2054.png)
    ![Untitled]( ../img/Untitled%2055.png)
        
        