# 6. Stepfunction

💡 아래 상태 머신 생성하는 방법을 그대로 수행하시는게 익힐 수 있는 가장 빠른 방법으로 생각됩니다. 
하지만 시간이 부족하거나 복잡하다고 생각하시는 분들께서는 금일 교육에서는 아래 전체 Json 파일을 복사 하셔 코드로 적용하기로 적용 후 본인이 생성한 리소스 명으로 변경하는 작업만 수행하셔도 무방할 것 으로 판단됩니다. 
전체 상태 머신 Json 파일 : [`JSONFile`](../scripts/stepfunction.json)


## 6.1 상태 머신 생성

- [Stepfunction Console](https://ap-northeast-2.console.aws.amazon.com/states/home?region=ap-northeast-2#/statemachines)

![Untitled]( ../img/Untitled%2067.png)

![Untitled]( ../img/Untitled%2068.png)

## 6.2 Step 생성


💡 T0 → T1 → T2 순서대로 작업을 수행
실제 현업에서 사용하려면 파라미터를 통해 날짜(년월일)을 파라미터로 하여 조회하는 날짜를 변경하여 각 Step이 진행되도록 해야되나 아래 작업은 Glue Job Script에 지정된 날짜로 조회되도록 진행

### 6.2.1 T0 Bronze Flow

1. Parallel 작업 추가
2. `StartJobRun` 2개 작업 병렬로 적용
    1. 작업 1 : T0 Dimension Job
        1. *태스트 완료 대기(Glue Job 이 모두 완료 될때까지)*
    2. 작업 2 : T0 Fact Job
        1. *태스트 완료 대기(Glue Job 이 모두 완료 될때까지)*
    
    ![Untitled]( ../img/Untitled%2069.png)
    
3. 각 Job 과 연결되는 Crawler (`StartCrawler`)
    1. cr_de_enhancement_t0_dimension
    2. cr_de_enhancement_t0_fact
    3. 태스트 완료 대기 X
        1. Crawler 에서는 적용이 안됨.
    
    ![Untitled]( ../img/Untitled%2070.png)
    
4. 각 Crawler 의 상태를 확인하여 작업 완료 여부를 확인하고자 `GetCrawler`와 `Choice`를 이용
    1. `GetCrawler`
        1. Crawler 현재 어느 상태인지 확인하는 Step → 결과를 다음 Job 으로 전달하기 위한 단계이기도함.
        2. Start Crawler 와 같은 Name을 API 파라미터로 입력
        - `결과물`
            
            아래 내용이 GetCrawler를 통해 전달되는 결과물인데 "State": "STOPPING" 값을 이용
            
            ```json
            {
            	"Crawler": {
            	      "Classifiers": [],
            	      "CrawlElapsedTime": 49000,
            	      "CreationTime": "2022-08-19T06:46:42Z",
            	      "DatabaseName": "hist-retail",
            	      "LakeFormationConfiguration": {
            	        "AccountId": "",
            	        "UseLakeFormationCredentials": false
            	      },
            	      "LastCrawl": {
            	        "LogGroup": "/aws-glue/crawlers",
            	        "LogStream": "cr_retail_factdata_sales",
            	        "MessagePrefix": "62ef7b9f-e54d-4b54-805c-d29e905ac4e4",
            	        "StartTime": "2022-08-19T08:01:25Z",
            	        "Status": "SUCCEEDED"
            	      },
            	      "LastUpdated": "2022-08-19T07:12:38Z",
            	      "LineageConfiguration": {
            	        "CrawlerLineageSettings": "DISABLE"
            	      },
            	      "Name": "cr_retail_factdata_sales",
            	      "RecrawlPolicy": {
            	        "RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"
            	      },
            	      "Role": "AWSGlueServiceRole-PipelineRole",
            	      "SchemaChangePolicy": {
            	        "DeleteBehavior": "LOG",
            	        "UpdateBehavior": "LOG"
            	      },
            	      "State": "STOPPING", // 다음 Step Choice State에서 사용할 Key값.
            	      "Targets": {
            	        "CatalogTargets": [],
            	        "DeltaTargets": [],
            	        "DynamoDBTargets": [],
            	        "JdbcTargets": [],
            	        "MongoDBTargets": [],
            	        "S3Targets": [
            	          {
            	            "Exclusions": [
            	              "**/_temporary/**"
            	            ],
            	            "Path": "s3://hist-retail/factdata/sales"
            	          }
            	        ]
            	      },
            	      "Version": 4
            	    }
            }
            ```
            
        
        ![Untitled]( ../img/Untitled%2071.png)
        
    2. `Choice` - 2개 동일하게 적용
        1. Crawler의 상태에 따라 Wait를 할 것인지 Success를 진행 할 것인지 판단.
            
            ![Untitled]( ../img/Untitled%2072.png)
            
            ![Untitled]( ../img/Untitled%2073.png)
            
        2. Rules 에 아래와 같은 내용을 추가 
            - 전체 식
                - $.Crawler.State == "RUNNING"
            - `Variable`
                - $.Crawler.State
            - `Operator`
                - is equal to
            - `Value`
                - String constant
            - `Text`
                - RUNNING
            
            ![Untitled]( ../img/Untitled%2074.png)
            
        3. RUNNING 연결부분에는 Wait → 다음 상태를 해당 Choice위의 GetCrawler 로 선택
            1. 예) T0 Dimension Choice → Wait → T0 Dimension GetCrawler
            
            ![Untitled]( ../img/Untitled%2075.png)
            
    
5. 최종 결과물 및 실행 결과
    
    ![Untitled]( ../img/Untitled%2076.png)
    
    ![Untitled]( ../img/Untitled%2077.png)
    

### 6.2.2 T1 Silver Flow (Full Join)

Silver Data인 전체 데이터를 Join 한 결과물을 만드는 순서

방벙은 위와 동일하게 StartJob → Crawler → Check Crawler Status → Next

1. StartJobRun
    1. 이름 : jb_de_enhancement_t1_fulljoin_s2s
    2. 태스크 완료 대기 선택
    
    ![Untitled]( ../img/Untitled%2078.png)
    
2. Crawler 실행 및 상태 체크 
    1. StartCrawler
        1. 이름 : cr_de_enhancement_t1_purchase_all
    2. GetCrawler
        1. 이름 : cr_de_enhancement_t1_purchase_all
    3. Choice
        1. Condition : 위에서 적용했던 Choice와 같음
        2. Default → Gold Data Flow로 가도록 처리
    
    ![Untitled]( ../img/Untitled%2079.png)
    

### 6.2.3 T2 Gold Flow(Data Mart)

총 4개의 마트를 생성 → Job 4, Crawler 4개 수행

단, 마지막 단계이므로 Status Check는 하지 않고 마무리함

1. 병렬 처리를 위한 `Parallel` 추가
2. 4개의 Glue Job Start 추가 
    1. 각 Job 이름 입력 
        1. jb_de_enhancement_t2_salesbyageproducts_s2s
        2. jb_de_enhancement_t2_salesbycustomer_s2s
        3. jb_de_enhancement_t2_salesbydatetime_s2s
        4. jb_de_enhancement_t2_salesbyresidence_s2s
    2. 각 태스크 완료 대기 선택
3. 각각 Job 에 맞는 Crawler를 다음 Step으로 입력
    1. 이름
        1. cr_de_enhancement_t2_salesbyageproducts
        2. cr_de_enhancement_t2_salesbycustomer
        3. cr_de_enhancement_t2_salesbydatetime
        4. cr_de_enhancement_t2_salesbyresidence

![Untitled]( ../img/Untitled%2080.png)

### 6.2.4 최종 전체 Step

1. 소요 시간
    - 초기 적재 : 20분
    - 증분 적재 : 10분
2. 전체 Json File
    - [`JSONFile`](../scripts/stepfunction.json)
        

![Untitled]( ../img/Untitled%2081.png)

![Untitled]( ../img/Untitled%2082.png)

![Untitled]( ../img/Untitled%2083.png)

![Untitled]( ../img/Untitled%2084.png)