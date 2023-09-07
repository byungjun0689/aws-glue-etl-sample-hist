
# 7. Amazon EventBridge

데이터를 적재해야하는 매 순간마다 해당 `AWS Stepfunction`의 상태 머신을 구동시키는것은 반복되는 엔지니어의 업무 과중 및 낭비가 되므로 해당 작업이 특정 조건(날짜)에 맞춰서 구동되도록 스케줄을 적용할 수 있다.

EventBridge의 TimeBase Rule을 통해 작업 등록 수행

![Untitled]( ../img/Untitled%2085.png)

![Untitled]( ../img/Untitled%2086.png)

- 규칙 : 0 16 15 * ? *  ⇒ Cron Tab 스케줄링 방법과 동일
    - 매월 16일 오전 1시에 수행(한국시간 기준)

![Untitled]( ../img/Untitled%2087.png)

- 대상 선택
    - Step Functions 상태 머신
    - 위에서 생성한 머신 선택
    - 실행 역할
        - 그자리에서 생성해도되고 따로 Role을 만들어도됨.

![Untitled]( ../img/Untitled%2088.png)

![Untitled]( ../img/Untitled%2089.png)