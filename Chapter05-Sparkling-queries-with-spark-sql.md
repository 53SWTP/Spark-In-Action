# 5장  SQL로 멋진 쿼리를 실행하자

## 5-1 DataFrame 다루기
Pandas(Python), R, Julia의 DataFrame의 영향을 받음. + 분산처리 설계와 카탈리스트 엔진 
SQL 및 DSL로 작성된 표현식을 _최적화된_ RDD operations  로 변환
컬럼명으로  데이터를 참조하고, SQL 쿼리를 사용할 수 있는 등 _데이터 분석가가_ 정형 데이터를 효과적으로 다루게 해 줌
다양한 소스의 데이터를 손쉽게 통합할 수 있음

DataFrame을 생성하는 방법
- 기존 RDD 변환
- SQL 쿼리 실행 <-제일쉬움
- 외부 데이터에서 로딩


5.1.1 RDD에서 DataFrame 생성
- Row의 데이터를 튜플 형태로 저장한 RDD 를 사용하는 방법 : 스키마 속성을 지정할 수 없음
- 케이스 클래스를 사용하는 방법 : 복잡함
- 스키마를 명시적으로 지정하는 방법 : De facto standard


## 5-2 DataFrame을 넘어 Dataset으로

RDD의 확장판(?)
- 일반 자바 객체를 Dataset에 저장할 수 있음
- Spark SQL의 Tungsten 과, catalyst optimizer 활용 가능

Spark 2.0에서는 DataFrame을 Dataset[Row]로 구현함

DataFrame에서 Dataset 으로 변환하기
```
val stringDataSet = spark.read.text("path/to/file").as[String]
```

## 5-3 SQL 명령

5.3.1 테이블 카탈로그와 하이브 메타스토어

테이블을 임시로 등록하기
```
postsDf.createOrReplaceTempView("posts_temp")
```
테이블을 영구적으로 등록하기
```
postDf.write.saveAsTable("posts")
votesDf.write.saveAsTable("votes")

//덮어쓰기
postDf.write.mode("overwrite").saveAsTable("posts")
votesDf.write.mode("overwrite").saveAsTable("votes")
```


## 5-4 DataFrame을 저장하고 불러오기
## 5-5 카탈리스트 최적화 엔진
## 5-6 텅스텐 프로젝트의 스파크 성능 향상
