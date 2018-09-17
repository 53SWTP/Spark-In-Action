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


### 5.1.1 RDD에서 DataFrame 생성
- Row의 데이터를 튜플 형태로 저장한 RDD 를 사용하는 방법 : 스키마 속성을 지정할 수 없음
- 케이스 클래스를 사용하는 방법 : 복잡함
- 스키마를 명시적으로 지정하는 방법 : De facto standard


```

object DataFrameTest {
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import java.sql.Timestamp

  case class Post (commentCount:Option[Int], lastActivityDate:Option[java.sql.Timestamp],
                   ownerUserId:Option[Long], body:String, score:Option[Int], creationDate:Option[java.sql.Timestamp],
                   viewCount:Option[Int], title:String, tags:String, answerCount:Option[Int],
                   acceptedAnswerId:Option[Long], postTypeId:Option[Long], id:Long)

  import org.apache.spark.rdd.RDD

  object StringImplicits {
    implicit class StringImprovements(val s: String) {
      import scala.util.control.Exception.catching
      def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
      def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
      def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
    }
  }

  def method1(spark: SparkSession, itPostsRows: RDD[String]): DataFrame = {

    import spark.implicits._ // let RDD call '.toDF'

    val itPostsSplit = itPostsRows.map(x => x.split("~"))

    val itPostsRDD = itPostsSplit.map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))
    val itPostsDFrame = itPostsRDD.toDF()
    itPostsDFrame.show(10)

    return itPostsRDD.toDF("commentCount", "lastActivityDate", "ownerUserId", "body",
      "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")

  }

  def method2(spark: SparkSession, itPostsRows : RDD[String]) : DataFrame = {
    import StringImplicits._
    def stringToPost(row:String):Post = {
      val r = row.split("~")
      return Post(r(0).toIntSafe,
        r(1).toTimestampSafe,
        r(2).toLongSafe,
        r(3),
        r(4).toIntSafe,
        r(5).toTimestampSafe,
        r(6).toIntSafe,
        r(7),
        r(8),
        r(9).toIntSafe,
        r(10).toLongSafe,
        r(11).toLongSafe,
        r(12).toLong)
    }

    import spark.implicits._ // let RDD call '.toDF'
    return itPostsRows.map(x => stringToPost(x)).toDF()
  }

  def method3(spark: SparkSession, itPostsRows : RDD[String]): DataFrame = {
    import StringImplicits._
    import org.apache.spark.sql.types._
    val postSchema = StructType(Seq(
      StructField("commentCount", IntegerType, true),
      StructField("lastActivityDate", TimestampType, true),
      StructField("ownerUserId", LongType, true),
      StructField("body", StringType, true),
      StructField("score", IntegerType, true),
      StructField("creationDate", TimestampType, true),
      StructField("viewCount", IntegerType, true),
      StructField("title", StringType, true),
      StructField("tags", StringType, true),
      StructField("answerCount", IntegerType, true),
      StructField("acceptedAnswerId", LongType, true),
      StructField("postTypeId", LongType, true),
      StructField("id", LongType, false))
    )
    import org.apache.spark.sql.Row
    def stringToRow(row:String):Row = {
      val r = row.split("~")
      Row(r(0).toIntSafe.getOrElse(null),
        r(1).toTimestampSafe.getOrElse(null),
        r(2).toLongSafe.getOrElse(null),
        r(3),
        r(4).toIntSafe.getOrElse(null),
        r(5).toTimestampSafe.getOrElse(null),
        r(6).toIntSafe.getOrElse(null),
        r(7),
        r(8),
        r(9).toIntSafe.getOrElse(null),
        r(10).toLongSafe.getOrElse(null),
        r(11).toLongSafe.getOrElse(null),
        r(12).toLong)
    }
    val rowRDD = itPostsRows.map(row => stringToRow(row))

    return spark.createDataFrame(rowRDD, postSchema)
  }

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .master("local[*]")  //스파크 클러스터 사용여부 + executor 갯수
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sparkContext

    val itPostsRows = sc.textFile("./italianPosts.csv")

    val itPostsDF = method1(spark, itPostsRows)
    println(itPostsDF.columns.toList)
    println(itPostsDF.dtypes.toList)

    val itPostsDFCase = method2(spark, itPostsRows)
    println(itPostsDFCase.columns.toList)
    println(itPostsDFCase.dtypes.toList)

    val itPostsDFStruct = method3(spark, itPostsRows)
    println(itPostsDFStruct.columns.toList)
    println(itPostsDFStruct.dtypes.toList)

  }
}

```

### 5.1.2 기본 DataFrame API

컬럼 선택
```
val postsDf = itPostsDFStruct
val postsIdBody = postsDf.select("id", "body")
val postsIdBody2 = postsDf.select(postsDf.col("id"), postsDf.col("body"))
```

Symbol 객체 사용
```
import spark.implicits._
val postsIdBody3 = postsDf.select(Symbol("id"), Symbol("body"))
val postsIdBody4 = postsDf.select('id, 'body)
```
$ 메서드 사용 : 문자열->ColumnName 변환
```
val postsIdBody5 = postsDf.select($"id", $"body")
```

컬럼 날리기
```
val postIds = postsIdBody.drop("body")
```


데이터 필터링
```
postsIdBody.filter('body contains "Italiano").count()

val noAnswer = postsDf.filter(('postTypeId === 1) and ('acceptedAnswerId isNull))

val firstTenQs = postsDf.filter('postTypeId === 1).limit(10)
```

컬럼이름 변경
```
val firstTenQsRn = firstTenQs.withColumnRenamed("ownerUserId", "owner")

```

새로운 컬럼 추가
```
postsDf.filter('postTypeId === 1).withColumn("ratio", 'viewCount / 'score).where('ratio < 35).show()
```

[정답] 가장 최근에 수정한 10개 질문 출력
```
//The 10 most recently modified questions:
postsDf.filter('postTypeId === 1).orderBy('lastActivityDate desc).limit(10).show
```

### 5.1.3 SQL 함수로 데이터에 연산 수행

SQL 함수 import!
```
import org.apache.spark.sql.functions._
```

datediff
```
val bodyText = postsDf.filter('postTypeId === 1).withColumn("activePeriod", datediff('lastActivityDate, 'creationDate)).orderBy('activePeriod desc).head.getString(3).replace("&lt;","<").replace("&gt;",">")
println(bodyText)
```

aggregations
```
postsDf.select(avg('score), max('score), count('score)).show
```

Window functions (frame)
```
import org.apache.spark.sql.expressions.Window

postsDf.filter('postTypeId === 1).select('ownerUserId, 'acceptedAnswerId, 'score, max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser").withColumn("toMax", 'maxPerUser - 'score).show(10)

postsDf.filter('postTypeId === 1).select('ownerUserId, 'id, 'creationDate, lag('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "prev", lead('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "next").orderBy('ownerUserId, 'id).show()
```

사용자 정의 함수
```
val countTags = udf((tags: String) => "&lt;".r.findAllMatchIn(tags).length)
val countTags_2 = spark.udf.register("countTags", (tags: String) => "&lt;".r.findAllMatchIn(tags).length)
postsDf.filter('postTypeId === 1).select('tags, countTags('tags) as "tagCnt").show(10, false)

```

### 5.1.4 결측값 다루기 (데이터 정제하기)
결측값 : null, NaN 등 비어 있거나 의미 없는 값 -> 허수??? 통계/분석/학습에 방해된다..
해결방법 :  제외(drop) / 채워넣기(fill) / 다른 값으로 치환(replace)
DataFrame.na 필드 : DataFrameNaFunctions

Drop N/A
```
val cleanPosts = postsDf.na.drop()
cleanPosts.count()
```

Drop acceptedAnswerId's N/A
```
postsDf.na.drop(Array("acceptedAnswerId"))
```

Fill 0s in viewCount column
```
postsDf.na.fill(Map("viewCount" -> 0))
```

Replace
```
val postsDfCorrected = postDf.na.replace(Array("id", "acceptedAnswerId"), Map(1177 -> 3000))
```

### 5.1.5 DataFrame을 RDD로 변환

```
val postsRdd = postDf.rdd //also lazily evaluated.
```

```
import org.apache.spark.sql.Row
val postsMapped = postsDf.rdd.map(row => Row.fromSeq(
	row.toSeq.updated(3, row.getString(3).replace("&lt;","<").replace("&gt;",">"))
	         .updated(8, row.getString(8).replace("&lt;","<").replace("&gt;",">"))))
val postsDfNew = spark.createDataFrame(postsMapped, postsDf.schema)
```
하지만 DataFrame API의 함수로 거의 모든 매핑 작업을 해결할 수 있다!


### 5.1.6 Data Grouping

```
postsDfNew.groupBy('ownerUserId, 'tags, 'postTypeId).count.orderBy('ownerUserId desc).show(10)

postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score)).show(10)
postsDfNew.groupBy('ownerUserId).agg(Map("lastActivityDate" -> "max", "score" -> "max")).show(10)

postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score).gt(5)).show(10)

val smplDf = postsDfNew.where('ownerUserId >= 13 and 'ownerUserId <= 15)
smplDf.groupBy('ownerUserId, 'tags, 'postTypeId).count.show()

smplDf.rollup('ownerUserId, 'tags, 'postTypeId).count.show()

smplDf.cube('ownerUserId, 'tags, 'postTypeId).count.show()

```

(당구장표시) 참고 스파크 SQL 파라미터 설정 방법
```
spark.sql("SET spark.sql.caseSensitive=true")
spark.conf.set("spark.sql.caseSensitive", "true")

```
### 5.1.7 Data Join

```

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


val itVotesRaw = sc.textFile("./italianVotes.csv").map(x => x.split("~"))
val itVotesRows = itVotesRaw.map(row => Row(row(0).toLong, row(1).toLong, row(2).toInt, Timestamp.valueOf(row(3))))
val votesSchema = StructType(Seq(
  StructField("id", LongType, false),
  StructField("postId", LongType, false),
  StructField("voteTypeId", IntegerType, false),
  StructField("creationDate", TimestampType, false))
  )
val votesDf = spark.createDataFrame(itVotesRows, votesSchema)


val postsVotes = postsDf.join(votesDf, postsDf("id") === votesDf("postId"))
postsVotes.show(5)

val postsVotesOuter = postsDf.join(votesDf, postsDf("id") === votesDf("postId"), "outer")
postsVotesOuter.show(11)
```



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

### 5.3.1 테이블 카탈로그와 하이브 메타스토어

테이블을 임시로 등록하기
```
postsDf.createOrReplaceTempView("posts_temp")
```
테이블을 영구적으로 등록하기
```
postDf.write.saveAsTable("posts")
votesDf.write.saveAsTable("votes")
```

덮어쓰기
```
postDf.write.mode("overwrite").saveAsTable("posts")
votesDf.write.mode("overwrite").saveAsTable("votes")
```

스파크 테이블 카탈로그
```
spark.catalog.listTables().show()
spark.catalog.listColumns("votes").show()
spark.catalog.listFunctions.show()
```

### 5.3.2 (드디어) SQL 쿼리 실행

```
val resultDf = sql("select * from posts")
```

spark-sql 사용
```
spark-sql> select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3;
```
셸에서
```
$ spark-sql -e "select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3"
```

### 5.3.3 Thrift 서버로 스파크 SQL 접속
Spark Thrift : JDBC(ODBC) 서버로 원격지에서 SQL명령을 실행할 수 있음.

## 5-4 DataFrame을 저장하고 불러오기
### 5.4.1 데이터 파일 포맷들
-JSON : 쉽지만... 저장 효율이 좀 떨어짐
-ORC : 하이브 데이터 저장 포맷
-Parquet : 특정 프레임워크에 종속되지 않음!


### 5.4.2 데이터 저장
JSON 형태로 저장
```
postsDf.write.format("json").saveAsTable("postsjson")

sql("select * from postsjson")
```

JDBC 메서드로 RDB 에 저장
```
val props = new java.util.Properties()
props.setProperty("user", "user")
props.setProperty("password", "password")
postsDf.write.jdbc("jdbc:postgresql://postgresrv/mydb", "posts", props)
```

### 5.4.3 데이터 불러오기

파일에서 불러오기
```
val postsDf = spark.read.table("posts")
val postsDf = spark.table("posts")
```

RDB에서 불러오기
```
val result = spark.read.jdbc("jdbc:postgresql://postgresrv/mydb", "posts", Array("viewCount > 3"), props)
```


SQL메서드로 등록한 데이터 소스에서 데이터 불러오기
```
sql("CREATE TEMPORARY TABLE postsjdbc "+
  "USING org.apache.spark.sql.jdbc "+
  "OPTIONS ("+
    "url 'jdbc:postgresql://postgresrv/mydb',"+
    "dbtable 'posts',"+
    "user 'user',"+
    "password 'password')")

sql("CREATE TEMPORARY TABLE postsParquet "+
  "USING org.apache.spark.sql.parquet "+
  "OPTIONS (path '/path/to/parquet_file')")
val resParq = sql("select * from postsParquet")

```


## 5-5 카탈리스트 최적화 엔진 Catalyst Optimizer

![convert_sql_to_rdd](https://thebook.io/img/006908/spark220.jpg)

DSL과 SQL표현식을 RDD연산으로 변환.


## 5-6 텅스텐 프로젝트의 스파크 성능 향상
 CPU , Memory 성능이 향상되었다.
 가비지 컬렉션 문제 (상당부분) 해결
 -> JVM Heap이 아닌 메모리의 Native 영역을 사용(sun.misc.Unsafe) 함으로서 Java object 오버헤드 및 GC 문제를 해결
참고 :  https://younggyuchun.wordpress.com/2017/01/31/spark-%EC%84%B1%EB%8A%A5%EC%9D%98-%ED%95%B5%EC%8B%AC-project-tungsten-%ED%86%BA%EC%95%84%EB%B3%B4%EA%B8%B0/
우리가 설치하고 사용중인 스파크는 이미 향상된 스파크이므로 조금만 감동받고 넘어갑니다
