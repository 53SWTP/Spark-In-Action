# 3장 스파크 애플리케이션 작성하기
이클립스를 사용해 스파크 프로그램을 작성하는 방법

## 3.1 이클립스로 스파크 프로젝트 생성
가상머신에 설치된 이클립스를 GUI를 사용하여 쓸 수 있도록 X 윈도 시스템 설정
1. Xming 다운로드 - https://goo.gl/QjwqFt
2. XLaunch 실행 - Multiple Windows > Start no client > No Access Control 체크 > 마침
3. putty 실행 - Configuration 목록 중 Connection > SSH > X11에서 Enable X11 forwarding 체크, X display location에 <IP 주소>:0 입력
4. eclipse 실행 - 가상머신 Shell에서 export DISPLAY=<IP 주소>:0 설정 후, /home/spark/eclipse/java-mars/eclipse/eclipse 실행

이클립스 세팅
1. Help > Install new Software > Add
2. Name : scala-ide
3. Location : http://download.scala-ide.org/sdk/lithium/e44/scala211/stable/site
4. OK
5. 목록 중 Scala IDE for Eclipse 선택
6. Restart Now

1. Help > Install new Software > Add
2. Name : m2eclipse-scala
3. Location : http://alchim31.free.fr/m2e-scala/update-site
4. OK
5. Maven Integration for Eclipse 선택
6. Restart Now

Maven 프로젝트 생성
Catalog File : https://github.com/spark-in-action/scala-archetype-sparkinaction/raw/master/archtype-catalog.xml
Description : Spark in Action

## 3.2 스파크 애플리케이션 개발
게임 회사의 모든 직원 명단과 각 직원이 수행한 푸시 횟수를 담은 일일 리포트
### 3.2.1 깃허브 아카이브 데이터셋 준비
```bash
$ mkdir -p $HOME/sia/github-archive
$ cd $HOME/sia/github-archive
$ wget http://data.githubarchive.org/2015-03-01-{0..23}.json.gz
```

```bash
$ gunzip *
```

### 3.2.2 JSON 로드
스파크 SQL과 DataFrame이 JSON 데이터를 스파크로 입수하는 기능을 제공함
DataFrame API - 스키마가 있는 RDD, 관계형 데이터베이스의 테이블과 유사
스파크 SQL의 메인 인터페이스는 SQLContext 클래스다. 

***`json` 메서드 시그니처***
```scala
def json(paths: String*): DataFrame
```

***App.scala***
```scala
import org.apache.spark.sql.SparkSession

object App {
	def main(args : Array[String]) {
		val spark = SparkSession.builder()
			.appName("GitHub push counter")
			.master("local[*]")
			.getOrCreate()

		val sc = spark.sparkContext

		// TODO expose inputPath as app. param
		val homeDir = System.getenv("HOME")
		val inputPath = homeDir + "/sia/github-archive/2015-03-01-0.json"
		val ghLog = spark.read.json(inputPath)

		val pushes = ghLog.filter("type = 'PushEvent'")
	}
}
```

### 3.2.3 이클립스에서 애플리케이션 실행

***App.scala 추가***
```scala
	pushes.printSchema
	println("all events: " + ghLog.count)
	println("only pushes: " + pushes.count)
	pushes.show(5)
```

### 3.2.4 데이터 집계
groupBy와 함께 사용된 count 연산은 다른 칼럽 값을 무시하고 actor.login 칼럽의 고윳값별로 그루핑한 로우 개수만 집계
DataFrame API는 count 연산 외에도 min, max, avg, sum 등 집계 함수를 제공

***App.scala 추가***
```scala
	val grouped = pushes.groupBy("actor.login").count
	grouped.show(5)
	val ordered = grouped.orderBy(grouped("count").desc)
	ordered.show(5)
```

### 3.2.5 분석 대상 제외
Set의 랜덤 룩업 성능이 Seq보다 빠르므로 Set을 사용

***App.scala 추가***
```scala
import scala.io.Source.fromFile

	val empPath = homeDir + "/first-edition/ch03/ghEmployees.txt"
	val employees = Set() ++ (
	  for {
		line <- fromFile(empPath).getLines
	  } yield line.trim
	)
```
스칼라의 for 컴프리헨션 - 반복 연산을 손쉽게 다룰 수 있는 간결하고 강력한 기능 제공

***filter 메서드 시그니처***
```scala
def filter(conditionExpr: String): Dataset
```

DataFrame SQL 표현식에서는 filter 함수를 사용할 수 없어 UDF(스파크의 사용자 정의 함수)를 활용

***App.scala 추가***
```scala
	val isEmp = user => employees.contains(user)
	val isEmployee = spark.udf.register("isEmpUdf", isEmp)
```

### 3.2.6 공유 변수
가십 프로토콜을 사용하여 공유 변수를 유기적으로 확산함(Like P2P)

***공유 변수 등록***
```scala
	val bcEmployees = sc.broadcast(employees)
	
	import spark.implicits._
    val isEmp = user => bcEmployees.value.contains(user) #수정
    val isEmployee = spark.udf.register("SetContainsUdf", isEmp) #기존 소스 isEmpUdf,SetContainsUdf 차이는?
    val filtered = ordered.filter(isEmployee($"login"))
    filtered.show()
```

### 3.2.7 전체 데이터셋 사용

## 3.3 애플리케이션 제출
스파크 클러스터의 모든 노드에 라이브러리가 설치되어 있어야 한다.

### 3.3.1 uberjar 빌드
spark-submit 스크립트의 --jars 매개변수에 프로그램에 필요한 JAR 파일을 모두 나열해 실행자로 전송한다.
모든 의존 라이브러리를 포함하는 uberjar를 빌드한다.

***pom.xml에 추가할 내용***
```xml
    <dependency>
      <groupId>org.apache.commons</groupId>
      <artifactId>commons-email</artifactId>
      <version>1.3.1</version>
      <scope>compile</scope>
    </dependency>
```

maven-shade-plugin 설정으로 uberjar 사용 가능
scope를 compile로 설정하면 uberjar에 포함된다.

### 3.3.2 애플리케이션의 적응력 올리기
spark-submit으로 인수 받는 방식으로 변경

***GitHubDay.scala***
```scala
package org.sia.chapter03App

import scala.io.Source.fromFile
import org.apache.spark.sql.SparkSession

object GitHubDay {
  def main(args : Array[String]) {
    val spark = SparkSession.builder().getOrCreate()

    val sc = spark.sparkContext

    val ghLog = spark.read.json(args(0))

    val pushes = ghLog.filter("type = 'PushEvent'")
    val grouped = pushes.groupBy("actor.login").count
    val ordered = grouped.orderBy(grouped("count").desc)

    // Broadcast the employees set
    val employees = Set() ++ (
      for {
        line <- fromFile(args(1)).getLines
      } yield line.trim
    )
    val bcEmployees = sc.broadcast(employees)

    import spark.implicits._
    val isEmp = user => bcEmployees.value.contains(user)
    val sqlFunc = spark.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(sqlFunc($"login"))

    filtered.write.format(args(3)).save(args(2))
  }
}
```
mvn clean package -Dtest.skip=ture로 빌드 수행

### 3.3.3 spark-submit 사용

```bash
$ tail -f /usr/local/spark/logs/info.log
$ spark-submit --class org.sia.chapter03App.GitHubDay --master local[*] --name "Daily GitHub Counter" chapter03App-0.0.1-SNAPSHOT.jar "$HOME/sia/github-archive/*.json" "$HOME/first-edition/ch03/ghEmployees.txt" "$HOME/sia/emp-gh-push-output" "json"
```
