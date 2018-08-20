# 2장 스파크의 기초
여기서는 스파크 자체 로컬 클러스터만 사용

## 2.1 가상 머신 사용
가상 머신 시작
```bash
$ vagrant up
```

SSH로 가상 머신에 접속
```bash
$ vagrant ssh -- -l spark
```

### 2.1.1 깃허브 저장소 복제
```bash
$ git clone https://github.com/spark-in-action/first-edition
```

### 2.1.2 자바 찾기
```bash
$ which java
/usr/bin/java

$ ls -al /usr/bin/java
lrwxrwxrwx 1 root root 22 Apr 19  2016 /usr/bin/java -> /etc/alternatives/java

$ ls -al /etc/alternatives/java
lrwxrwxrwx 1 root root 46 Apr 19  2016 /etc/alternatives/java -> /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

$ echo $JAVA_HOME
/usr/lib/jvm/java-8-openjdk-amd64/jre
```

실제로 자바가 설치된 위치는 `/usr/lib/jvm/java-8-openjdk-amd64`

### 2.1.3 가상 머신에 설치된 하둡 사용
```bash
$ ls -al /usr/local/hadoop
lrwxrwxrwx 1 root root 17 Apr 19  2016 /usr/local/hadoop -> /opt/hadoop-2.7.2

$ hadoop fs -ls /usr # hadoop 명령어를 사용할 수 있음
```
전체 명령어는 [하둡 공식 문서](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/FileSystemShell.html) 참조

가상 머신이 부팅하면서 HDFS 데몬 프로세스를 자동으로 시작했기 때문에 아무런 준비 없이 하둡 명령어를 사용해볼 수 있는 것이고, 구체적인 HDFS 실행/종료 명령어는 아래와 같다.

```bash
$ /usr/local/hadoop/sbin/start-dfs.sh # HDFS 데몬 프로세스 시작
$ /usr/local/hadoop/sbin/stop-dfs.sh # HDFS 데몬 프로세스 중지 
```

### 2.1.4 가상 머신에 설치된 스파크 살펴보기
```bash
$ ls -al /usr/local/spark
lrwxrwxrwx 1 root root 31 Sep 17  2016 /usr/local/spark -> /opt/spark-2.0.0-bin-hadoop2.7/
```

#### 2.1.4.1 스파크 릴리스 관리
스파크의 릴리스 주기가 매우 빠르기 때문에 여러 버전의 스파크를 관리하고 현재 사용할 버전을 손쉽게 선택할 수 있는 방법이 필요

여기서는 심볼릭 링크를 활용해 `/opt` 디렉토리 아래 여러 스파크 바이너리를 모아두고, 필요할 때마다 `/usr/local/spark` 심볼릭 링크를 변경하는 방안을 사용한다.

```bash
$ sudo rm -f /usr/local/spark # 기존 심볼릭 링크 삭제
$ sudo ln -s /opt/spark-${version}-bin-hadoop${hadoop_vertion} /usr/local/spark # 새로운 스파크 바이너리를 /usr/local/spark 에 심볼릭 링크로 연결
```

#### 2.1.4.2 기타 세부 사항
스파크 스크립트는 대부분 `SPARK_HOME` 환경 변수를 사용한다. 그러니 환경에 `SPARK_HOME`이 잡혀 있어야겠지?

## 2.2 스파크 셸로 첫 스파크 프로그램 작성
스파크를 사용하는 방법은 두 가지
1. 스파크 라이브러리, 즉 스파크 API를 사용해 독립형 프로그램(스칼라, 자바, 파이썬) 작성  
애플리케이션을 작성하고 스파크에 제출해 실행한 후, 애플리케이션이 파일로 출력한 결과를 분석하는 복잡한 과정 필요
2. 스칼라 셸이나 파이썬 셸 사용 (Read-Eval-Print Loop)  
간단한 애플리케이션을 개발하거나 테스트할 때 사용한다. 셸을 종료하면 셸에서 작성한 프로그램이 삭제되므로 주로 일회성 작업에 사용

### 2.2.1 스파크 셀 시작
```bash
$ spark-shell # scala
$ pyspark # python
```

*Spark Context*와 *Spark Session* 객체가 각각 `sc`와 `spark` 변수로 제공됨

### 2.2.2 첫 스파크 코드 예제
LICENSE 파일에서 BSD 문자열이 포함된 라인 수 계산하기

파일 로드
```scala
scala> val licLines = sc.textFile("/usr/local/spark/LICENSE")
scala> val lineCnt = licLines.count
```

익명 함수 사용
```scala
scala> val bsdLines = licLines.filter(line => line.contains("BSD"))
scala> val bsdCnt = bsdLines.count
```

기명 함수 사용
```scala
scala> def isBSD(line: String) = { line.contains("BSD") }
// OR
scala> def isBSD = (line: String) => line.contains("BSD")
scala> val bsdLines = licLines.filter(isBSD)
scala> val bsdCnt = bsdLines.count
```

결과 출력
```scala
scala> bsdLines.foreach(println)
```

### 2.2.3 RDD의 개념
위 예제에서 `licLines`와 `bsdLines`는 평범한 스칼라 컬렉션처럼 보이지만, 사실 *RDD라는 스파크 전용 분산 컬렉션*

RDD의 성질
* 불변성(immutable): 읽기 전용(read-only)
* 복원성(resilient): 장애 내성
* 분산(distributed): 노드 한 개 이상에 저장된 데이터 셋

RDD는 *데이터를 조작할 수 있는 다양한 변환 연상자를 제공*하고 변환 결과는 항상 **새로운 RDD 객체**가 된다. 다시 말해 RDD는 한 번 생성되면 상태가 변경되지 않으며, 스파크는 이런 불변성을 사용해 장애 내성을 보장한다.

또한 스파크는 노드에 장애가 발생해도 유실된 RDD를 원래대로 복구할 수 있다. 다른 분산 컴퓨팅 프레임워크에서는 데이터 자체를 복제하는 방식으로 장애 내성 기능을 지원한다. 하지만 RDD는 데이터셋 자체를 중복 저장하는 대신 *데이터셋을 만드는 데 사용된 변환 연산자의 로그(lineage)를 남기는 방식*으로 장애 내성 기능을 제공한다. 다시 말해, 일부 노드에서 장애가 발생하면 스파크는 *해당 노드가 가진 데이터셋을 다시 계산*해 RDD를 복원한다. 이렇게 RDD에 적용된 변환 연산자와 그 순서를 **RDD 계보(lineage)** 라고 한다.

## 2.3 RDD의 기본 행동 연산자 및 변환 연산자
* 변환(Transformation): RDD를 조작해 새로운 RDD 생성(ex: `filter`, `map`)
* 행동(Action): 계산 결과를 반환하거나 RDD 요소에 특정 작업을 수행하려고 *실제 계산을 시작*하는 역할 (ex: `count`, `foreach`)

스파크에서 변환 연산자는 *지연 실행(lazy evaluation)* 된다. 변환 연산자의 계산은 *행동 연산자가 호출되기 전까지는 실제로 수행되지 않*는다. 행동 연산자가 호출되고 난 뒤에야 Spark가 해당 RDD의 lineage를 살펴보고, 연산 그래프를 작성해서 행동 연산자를 계산한다.

### 2.3.1 `map` 변환 연산자
원본 RDD의 각 요소에 *임의의 함수를 적용* 후 변환된 요소로 새로운 RDD를 생성

***RDD에 선언된 `map` 메서드 시그니처***
```scala
class RDD[T] {
    def map[U](f: (T) => U): RDD[U] // T타입의 요소를 가진 RDD에 T를 U로 변환하는 함수를 요소별로 적용해 새로운 RDD[U]를 만들어냄
}
```
위의 코드에서 `T` 와 `U`는 같은 타입일 수도, 다른 타입일 수도 있다.

`map` 함수를 사용해 RDD 요소의 제곱 값을 계산하는 예제
```scala
scala> val numbers = sc.parallelize(10 to 50 by 10) // 여기서 만들어진 새로운 RDD는 여러 스파크 실행자로 분산됨
// val numbers = sc.makeRDD(10 to 50 by 10) 도 동일함
scala> numbers.foreach(x => println(x))
scala> val numbersSquared = numbers.map(num => num * num)
scala> numbersSquared.foreach(x => println(x))
```

`map` 함수를 사용해 RDD 요소의 타입을 변경하는 예제
```scala
scala> val reversed = numbersSquared.map(x => x.toString.reverse)
scala> reversed.foreach(x => println(x))
001
004
009
0061
0051
```

placeholder를 이용해 `map`을 더 간결하게 작성해보자
```scala
scala> val alsoReversed = numbersSquared.map(_.toString.reverse)
scala> alsoReversed.first // RDD의 첫번째 요소 반환
scala> alsoReversed.top(4) // RDD에서 값을 내림차순으로 정렬해 상위 k개 반환 
```

scala 의 placeholder는 함수 호출과 함께 전달된 인자를 사용한다.

### 2.3.2 `distinct`와 `flatMap` 변환 연산자
일부 스칼라 컬렉션에도 `distinct`와 `flatMap` 함수가 제공되지만, 여기서 살펴볼 RDD 함수는 *분산 데이터에 적용되며, 실제 연산이 지연*된다.

#### 지난 한 주간 한 번 이상 물품을 구매한 고객의 수 알아내는 예제

고객의 구매 내역 로그는 매일 하나의 줄 끝에 구매한 고객의 ID가 덧붙여지고, 날짜가 바뀌면 개행한 뒤 같은 작업을 반복한다. 각 고객의 ID는 쉼표로 구분한다. 예를 들어 아래와 같은 로그가 client-ids.log 파일로 있다고 가정한다.

```
15,16,20,20
77,80,94
94,98,16,31
31,15,20
```

client-ids.log 파일을 읽어서 쉼표로 분리
```scala
scala> val lines = sc.textFile("/home/spark/client-ids.log")
scala> val idsStr = lines.map(line => line.split(","))
scala> idsStr.foreach(println(_))
[Ljava.lang.String;@739b302b
[Ljava.lang.String;@5dab3b4f
[Ljava.lang.String;@20858953
[Ljava.lang.String;@76a6ceb
scala> idsStr.first
res0: Array[String] = Array(15, 16, 20, 20)
scala> idsStr.collect
res1: Array[Array[String]] = Array(Array(15, 16, 20, 20), Array(77, 80, 94), Array(94, 98, 16, 31), Array(31, 15, 20))
```

각 일자별로 Array에 고객 ID 가 담긴 *이중 배열*을 받았는데, 전체를 *단일 배열*로 받고 싶다면? `flatMap` 을 사용

***`flatMap` 메서드 시그니처***
```scala
def flatMap[U](f: (T) => TraversableOnce[U]): RDD[U]
```

`map` 대신 `flatMap`을 이용해 다시 데이터 분할

```scala
scala> val ids = lines.flatMap(_.split(","))
scala> ids.collect
res2: Array[String] = Array(15, 16, 20, 20, 77, 80, 94, 94, 98, 16, 31, 31, 15, 20)
scala> ids.collect.mkString(";")
res3: String = 15; 16; 20; 20; 77; 80; 94; 94; 98; 16; 31; 31; 15; 20
scala> val intIds = ids.map(_.toInt)
scala> intIds.collect
res3: Array[Int] = Array(15, 16, 20, 20, 77, 80, 94, 94, 98, 16, 31, 31, 15, 20)
```

***`distinct` 메서드 시그니처***
```scala
def distinct(): RDD[T]
```

`distinct`는 *중복 요소를 제거*한 새로운 RDD를 반환

```scala
scala> val uniqueIds = intIds.distinct
scala> uniqueIds.collect
res4: Array[Int] = Array(16, 80, 98, 20, 94, 15, 77, 31)
scala> val finalCount = uniqueIds.count
finalCount: Long = 8
```

한 주 동안 상품을 구매한 고객은 총 8명

### 2.3.3 `sample`, `take`, `takeSample` 연산으로 RDD의 일부 요소 가져오기
#### `sample` 메서드
RDD에서 *무작위로 요소를 뽑아 새로운 RDD를 만드*는 변환 연산자

이전 예제의 구매 로그에서 *고객 ID의 30%를 무작위로 고른 샘플 데이터셋*이 필요한 상황이라면 RDD 클래스에 구현된 `sample`메서드를 사용할 수 있다.

***`sample` 메서드 시그니처***
```scala
def sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.random.nextLong): RDD[T]
```

* `withReplacement`: `true`인 경우 복원 샘플링/`false`인 경우 비복원 샘플링
* `fraction`: 복원 샘플링인 경우 각 요소가 샘플링될 횟수의 기댓값/비복원 샘플링의 경우 각 요소가 샘플링될 기대 확률
* `seed`: 난수 생성에 사용하는 시드

30%의 고객 비복원 샘플링
```scala
scala> val s = uniqueIds.sample(false, 0.3)
scala> s.count
res5: Long = 2 // 8개의 30% = 2.4에 근사
scala> s.collect
res6: Array[String] = Array(94, 21)
```
여기서는 결과가 2개 나왔지만 1개가 나올 수도, 3개가 나올 수도 있다. 비복원 샘플링의 `fraction`값은 각 요소가 샘플링 결과에 포함될 확률이다.

50%의 고객 복원 샘플링
```scala
scala> val swr = uniqueIds.sample(true, 0.5)
scala> swr.count
res7: Long = 5
scala> swr.collect
res8: Array[String] = Array(16, 80, 80, 20, 94) // 80이 두 번 샘플링된 것을 확인할 수 있음
```

#### `takeSample`메서드
확률값 대신 *정확한 개수*의 요소를 샘플링

***`takeSample` 메서드 시그니처***
```scala
def takeSample(withReplacement: Boolean, num: Int, seed: Long = Utils.random.nextLong): Array[T]
```

`sample`과 `takeSample`의 차이점
1. `takeSample`의 두 번째 인자가 정수형 변수. 즉, 개수의 기대값이 아니라 정확한 샘플의 개수를 받는다.
2. `sample`은 *변환 연산자*이지만 `takeSample`은 *Array를 반환하는 액션 연산자*

#### `take` 메서드
RDD의 파티션을 하나씩 처리해가며 지정된 개수의 요소를 모아 리턴

`take` 메서드의 결과는 *단일 머신에 전송*되므로 인자에 너무 큰 수를 지정하면 안됨

```scala
scala> uniqueIds.take(3)
res9: Array[String] = Array(80, 20, 15)
```

## 2.4 Double RDD 전용 함수
`Double`객체만 사용해 RDD를 구성하면 *암시적 변환(implicit conversion)* 을 이용해 몇 가지 추가 함수를 사용할 수 있음

#### Scala 의 암시적 변환
```scala
class ClassOne[T](val input: T){}
class ClassOneStr(val one:ClassOne[String]) {
    def duplicatedString() = one.input + one.input
}
class ClassOneInt(val one:ClassOne[Int]) {
    def duplicatedInt() = one.input.toString + one.input.toString
}

implicit def toStrMethods(one: ClassOne[String]) = new ClassOneStr(one)
implicit def toIntMethods(one: ClassOne[Int]) = new ClassOneInt(one)
```

위와 같이 구현한 경우, `ClassOne`에 넘어오는 input의 타입에 따라 `ClassOne[String]`인 경우 `ClassOneStr`로 `ClassOne[Int]`인 경우 `ClassOneInt`로 암시적으로 변환된다. 따라서 `input`이 `String`이나 `Int`인 경우에는 자동으로 `duplicatedString`이나 `duplicatedInt` 메서드를 사용할 수 있다.

Spark의 RDD에서도 위와 같은 암시적 변환이 일어나서, 데이터 타입에 따라 맞는 메서드를 추가적으로 사용할 수 있다. `Double` 객체만 포함하는 RDD는 `org.apache.spark.rdd.DoubleRDDFunctions` 클래스 인스턴스로 자동 변환된다. 이 클래스는 요소의 전체 합계, 평균, 표준 편차, 분산, 히스토그램을 계산하는 함수들을 제공한다.

### 2.4.1 double RDD 함수로 기초 통계량 계산
이전 예제에서 생선한 `intIds` RDD는 `Int` 객체로 구성되지만 `Double`객체로 자동 변환되므로 double RDD실습에 사용할 수 있다.

```scala
scala> intIds.mean
res0: Double = 44.785714285714285
scala> intIds.sum
res1: Double = 627.0
```

double RDD 에는 `stats` *행동 연산자*가 있는데, 이 연산자는 한 번의 호출로 RDD 요소의 전체 개수, 합계, 평균, 최댓값, 최솟값, 분산, 표준편차를 계산한다. 계산 결과는 `org.apache.spark.util.StatCount` 객체로  리턴된다.

```scala
scala> val statsResult = intIds.stats
res2: org.apache.spark.util.StatCounter = (count: 14, mean: 44.785714, stdev: 33.389859, max: 98.000000, min: 15.000000)
scala> statsResult.variance
res3: Double = 1114.8826530612246
scala> intIds.variance // 내부적으로 stats().variance 호출
res4: Double = 1114.8826530612246
scala> statsResult.stdev
res5: Double = 33.38985853610681
scala> intIds.stdev // 내부적으로 stats().stdev 호출
res6: Double = 33.38985853610681
```

### 2.4.2 히스토그램으로 데이터 분포 시각화
히스토그램의 X축에는 데이터 값의 구간(interval)을 그리고, Y축에는 각 구간에 해당하는 데이터 밀도나 요소 개수를 그린다.

double RDD의 히스토그램 *행동 연산자*에는 두 가지 버전이 존재
1. 구간 경계를 표현하는 `Double` 값의 배열을 받고, 각 구간에 속한 요소 개수를 담은 Array 객체를 반환  
`Double` 값의 배열은 반드시 오름차순이어야 하고, 두 개 이상의 요소를 포함해야 하며, 중복된 요소가 있으면 안됨
```scala
scala> intIds.histogram(Array(1.0, 50.0, 100.0)) // 1 <=x<50, 50<=x<=100
res7: Array[Long] = Array(9, 5)
```

2. 구간 개수를 받아 입력 데이터 전체 범위를 균등하게 나눈 후, 요소 두 개로 구성된 튜플 하나의 결과로 반환  
반환된 튜플의 첫 번째 요소는 입력 받은 구간 개수로 계산된 구간 경계 배열이며, 두 번째 요소부터는 1번 버전과 마찬가지로 각 구간에 속한 요소 개수의 배열
```scala
scala> intIds.histogram(3)
res8: (Array[Double], Array[Long]) = (Array(15.0, 42.66666666666667, 70.33333333333334, 98.0),Array(9, 0, 5))
```

### 2.4.3 근사 합계 및 평균 계산
대규모 `Double` 데이터셋을 다룰 때는 시간이 오래 걸릴 수가 있다. 이럴 때 스파크가 실험적으로 제공하는 `sumApprox` 나 `meanApprox` *행동 연산자*를 사용할 수 있다. 이 연산자들은 *지정된 제한 시간* 동안 근사값을 계산한다.

***`sumApprox` 메서드와 `meanApprox` 메서드의 시그니처***
```scala
sumApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble]
meanApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble]
```

여기서 `PartialResult`는 `finalValue` 필드와 `failure` 필드로 구성되어 있는데, `finalValue`는 `BoundedDouble`의 객체로 단일 결과 값이 아닌 값의 확률 범위(하한 및 상한), 평균값, 신뢰 수준을 제공한다.