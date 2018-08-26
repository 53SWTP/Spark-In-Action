# 1장 아파치 스파크 소개

## 하둡 Hadoop

![http://toppertips.com/hadoop-3-0-architecture/](http://toppertips.com/wp-content/uploads/2018/03/Hadoop-1.0-to-3.0-History-768x396.png)

### 동작 원리

하둡 프레임워크는 파일 시스템인 HDFS(*Hadoop Distributed File System*)과 데이터를 처리하는 맵리듀스(*MapReduce*) 엔진을 합친 것으로 대규모 클러스터 상의 데이터 처리를 다음과 같은 방식으로 단순화합니다.

1. 잡을 잘게 분할하고 클러스터의 모든 노드로 매핑(*map*)
2. 각 노드는 잡을 처리한 중간결과를 생성하고
3. 분할된 중간 결과를 집계(*reduce*)해서 최종 결과를 낸다.

이를 통해서 데이터를 여러 노드로 분산(*distribution*)하고 전체 연산을 잘게 나누어 동시에 처리하는 병렬 처리(*parallelization*)가 가능하고,  부분적으로 장애가 발생할 경우 전체 시스템은 동작할 수 있는 장애 내성(*fault tolerance*)을 갖는 시스템을 만들 수 있습니다.

맵리듀스의 핵심은 최소한의 API(`map`과 `reduce`) 만 노출해 대규모 분산 시스템을 다루는 복잡한 작업을 감추고, 병렬 처리를 프로그래밍적 방법으로 지원하는 점입니다. 또한 다루는 데이터가 크기 때문에 데이터를 가져와서 처리하는 것이 아니라, 데이터가 저장된 곳으로 프로그램을 전송합니다. 이 점이 바로 기존 데이터 웨어하우스 및 관계형 데이터베이스와 맵리듀스의 가장 큰 차이점입니다.

### 한계

- 맵리듀스 잡의 결과를 다른 잡에서 사용하려면 이 결과를 HDFS 에 저장해야 하기 때문에, 이전 잡의 결과가 다음 잡의 입력이 되는 반복 알고리즘에는 본질적으로 맞지 않습니다.
- 나눴다가 다시 합치는 하둡의 2단계 패러다임을 적용하기 경우가 있습니다.
- 하둡은 low-level 프레임워크이다 보니 데이터를 조작하는 high-level 프레임워크나 도구가 많아 환경이 복잡해졌습니다.

## 스파크 Spark

![http://backtobazics.com/big-data/spark/understanding-apache-spark-architecture/](http://backtobazics.com/wp-content/themes/twentyfourteen/images/spark/spark-architecture-and-ecosystem.png)

### 기능

빅데이터 애플리케이션에 필요한 대부분의 기능을 지원합니다.

- 맵리듀스와 유사한 일괄 처리 기능
- 실시간 데이터 처리 기능 (*Spark Streaming*)
- SQL과 유사한 정형 데이터 처리 기능 (*Spark SQL*)
- 그래프 알고리즘 (*Spark GraphX*)
- 머신 러닝 알고리즘 (*Spark MLlib*)

### 장점

스파크는 메모리 효율을 높여서 하둡의 맵리듀스보다 10~100배 빠르게 설계되었습니다. 맵리듀스처럼 잡에 필요한 데이터를 디스크에 매번 가져오는 대신, 데이터를 메모리에 캐시로 저장하는 인-메모리 실행 모델로 비약적인 성능을 향상시켰습니다. 이러한 성능 향상은 특히 머신 러닝, 그래프 알고리즘 등 반복 알고리즘과 기타 데이터를 재사용하는 모든 유형의 작업에 많은 영향을 줍니다.

사용자가 클러스터를 다루고 있다는 사실을 인지할 필요가 없도록 설계된 컬렉션 기반의 API 제공합니다. 또한 맵리듀스는 기본적으로 메인, 매퍼, 리듀스 클래스 세 가지를 만들어야 하지만 스파크는 동일한 문제를 간단한 코드로 짤 수 있습니다.

로컬 프로그램을 작성하는 것과 유사한 방식으로 분산 프로그램을 작성할 수 있습니다. 그리고 여러 노드에 분산된 데이터를 참조하고 복잡한 병렬 프로그래밍으로 자동 변환됩니다.

스칼라, 자바, 파이썬, R 을 지원합니다. 특히 스칼라를 사용하면 융통성, 유연성, 데이터 분석에 적합한 함수형 프로그래밍 개념을 사용할 수 있습니다.

대화형 콘솔인 스파크 shell(*Read-Eval-Print Loop, REPL*)을 이용해 간단한 실험을 하거나 테스트를 할 수 있습니다. 프로그램 문제를 테스트하기 위해 컴파일과 배포를 반복하지 않아도 되며, 전체 데이터를 처리하는 작업도 REPL에서 처리할 수 있습니다.

스파크는 다양한 유형의 클러스터 매니저를 사용할 수 있다. 스파크 standalone 클러스터, 하둡 YARN(*Yet Another Resource Negotiator*) 클러스터, 아파치 메소스(*Mesos*) 클러스터 등을 사용할 수 있습니다.

일괄 처리 작업이나 데이터 마이닝 같은 온라인 분석 처리 (*Online Analytical Processing, OLAP*)에 유용합니다. 

### 단점

데이터 셋이 적어서 단일 노드로 충분한 애플리케이션에서 스파크는 분산 아키텍처로 인해 오히려 성능이 떨어집니다. 또한 대량의 트랜잭션을 빠르게 처리해야 하는 애플리케이션은 스파크가 온라인 트랜잭션 처리를 염두에 두고 설계되지 않았기 때문에 유용하지 않습니다.

## 컴포넌트

### Spark Core

- 스파크 잡과 다른 스파크 컴포넌트에 필요한 기본 기능을 제공합니다.
- 특히 분산 데이터 컬렉션(데이터셋)을 추상화한 객체인 RDD(*Resilent Distributed Dataset*)로 다양한 연산 및 변환 메소드를 제공합니다. RDD 는 노드에 장애가 발생해도 데이터셋을 재구성할 수 있는 복원성을 가지고 있습니다.
- 스파크 코어는 HDFS, GlusterFS, 아마존 S3 등 다양한 파일 시스템에 접근할 수 있습니다.
- 공유 변수(*broadcast variable*)와 누적 변수(*accumulator*)를 사용해 컴퓨팅 노드 간 정보 공유합니다.
- 스파크 코어에는 네트워킹, 보안, 스케쥴링 및 데이터 셔플링(*shuffling*) 등 기본 기능을 제공합니다.

### Spark SQL

- 스파크와 하이브 SQL 이 지원하는 SQL 을 사용해 대규모 분산 정형 데이터를 다룰 수 있습니다.
- JSON 파일, Parquet 파일, RDB 테이블, 하이브 테이블 등 다양한 정형 데이터를 읽고 쓸 수 있습니다.
- DataFraem 과 Dataset 에 적용된 연산을 일정 시점에 RDD 연산으로 변환해 일반 스파크 잡으로 실행 합니다.

### Spark Streaming

- 실시간 스트리밍 데이터를 처리하는 프레임워크입니다.
- HDFS, 아파치 카프카(*Kafka*), 아파치 플럼(*Flume*), 트위터, ZeroMQ 와 더불어 커스텀 리소스도 사용할 수 있습니다.
- 이산 스트림(*Discretized Stream, DStream*) 방식으로 스트리밍 데이터를 표현하는데, 가장 마지막 타임 윈도 안에 유입된 데이터를 RDD 로 구성해 주기적으로 생성합니다.
- 다른 스파크 컴포넌트와 함께 사용할 수 있어 실시간 데이터 처리를 머신 러닝 작업, SQL 작업, 그래프 연산 등을 통합할 수 있습니다.

### Spark MLlib

- 머신 러닝 알고리즘 라이브러리입니다.
- RDD 또는 DataFrame 의 데이터셋을 변환하는 머신 러닝 모델을 구현할 수 있습니다.

### Spark GraphX 

- 그래프는 정점과 두 정점을 잇는 간선으로 구성된 데이터 구조입니다.
- 그래프 RDD(EdgeRDD 및 VertexRDD) 형태의 그래프 구조를 만들 수 있는 기능을 제공합니다.

## Spark Ecosystem

기존 하둡의 생태계(*ecosystem*)는 다음과 같습니다.

![http://www.bogotobogo.com/Hadoop/BigData_hadoop_Ecosystem.php](http://www.bogotobogo.com/Hadoop/images/Ecosystem/Hadoop_Ecosystem3.png)

### 데이터 변환 및 조작하는 함수를 제공하는 분석 도구

- [Apache Mahout](https://mahout.apache.org/) : 스케일러블한 머신 러닝 프레임워크
- [Apache Giraph](http://giraph.apache.org/) : 빅데이터 그래프 프로세싱
- [Apache Pig](https://pig.apache.org/) : 대용량 데이터 분석 플랫폼
- [Apache Hive](https://hive.apache.org/) : 데이터 웨어하우스
- [Apache Drill](https://drill.apache.org/) : 대규모 데이터의 SQL 분석 제공
- [Apache Impala](https://impala.apache.org/) : SQL 병렬 처리 엔진

### 클러스터 관리 도구

- [Apache Ambari](https://ambari.apache.org/) : 클러스터 관리 도구 (프로비저닝, 관리, 모니터링 등)

### 인터페이스 도구

하둡과 다른 시스템 간 데이터를 전송하는 도구

- [Apache Sqoop](http://sqoop.apache.org/) : 하둡과 다른 데이터 저장소 간 대용량 데이터 전송
- [Apache Flume](https://flume.apache.org/) : 대용량 로그 데이터를 수집
- [Apache Chukwa](http://chukwa.apache.org/) : 대용량 로그 데이터 수집 및 분석
  - [Flume comparison to Chukwa](https://groups.google.com/a/cloudera.org/forum/#!topic/flume-user/BMtsyGedcPo)
- [Apache Storm](http://storm.apache.org/) : 실시간 데이터 처리

### 인프라도구

기본적인 데이터 스토리지, 동기화, 스케줄링 등을 제공하는 도구

- [Apache Oozie](http://oozie.apache.org/) : 워크플로우 스케쥴러
- [Apache HBase](https://hbase.apache.org/) : 비관계형(*non-relational*) 분산(*distributed*) 데이터베이스
- [Apache Zookeeper](https://zookeeper.apache.org/) : 분산된 시스템을 관리하는 코디네이션 서비스(*coordination service*)

### 스파크로 대체할 수 있는 기능

- 그래프 프로세싱 Giraph -> Spark GraphX
- 머신러닝 Mahout -> Spark MLlib
- 실시간 데이터 처리 Storm -> Spark Streaming (2.2.1 이하는 완벽 대체 불가)
- 데이터 분석 Pig, 데이터 전송 Sqoop -> Spark Core 와 Spark SQL 로 대체
- SQL 관련 Impala, Drill -> Spark SQL 을 포괄하는 기능으로 함께 사용할 수 있음
- 인프라 관련 도구는 대체할 수 없음
