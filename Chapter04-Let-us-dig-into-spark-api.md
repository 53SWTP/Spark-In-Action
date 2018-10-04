# 4장 스파크 API 깊이 파헤치기

## 4-1 Pair RDD 다루기
1. Pair RDD란
    - Key, Value의  튜플로 구성된 RDD
2. Pair RDD생성
      - 어떤 형태든 2-element tuple을 값으로 포함하는 RDD는 PairRDD가 된다 (암시적변환 74p 참조)
      
        RDD[(K,V)] === PairRDD
3. Pair RDD 함수
      - pairRDD api문서 (http://bit.ly/2wuWgh9)

      **Transformation**
      
        keys:  키만 추출.
        mapValues: 각 키의 값(value)을 변경.
        flatMapValues: 값(value)을 0개 또는 한개 이상의 값으로 포함해 요소개수를 변경 f
        flatmap 및 모나드 설명(http://bit.ly/2NGgz1N)
        reduceByKey: 각 키의 모든 값을 동일한 타입의 단일 값으로 병합
        foldByKey: 각 키의 모든 값을 동일한 타입의 단일 값으로 병합 + 항등원 추가
        foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K,V)]
        aggregateByKey: 값을 변환한 후, 단일 값으로 병합
            aggregateByKey(zeroValue: U)(trans:(U, V) => , combOp: (U, U) ⇒ U))
     **Action**
        
        lookup: 특정키에 해당하는 value를 seq로 반환
4. 예제
```scala
object Chapter4 {
  def main(args: Array[String]): Unit = {

    /**
      * 4.1의 예제 수행하는 코드
      *
      * 요건
      *   구매 횟수가 가장 많은 고객에서는 곰 인형을 보낸다.
      *    바비 쇼핑몰 놀이 세트를 두 개 이상 구매하면 청구 금액을 5% 할인해 준다.
      *    사전을 다섯 권 이상 구매한 고객에게는 칫솔을 보낸다.
      *   가장 많은 금액을 지출한 고객에게는 커플 잠옷 세트를 보낸다.
      *
      * 제약사항
      *   사은품은 구매 금액이 0.00달러인 추가 거래로 기입
      */
    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")  //스파크 클러스터 사용여부 + executor 갯수
      .config("spark.eventLog.enabled", false)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // path는 환경에 맞게 변경
    val path = "/Users/hojinjung/Workspace/spark/sample/ch04_data_transactions.txt"

    val tranFile = sc.textFile(path);
    /* 데이터 한줄당 고객의 구매 내역으로 아래와 같은 정보가 포함됨
      2015-03-30#6:55 AM#51#68#1#9506.21  0:날짜 1: 일시 2: 고객ID 3:상품ID 4:구매수량, 5:구매금액
      총 라인 수는 1000

     */
    val tranData = tranFile.map(_.split("#"))
    var transByCust = tranData.map(tran => (tran(2).toInt, tran))

    // 고객 ID 수
    println("총 고객 수: "  + transByCust.keys.distinct.count)

    // 총 구매 수
    println("총 구매 횟수: " + transByCust.countByKey.values.sum)

    // 가장 많이 구매한 유저 아이디와 구매 횟수
    // _._2 는 각 엘리먼트의 두번째 튜플을 의미한다.
    println("가장 많이 구매한 유저 아이디와 구매 횟수: " + transByCust.countByKey().toSeq.sortBy(_._2).last)

    // 최다 구매자에게 곰인형(상품ID 4)의 구매기록 추가
    var compTrans = Array(Array("2015-03-30", "11:59 PM", "53", "4", "1", "0.00"))

    // 53의 키를 갖는 정보를 다 가져 옮
    transByCust.lookup(53)
    println(transByCust.lookup(53).foreach(tran => println(tran.mkString(", "))))


    // 바비 쇼핑몰 세트(상품ID 25)를 2개 이상 구매하면 5% 청구할인
    transByCust = transByCust.mapValues(tran => {
      if(tran(3).toInt == 25 && tran(4).toDouble > 1) {
        tran(5) = (tran(5).toDouble * 0.95).toString()
      }
      tran
    })

    // 사전(상품ID 81)을 5번 이상 구매한 고객에게 칫솔 (상품ID 70)을 추가
    transByCust = transByCust.flatMapValues(tran => {
      if(tran(3).toInt == 81 && tran(4).toDouble >= 5) {
        val cloned = tran.clone()
        cloned(5) = "0.00"
        cloned(3) = "70"
        cloned(4) = "1"
        List(tran, cloned)
      }
      else
        List(tran)
    })

    val amounts = transByCust.mapValues(t => t(5). toDouble)
    val totals = amounts.foldByKey(0)(_ + _).collect()

    println("가장 많은 금액을 지출한 고객: " + totals.toSeq.sortBy(_._2).last)

    // 최고가 지출자에게 커플 잠옷 세트(상품 ID 63)추가
    compTrans = compTrans :+ Array("2015-03-30", "11:59 PM", "76", "63", "1", "0.00")

    // RDD 병합
    transByCust = transByCust.union(sc.parallelize(compTrans).map(t => (t(2).toInt, t)))

    // 고객이 구매한 제품의 전체 목록을 가져오기
    val prods = transByCust.aggregateByKey(List[String]())((prods, tran) => prods ::: List(tran(3)), (prods1, prods2) => prods1 ::: prods2).collect

    println("고객이 구매한 제품의 전체 목록을 가져오기:" + prods)

    // 텍스트 파일로 다시 쓰기
    // transByCust.map(t => t._2.mkString("#")).saveAsTextFile("ch04output-transByCust")

  }


}
```
## 4-2 데이터 파티셔닝을 이해하고 데이터 셔플링 최소화
1. 데이터 파티셔닝
   - 데이터를 여러 클러스터 노드로 분할하는 메커니즘
   
2. 파티션
   - RDD 데이터를 조각한 부분
3. 파티셔너(Partitioner)
   - RDD의 각요소에 파티션 번호를 할당하는 객체
   - 종류 
     - HashPartitioner(기본 파티셔너)
       - 키의 hashCode를 이용해서 파티션 번호를 구함
         - (partitionIndex = hashCode % numberOfPartitions)
         - 랜덤하게 분포되지만, 대규모의 데이터를 상대적으로 적은 수의 파티션으로 나누면 데이터를 고르게 분산
     - RangePartitioner
       - 정렬된 RDD를 같은 간격으로 나눔(잘 사용 되지 않음)
     - 사용자정의 Partitioner (http://bit.ly/2MH59y9)
       - Pair RDD에서만 사용 가능
   - 적용
     - Pair RDD의 변환 연산자를 호출할때 두번째 인수로 전달(Int형 혹은 Partitioner) 
     ```scala
     rdd.foldByKey(afunction, 100) //100개의 파티션으로 나눔
     rdd.foldByKey(afunction, new HashPartitioner(100)) //100개의 파티션으로 나눔
     ```       
     - 따로 전달하지 않을시 부모RDD(이전 RDD)의 파티션 수를 따른다.
4. 셔플링
   - 파티션 간의 물리적인 데이터 이동
   - 파일 및 메모리 일기/쓰기 연산과 네트워크 연산이 포함되기 때문에 성능에 영향을 줌
   
     4.1 셔플링 발생조건
       - Partitioner를 명시적으로 변경하는 경우
         - Pair RDD의 변환 연산자는 대부분 파티셔너를 추가할 수 있도록 오버라이딩 되어 있음.
         - Partitioner의 클래스가 달라지면 셔플함.
         - Partitioner의 클래스와 파티션 수가 동일하면 동일한 Partitioner로 처리하여 셔플하지 않음.
       - Partitioner를 제거하는 경우
         - map과 flatMap인 경우 RDD의 Partitioner를 제거하여 특정 변환 연산자가 쓰이는 경우 셔플함
           - Pair RDD
             - aggregateByKey, foldByKey, reduceByKey, groupByKey, join, leftOuterJoin, rightOuterJoin, fullOuterJoin, subtractByKey
           - RDD
             - subtract, intersection, groupWith
           - sortByKey
           - partitionBy, coalesce(shuffle=true인경우)
   
     4.2 셔플링 기반 매개변수
  
   ```bash
       spark.shuffle.service.enabled=true      # 외부 셔플링 서비스설정
       spark.shuffle.manage=hash               # [hash/sort]가능
       spark.shuffle.consolidateFiles=false    # 셔플 도중 생기는 중간 파일의 통합 여부
       spark.shuffle.spill=true                # 메모리 리소스의 제한여부
       spark.shuffle.spill.compres=true        # 디스크에 쓸때 압축여부
       spark.shuffle.spill.batchSize=10000     # 데이터를 디스크로 내보낼 때 일괄로 직렬화 혹은 역직렬화 할 객체 수
       spark.shuffle.service.port=7337         # 외부 셔플링 서비스를 활성화할 경우 서비스 서버가 사용할 포트 번호
       #spark.memory.useLegacyMode=true        # spark.shuffle.memoryFraction을 사용하고 싶을때 true로 설정
       #spark.shuffle.memoryFraction=0.2       # 메모리 제한 임계치 (임계치가 넘어가면 디스크에 쓴다.) 1.6이후에는 사용되지 않음.
   ```
5. RDD 파티션 변경
   - 작업 부하를 효율적으로 분산시키거나, 메모리 문제를 방지하려고 사용
   - 파티션 변환 연산자
     - partitionBy
     - coalesce
     - repartition
     - repartitionAndSortWithPartition
6. 파티션 단위로 데이터 매핑
   - 파티션 내에서만 데이터가 매핑되도록 함으로써, 셔플링을 억제
     - mapPartitions
     - mapPartitionsWithIndex
7. glom
   - 파티션의 모든 요소를 하나의 배열로 모음
   - 데이터가 많을시 메모리 문제가 발생할 수 있다.
   
## 4-3 데이터 조인, 정렬, 그루핑
1. 데이터 조인
   - 여러 RDD간의 내용을 합침
     1. 조인 연산자
        - 특징
          - Partitioner를 지정하지 않으면 첫번째 RDD의 Partitioner를 사용
          - partitioner를 명시적으로 지정하지 않으면 HashPartitioner를 생성하고 spark.default.partitions의 값을 참조하거나, 두 RDD의 파티션 중 큰 값을 참조
        - 종류
          - join
            - (Pair RDD[(K, V)] , Pair RDD[(K, W)]) => Pair RDD(K, (V, W))] 
            - 한쪽에 있는 키요소는 제외
          - leftOuterJoin
            - (Pair RDD[(K,V)] , Pair RDD[(K,W)]) => Pair RDD(K, (V, Option(W)))] 
            - 두번째 RDD에만 있는 키의 요소는 결과에서 제외
          - rightOuterJoin
            - ((Pair RDD[(K,V)] , Pair RDD[(K,W)]) => Pair RDD(K, (Option(V), W))]
            - 첫번째 RDD에만 있는 키의 요소는 결과에서 제외
          - fullOuterJoin
            - ((Pair RDD[(K,V)] , Pair RDD[(K,W)]) => Pair RDD(K, (Option(V), Option(W)))]
     2. 변환 연산자
        - substract
        - substractByKey
        - cogroup
        - intersection
        - cartesian
        - zip
        - zipPartitions
 2. 데이터 정렬
    - 특징
      - 정렬 기준은 orderable 클래스만 가능하다.
      - 해당 기준 값이 orderable클래스를 상속하지 않았으면, Ordered 트레잇, Ordering 트레잇을 통해 구현 가능
      ```scala
        implicit val emplOrdering: Ordering[Employee] = Ordering.by(_.lastName)
      ```
    - 종류
      - repartitionAndSortWithinPartition
      - sortByKey
      - sortBy
 3. 데이터 그루핑
    - 특정 기준에 따라 단일 컬렉션으로 집계하는 연산
    - 종류
      - aggregateByKey
      - groupByKey 
        - 모든 값을 메모리로 가져오기 때문에 주의
      - groupBy
      - combineByKey
        - aggregateByKey, groupByKey, foldByKey, reduceByKey를 구현할때 사용
      
      **combineByKey 시그니처**
      ```scala
      def combineByKey[C](createCombiner: V => C,
          mergeValue: (C,  V) => C,
          mergeCombiners: (C, C) => C,
          partitioner: Partitioner,
          mapSideCombine: Boolean = true,
          serializer: Serializer = null): RDD[(K,C)]
      ```
      
      - 예제
      ```scala
          def main(args: Array[String]): Unit = {

            /**
              *
              * 어제 판매한 이름과 상품별 매출액 합계(알파벳 오름차순으로 정렬할 것)
              * 어제 판매하지 않은 상품 목록
              * 전일 판매 실적 통계: 각 고객이 구매한 구입한 상품의 평균 가격, 최저 가격 및 최고 가격, 구매금액 합계
              */


            val spark = SparkSession.builder()
              .appName("GitHub push counter")
              .master("local[*]")  //스파크 클러스터 사용여부 + executor 갯수
              .config("spark.eventLog.enabled", false)
              .getOrCreate()
            val sc = spark.sparkContext
            sc.setLogLevel("ERROR")

            // path는 환경에 맞게 변경
            val path = "/Users/hojinjung/Workspace/spark/sample/ch04_data_transactions.txt"

            val tranFile = sc.textFile(path);
            /* 데이터 한줄당 고객의 구매 내역으로 아래와 같은 정보가 포함됨
              2015-03-30#6:55 AM#51#68#1#9506.21  0:날짜 1: 일시 2: 고객ID 3:상품ID 4:구매수량, 5:구매금액
              총 라인 수는 1000

             */
            val tranData = tranFile.map(_.split("#"))

            val transByProd = tranData.map(tran => (tran(3).toInt, tran))
            var transByCust = tranData.map(tran => (tran(2).toInt, tran))
            val totalsByProd = transByProd.mapValues(t => t(5).toDouble).reduceByKey{case(tot1, tot2) => tot1 + tot2}
            val products = sc.textFile("/Users/hojinjung/Workspace/spark/sample/ch04_data_products.txt")
                             .map(line => line.split("#"))
                             .map(p => (p(0).toInt, p))

            // 판매한 상품 이름과 각 상품별 매출액
            val totalsAndProds = totalsByProd.join(products)
            println(totalsAndProds.first())

            val totalsWithMissingProds = products.leftOuterJoin(totalsByProd)
            // val totalsWithMissingProds = totalsByProd.rightOuterJoin(products)

            // 판매하지 않는 상품 목록 by outerJoin
            val missingProds = totalsWithMissingProds
                               .filter(x => x._2._1 == None)
                               .map(x => x._2._2)
            missingProds.foreach(p => println(p.mkString(", ")))

            // 판매하지 않는 상품 목록 by cogroup
            val prodTotCogroup = totalsByProd.cogroup(products)
            prodTotCogroup.filter(x => x._2._1.isEmpty)
              .foreach(x => println(x._2._2.head.mkString(",")))

            // 이름 알파벳 오름차순으로 정렬
            val sortedProds = totalsAndProds.sortBy(_._2._2(1))
            sortedProds.collect()


            // 전일 판매 실적 통계
            def createComb = (t:Array[String]) => {
              val total = t(5).toDouble
              val q = t(4).toInt
              (total/q, total/q, q, total) //초깃값
            }

            def mergeVal:((Double,Double,Int,Double), Array[String]) => (Double,Double,Int,Double) =
            {case((mn,mx,c,tot), t) => {
                val total = t(5).toDouble
                val q = t(4).toInt
                (scala.math.min(mn,total/q), scala.math.max(mx, total/q), c+q, tot+total)
              }}

            def mergeComb:((Double,Double,Int,Double), (Double,Double,Int,Double)) => (Double,Double,Int,Double) = {
              case((mn1, mx1, c1, tot1), (mn2, mx2, c2, tot2)) =>
                (scala.math.min(mn1, mn2), scala.math.max(mx1,mx2), c1+c2, tot1+tot2)
            }

            val avgByCust = transByCust.combineByKey(createComb, mergeVal, mergeComb,
              new HashPartitioner(transByCust.partitions.size))
              .mapValues({case(mn,mx,cnt,tot) => (mn, mx, cnt, tot, tot/cnt)})


            println(avgByCust.first)


          }
      ```      
## 4-4 RDD 의존 관계
   1. 의존 관계
      - 스파크의 실행모델은 방향성 비순환 그래프 (DAG), 이 그래프를 RDD 계보(lineage)라고 함
      - 좁은 의존관계(Narrow)
        - 1대1의존관계(one-to-one)
          - 셔플링이 필요하지 않은 모든 변환 연산
        - 범위형 의존관계(range)
          - 여러 부모에 대한 의존 과게를 하나로 결합한 경우 (union 연산)
      - 넓은 의존관계(wide)
          - 셔플링이 필요한 연산
      - 스테이지
        - 셔플링이 일어나기 전까지의 RDD계보
   2. RDD 체크포인트
      - 장애 발생시 연산을 줄이기 위해 특정 지점까지의 RDD계보를 저장 후 관리하는 것
      
## 4-5 누적 변수와 공유 변수
   1. 누적변수
      - SparkContext.accumulater(initialValue)를 호출해서 생성
      - value는 드라이버 프로그램에서만 참조할 수 있다.
      - 예제
      ```scala
      val acc = sc.accumulater(0, "acc name")
      val list = sc.paralleize(1 to 100000)
      list.foreach(x => acc.add(1)) //executor 수행코드
      acc.value // driver 수행코드
      list.foreach(x => acc.value) //예외

      ```
      - 사용자 정의 누적 변수
        - AccumulableParam을 구현해야함
          - 구현 메소드
            - zero(initialValue: T):  초깃값
            - addInPlace(v1: T, v2: T): T  누적 값 두개 병합
            - addAccumulator(v1: T, v2: V): T  누적 값에 새로운 값을 누적
          - 예제
          ```scala
          val rdd = sc.paralleize(1 to 100)
          import org.apache.spark.AccumulableParam
          implicit object AvgAccParam extends AccumulableParam[(Int, Int), Int] {
            def zero(v:(Int, Int) = (0, 0)
            def addInPlace(v1:(Int, Int), v2: (Int, Int)) = (v1._1+v2._1, v1._2+v2._2)
            def addAccumulator(v1:(Int, Int), v2:Int) = (v1._1+1, v1._2+v2)
            val acc = sc.accumulable((0,0))
            val mean = acc.value._2.toDoule /acc.value._1
          ```
      - Accumulable 컬렉션에 값 누적
        - SparkContext.accumulableCollection을 사용하면 컬렉션에 값 누적 가능
        ```scala
        import scala.collection.mutable.MutableList
        val colacc = sc.accumulableCollection(MutableList[Int]())
        rdd.foreach(x => colacc += x)
        colacc.value
        ```
   2. 공유 변수
      - SparkContext.broadcast(value)를 사용하서 생성하며, 직렬화 가능한 모든 변수는 value로 전달가능
      - executor가 공유 변수를 필요시 로드되었는지 확인한다. 로드 하지 않으면 드라이버에 청크단위로 데이터 요청 (pull-based)
      
      - 읽기 연산만 가능
      - 공유 변수 접근시는 value 메소드를 사용
      
      - destroy를 호출해서 완전히 삭제 가능. unpersist
      
      - 설정정보
      ```bash
      spark.broadcast.compress=true    # 전달 전에 데이터 압축 여부
      spark.io.compression.codec=lz4   # 압축 방식
      spark.broadcast.blockSize=4096   # 공유변수 전송시 사용할 청크 크기
      ```
      
          

   
