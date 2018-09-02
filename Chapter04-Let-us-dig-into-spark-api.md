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
