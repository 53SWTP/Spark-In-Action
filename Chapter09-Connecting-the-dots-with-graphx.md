# 9장 GraphX

## 9.1 스파크의 그래프 연산

**그래프 만드는 이유**
- 그래프 알고리즘 문제로 더 쉽게 풀릴수있다
    - 계층데이터
    - 사람과 사람, 사람과 SNS와의 관계
    - 하이퍼링크로 연결된 웹페이지


**그래프(graph) = 정점(vertex) + 간선(edge)**

- 간선은 방향성이 있다
- 정점에 속성객체(property object) 부여

- VertexRDD[VD]: (ID: Long, attr: VD)
    - ex) (1L, Person("Homer", 39)
    
- EdgeRDD[ED]: (srcID: Long, destId: Long, attr: ED)
    - ex) Edge(4L, 3L, "friend")


__그림이첨부가안돼서 직접보여줄게염😆__
![Graph_simson](https://github.com/53SWTP/Spark-In-Action/blob/master/9-1.png)

### 9.1.1 GraphX API를 사용해 그래프 만들기

```scala
import org.apache.spark.graphx._

case class Person(name:String, age:Int)
val vertices = sc.parallelize(Array((1L, Person("Homer", 39)),
    (2L, Person("Marge", 39)), (3L, Person("Bart", 12)),
    (4L, Person("Milhouse", 12))))
val edges = sc.parallelize(Array(Edge(4L, 3L, "friend"),
    Edge(3L, 1L, "father"), Edge(3L, 2L, "mother"),
    Edge(1L, 2L, "marriedTo")))

val graph = Graph(vertices, edges)


graph.vertices.count()
//4
graph.edges.count()
//4
```

---
### 9.1.2 그래프 변환

#### Edge의 attribute수정하기(`mapEdges`)

위의 예제에서 attribute를 Relationship 클래스 인스턴스로 변경
(나중에 더 많은 정보를 추가할수있다!)

```scala
case class Relationship(relation:String)
var newgraph = graph.mapEdges((partId, iter) =>
    iter.map(edge => Relationship(edge.attr))
)


newgraph.edges.collect()
//Array[org.apache.spark.graphx.Edge[Relationship]] = 
//  Array(Edge(3,1,Relationship(father)), 
//  Edge(4,3,Relationship(friend))
//  ...
```

#### Vertex의 attribute수정하기(`mapVertices`)
children수, friends수, married 여부 추가하기

```scala
case class PersonExt(name:String, age:Int, children:Int=0, friends:Int=0, married:Boolean=false)
val newGraphExt = newgraph.mapVertices((vid, person) => PersonExt(person.name, person.age))
```

요기까지하면 default값(children:0, friends:0, married: false)로 되어있다.

`aggregateMessages`를 사용해 추가한 attribute에 값을 추가해보자

>[aggregateMessages 메서드 시그니처] 
```
def aggregateMessages[A: ClassTag](
    //각 edge의 메시지 전송여부 검토 + 연결된 vertex로 메시지 전송(sendToSrc, sendToDst)
    sendMsg: EdgeContext[VD, ED, A] => Unit,
    //각 vertex에 도착한 메시지 집계
    mergeMsg: (A,A) => A,
    //어떤 데이터 필드를 전달할지 지정(optional)
    tripletFields: TripletFields = TripletFields.All)
: VertexRDD[A]
```



```scala
val aggVertices = newGraphExt.aggregateMessages(
    //sendMsg
    (ctx:EdgeContext[PersonExt, Relationship, Tuple3[Int, Int, Boolean]]) => {
        if(ctx.attr.relation == "marriedTo")
            { ctx.sendToSrc((0, 0, true)); ctx.sendToDst((0, 0, true)); }
        else if(ctx.attr.relation == "mother" || ctx.attr.relation == "father")
            { ctx.sendToDst((1, 0, false)); }
        else if(ctx.attr.relation == "friend")
            { ctx.sendToDst((0, 1, false)); ctx.sendToSrc((0, 1, false)); }
    },
    //mergeMsg
    (msg1:Tuple3[Int, Int, Boolean], msg2:Tuple3[Int, Int, Boolean]) => (msg1._1+msg2._1, msg1._2+msg2._2, msg1._3 || msg2._3))


aggVertices.collect.foreach(println)
// (4,(0,1,false))
// (2,(1,0,true))
// (1,(1,0,true))
// (3,(0,1,false))
```

aggregateMessages의 결과값이 VertexRDD[A]니까 실제 그래프에 반영하려면

`outerJoinVertices`로 aggVertices의 정점과 기존 그래프를 조인해야함


> [outerJoinVertices 메서드 시그니처]
```
def outerJoinVertices[U: ClassTag, VD2:ClassTag]
    //VID, 새로입력한속성U 로 구성된 RDD
    (other: RDD[(VertexId, U)])
    // VID, 기존그래프의VD, 새로입력한속성U (새로입력한 RDD에 U가 없으면 None객체리턴)
```


```scala
val graphAggr = newGraphExt.outerJoinVertices(aggVertices)(
    (vid, origPerson, optMsg) => { optMsg match {
    case Some(msg) => PersonExt(origPerson.name, origPerson.age, msg._1, msg._2, msg._3)
    case None => origPerson
    }}
)


graphAggr.vertices.collect().foreach(println)
// (4,PersonExt(Milhouse,12,0,1,false))
// (2,PersonExt(Marge,39,1,0,true))
// (1,PersonExt(Homer,39,1,0,true))
// (3,PersonExt(Bart,12,0,1,false))
```


> Pregel
- 구글이 개발한 대규모 그래프 처리시스템. GraphX 그래프 알고리즘 상당수가 프리겔사용
- superstep이라는 반복 시퀀스를 실행해 메시지를 전달
   - 첫번째 superstep은 모든 vertex에서 실행
   - -> 두번째 superstep은 메시지를 받은 vertex에서만 실행(여기서만 sendMsg 호출)


#### 그래프 부분집합 선택(필터링)
- **subgraph**: 주어진 조건을 만족하는 정점과 간선선택

> [subgraph 메서드 시그니처]
```
def subgraph(
    //edge 조건함수 (true면 포함)
    epred: EdgeTriplet[VD, ED] => Boolean = (x => true),
    //vertex 조건함수 (true면 포함)
    //(vertex가 없어지면 연결돤 edge는 자동으로 지워짐)
    vpred: (VertexId, VD) => Boolean = ((v,d) => true))
   : Graph[VD,ED]
```

```scala
//자녀가 있는 사람만 선택
val parents = graphAggr.subgraph(_ => true, (vertexId, person) => person.children > 0)


parents.vertices.collect.foreach(println)
//(2,PersonExt(Marge,39,1,0,true))
//(1,PersonExt(Homer,39,1,0,true))


parents.edges.collect.foreach(println)
//Edge(1,2,Relationship(marriedTo))
```

- **mask**: 그래프 vs 그래프에서 두 그래프에 모두 존재하는 vertex, edge만 유지
    - 속성개체 고려하지않고 그래프 객체만 인수로 받음
    
- **filter**: subgraph와 mask메서드를 조합한 메서드
    - 전처리함수, 정점 조건 함수, 간선 조건함수를 인수로 받음.
    - 전처리함수로 생성된 새로운 그래프를 subgraph로 일부만 선택 vs 원본그래프 mask함
    - 전처리 그래프를 만들 필요가 없을때 사용


## 9.2 그래프 알고리즘

**최단거리, 페이지랭크, 연결요소, 강연결요소**를 하나씩 볼건데 일단 아래 데이터를 가져온다.

### 9.2.1 예제 데이터셋

- articles.tsv: 문서이름이 한줄에 하나씩
- links.tsv: 각 링크별 출발문서이름, 도착문서이름

```scala
//내용이 없거나 주석인것 제거 + ID 부여
//cache는 문서 이름과 ID를 빠르게 검색할려고 articles RDD를 메모리에 저장
val articles = sc.textFile("first-edition/ch09/articles.tsv", 6).
               filter(line => line.trim() != "" && !line.startsWith("#")).
               zipWithIndex().cache()

val links = sc.textFile("first-edition/ch09/links.tsv", 6).
            filter(line => line.trim() != "" && !line.startsWith("#"))

//links의 문서이름 -> 문서ID replace
val linkIndexes = links.
     map(x => {
        val spl = x.split("\t");
        (spl(0), spl(1)) }).
     join(articles).map(x => x._2).join(articles).map(x => x._2)

val wikigraph = Graph.fromEdgeTuples(linkIndexes, 0)

//확인
wikigraph.vertices.count()
//Long = 4592
articles.count()
//Long = 4604

//링크파일에 일부 문서가 없어서 차이가있다 ㅎㅎ 아래로 확인해보면 그래프의 vertex와 동일
linkIndexes.map(x => x._1).union(linkIndexes.map(x => x._2)).distinct().count()
//Long = 4592
```

### 9.2.2 최단거리: 정점에서 다른정점으로 향하는 최단경로
- `ShortestPaths` 객체 사용

- Rainbow문서에서 14th_centry 문서로 가는 최단경로 찾기
    - step1. 문서ID 찾기
    - step2. 14th_century의 ID를 ShortestPaths의 run메서드에 전달
```scala
articles.filter(x => x._1 == "Rainbow" || x._1 == "14th_century").collect().foreach(println)
// (14th_century,10)
// (Rainbow,3425)
```

```scala
import org.apache.spark.graphx.lib._
//shortest의 vertex 속성 == 다른 vertex까지 거리를 담은 map
val shortest = ShortestPaths.run(wikigraph, Seq(10))

//Rainbow에 해당하는 vertex찾기
shortest.vertices.filter(x => x._1 == 3425).collect.foreach(println)
// (3425,Map(10 -> 2)) 

//실제 최소 클릭횟수는 2번!
```

### 9.2.3 페이지랭크(PR): 정점의 상대적 중요도 계산 (정점으로 들어오고 나가는 간선개수)
- Graph의 `pageRank` 메서드 사용

    - step1. 각 vertex의 PR값을 1로 초기화
    - step2. (vertex의 PR값 / 정점에서 나가는 간선 개수) + 인접 정점의 PR값
        * 페이지를 나가는 링크가 적고 들어오는 링크가 많을수록 PR값이 높다
    - step3. 모든 PR값의 변동폭이 수렴허용치보다 작을때까지 반복!

```scala
//수렴허용치: 0.001
// ranked = vertex 속성에 PR값이 저장된 그래프
val ranked = wikigraph.pageRank(0.001)

//PR값으로 줄세우기
val ordering = new Ordering[Tuple2[VertexId,Double]]{
    def compare(x:Tuple2[VertexId, Double], y:Tuple2[VertexId, Double]): Int = x._2.compareTo(y._2) }
//가장 중요한 페이지 열개 (페이지ID, PR값)
val top10 = ranked.vertices.top(10)(ordering)

//articles문서와 조인해서 문서이름 출력
sc.parallelize(top10).join(articles.map(_.swap)).collect.
sortWith((x, y) => x._2._1 > y._2._1).foreach(println)
// (vertexID, (PR값, 페이지이름))
// (4297,(43.064871681422574,United_States))
// (1568,(29.02695420077583,France))
// (1433,(28.605445025345137,Europe))
// (4293,(28.12516457691193,United_Kingdom))
// (1389,(21.962114281302206,English_language))
// (1694,(21.77679013455212,Germany))
// (4542,(21.328506154058328,World_War_II))
// (1385,(20.138550469782487,England))
// (2417,(19.88906178678032,Latin))
// (2098,(18.246567557461464,India))
```

### 9.2.4 연결요소(Connected Components): 그래프에서 서로 완전히 분리된 서브그래프 찾음
- 비방향성 그래프의 서브그래프
- vertex A -> 모든 vertex로 연결되는 그래프

![CC](https://i.imgur.com/EK3sI5c.png)

- 연결그래프인지 아닌지 확인하는게 좋다. 연결이 안되어있으면 알고리즘 결과도 이상함
- Graph의 `connectedComponents` 사용 (메서드는 GraphOps객체로 암시적으로 제공)

```scala
//각 연결요소는 vertexId가 가장 작은걸로 식별
val wikiCC = wikigraph.connectedComponents();

//그래프의 모든 연결 요소를 찾을 수 있다.
wikiCC.vertices.map(x => (x._2, x._2)).distinct()

//articlesRDD와 조인해 페이지 이름 조회
wikiCC.vertices.map(x => (x._2, x._2)).
distinct().join(articles.map(_.swap)).collect.foreach(println)
// (0,(0,%C3%81ed%C3%A1n_mac_Gabr%C3%A1in)) //Aedan mac Gabrain
// (1210,(1210,Directdebit))

//각 군집의 페이지개수
wikiCC.vertices.map(x => (x._2, x._2)).countByKey().foreach(println)
// (0,4589)
// (1210,3) 이 군집에 속한 페이지는 3개다. 대체로 잘연결됨

```

### 9.2.5 강연결요소(Strongly Connected Components, SCC): 모든 정점이 다른 모든 정점과 연결된 서브 그래프
- 방향성 그래프의 서브그래프. 
- 연결요소보다 더 엄격한 기준으로 군집을 만든다.
- vertex u에서 v로 향하는 경로가 있고 v에서 u로 향하는 경로가 있으면 강연결요소
- 아래 그림은 4개의 강연결요소가 있음
![SCC](https://i.imgur.com/voD317r.png)

- 강연결요소끼리는 다음과 같이 정보를 압축하는데 사용가능하다. (군집을 찾음)
![SCC2](https://i.imgur.com/ZG6xwHP.png)

출처: https://ratsgo.github.io/data%20structure&algorithm/2017/11/23/SCC/

- Graph의 `stronglyConnectedComponents(최대반복횟수)` 사용

```scala
val wikiSCC = wikigraph.stronglyConnectedComponents(100)

wikiSCC.vertices.map(x => x._2).distinct.count
// 519
//wikiSCC에 강연결요소가 519개있다

//어떤 SCC의 규모가 가장큰지
wikiSCC.vertices.map(x => (x._2, x._1)).countByKey().
    filter(_._2 > 1).toList.sortWith((x, y) => x._2 > y._2).foreach(println)
//(6,4051)  가장큰 SCC에는 정점이 4051개나 있다
//(2488,6)
//(1831,3)
//...
```

좀 작은 2488 SCC를 살펴보쟈
```scala
wikiSCC.vertices.filter(x => x._2 == 2488).
    join(articles.map(x=> (x._2, x._1))).collect.foreach(println)
//(2490,(2488,List_of_Asian_countries)) //대륙별 국가목록
//(2496,(2488,List_of_Oceanian_countries))
//(2498,(2488,List_of_South_American_countries))
//(2493,(2488,List_of_European_countries))
//(2488,(2488,List_of_African_countries))
//(2495,(2488,List_of_North_American_countries))

wikiSCC.vertices.filter(x => x._2 == 1831).
    join(articles.map(x => (x._2, x._1))).collect.foreach(println)
// (1831,(1831,HD_217107))  //행성들의 정보
// (1832,(1831,HD_217107_b))
// (1833,(1831,HD_217107_c))

wikiSCC.vertices.filter(x => x._2 == 892)
    .join(articles.map(x => (x._2, x._1))).collect.foreach(println)
// (1262,(892,Dunstable_Downs)) //영국 지역
// (892,(892,Chiltern_Hills))
```


## 9.3 A* 검색 알고리즘 구현

### 9.3.1 A* 알고리즘의 이해

- A*: 두 정점 사이의 최단 경로를 찾는 알고리즘
    - [시작] - 비용G - [정점V] -비용H- [도착]
- G+H 가 가장 작은 정점들을 선택한다
- 두 정점 사이의 거리를 예상할수 없는 그래프에는 이 알고리즘 사용못함
    - ex) 심슨가족 그래프

```
AStar object를 구현한 다음 
AStar.run(graph3dDst, 1, 10, 50, calcDistance3d, (e:Double) => e) 
```

![](https://github.com/53SWTP/Spark-In-Action/blob/master/9-2.png)

![A* 알고리즘 2차원지도]()


### 9.3.2 A* 알고리즘 구현
```scala
object AStar extends Serializable {
    import scala.reflect.ClassTag

    private val checkpointFrequency = 20

    /**
    * Computes shortest path between two vertices in a graph using the A* (A-star) algorithm.
    *
    * @tparam VD type of the vertex attribute
    * @tparam ED type of the edge attribute
    *
    * @graph [필수] A* 알고리즘을 실행할 그래프
    * @origin [필수]시작 VID
    * @dest 종료 VID
    * @maxIterations 최대 반복 횟수(default: 100)
    * @estimateDistance [필수]두 vertex 거리 계산하는 함수 (위에선 피타고라스)
    * @edgeWeight [필수]간선의 가중치 (위에선 1)
    * @shouldVisitSource 간선의 출발정점을 방문할지여부(default true)
    * @shouldVisitDestination 간선의 도착정점을 방문할지여부(default true)
    *
    * @return an array of vertex IDs on the shortest path, or an empty array if the path is not found.
    */
    def run[VD: ClassTag, ED: ClassTag](
            graph:Graph[VD, ED],
            origin:VertexId,
            dest:VertexId,
            maxIterations:Int = 100,
            estimateDistance:(VD, VD) => Double,
            edgeWeight:(ED) => Double,
            shouldVisitSource:(ED) => Boolean = (in:ED) => true,
            shouldVisitDestination:(ED) => Boolean = (in:ED) => true):Array[VD] = {

        val resbuf = scala.collection.mutable.ArrayBuffer.empty[VD]

        //시작 VID, 종료 VID가 그래프에 있는지 확인
        val arr = graph.vertices.flatMap(n =>
            if(n._1 == origin || n._1 == dest)
                List[Tuple2[VertexId, VD]](n)
            else
                List()).collect()
            if(arr.length != 2)
                throw new IllegalArgumentException("Origin or destination not found")
            val origNode = if (arr(0)._1 == origin) arr(0)._2 else arr(1)._2
            val destNode = if (arr(0)._1 == origin) arr(1)._2 else arr(0)._2

        //시작~종료 거리 추정
        var dist = estimateDistance(origNode, destNode)

        //vertex 속성 정의
        case class WorkNode(
            origNode:VD,
            g:Double=Double.MaxValue,
            h:Double=Double.MaxValue,
            f:Double=Double.MaxValue,
            visited:Boolean=false, //true면 기방문 그룹에 있는거
            predec:Option[VertexId]=None)   //이전의 VID

        //WorkNode로 작업그래프(gwork)생성
        var gwork = graph.mapVertices{ case(ind, node) => {
                if(ind == origin)
                    WorkNode(node, 0, dist, dist)   //출발지의 F,G,H 설정
                else
                    WorkNode(node)
                }}.cache()

        //현재 정점을 시작정점으로
        var currVertexId:Option[VertexId] = Some(origin)

        //메인루프 시작합니다!!!!!
        // 1. 현재 정점을 방문완료료 표시(기방문 그룹에 넣는다)
        // 2. 현재 정점과 인접한 이웃 정점들의 F,G, H계산
        // 3. 미방문 그룹에서 다음 반복차수의 현재 정점을 선정

        var lastIter = 0
        for(iter <- 0 to maxIterations  //최대반복횟수
            if currVertexId.isDefined;  //미방문그룹에 Vertex가 없으면 currVertexId == None이다
            if currVertexId.getOrElse(Long.MaxValue) != dest){  //종료VID에 도착
            lastIter = iter
            println("Iteration "+iter)

            //unpersist only vertices because we don't change the edges
            //작업그래프의 Vertex를 캐시에서 제거(unpersis), edge는 안고치니까 캐시에 그대로 남겨두기
            gwork.unpersistVertices()

            //Mark current vertex as visited
            gwork = gwork.mapVertices((vid:VertexId, v:WorkNode) => {
                if(vid != currVertexId.get) //현재VID가아니면 그대로
                    v
                else    //현재 VID만 visited = true
                    WorkNode(v.origNode, v.g, v.h, v.f, true, v.predec)
                }).cache()

            //checkpointFrequency = 20이므로 20회에 한번씩 체크포인트를 저장
            //체크포인트는 DAG계획을 저장함 FIXME: ㅎㅎㅎㅎ모르겠음
            //to avoid expensive recomputations and stack overflow errors
            if(iter % checkpointFrequency == 0)
                gwork.checkpoint()

            //subgraph로 이웃한 정점을 찾음
            val neighbors = gwork.subgraph(trip =>
                trip.srcId == currVertexId.get || trip.dstId == currVertexId.get)

            //Calculate G, H and F for each neighbor
            //새 G값을 전달받은 VertexRDD
            val newGs = neighbors.aggregateMessages[Double](ctx => {
                    //send message
                    if(ctx.srcId == currVertexId.get &&
                        !ctx.dstAttr.visited &&
                        shouldVisitDestination(ctx.attr)) {
                        ctx.sendToDst(ctx.srcAttr.g + edgeWeight(ctx.attr))
                    }
                    else if(ctx.dstId == currVertexId.get  &&
                        !ctx.srcAttr.visited &&
                        shouldVisitSource(ctx.attr)) {
                        ctx.sendToSrc(ctx.dstAttr.g + edgeWeight(ctx.attr))
                    }},
                //merge message
                (a1:Double, a2:Double) => a1, //never supposed to happen //FIXME: 왜지? 왜 a1만 선택할까
                TripletFields.All)

            //newGs + 작업그래프
            val cid = currVertexId.get
            gwork = gwork.outerJoinVertices(newGs)((nid, node, totalG) =>
                totalG match {
                    case None => node
                    case Some(newG) => {
                        if(node.h == Double.MaxValue) { // neighbor's H has not been calculated yet (it is not open, nor closed)
                            val h = estimateDistance(node.origNode, destNode)
                            WorkNode(node.origNode, newG, h, newG+h, false, Some(cid))
                        } else if(node.h + newG < node.f) { // 계산한 G + H 가 기존의 F보다 작으면 계산한걸로 replace!
                            WorkNode(node.origNode, newG, node.h, newG+node.h, false, Some(cid))
                        }
                        else
                            node
                    }
                })

            //다음으로 이동할 정점 선정
            //미방문 그룹에 있(으면서 H값이 계산되어있)는 vertex가져옴
            val openList = gwork.vertices.filter(v => v._2.h < Double.MaxValue && !v._2.visited)
            if(openList.isEmpty)
                currVertexId = None //미방문한게 없어? 목적지에 갈수없어 ㅠㅠ
            else {
                // F값이 가장 작은 vertex 가져옴
                val nextV = openList.map(v => (v._1, v._2.f)).
                    reduce((n1, n2) => if(n1._2 < n2._2) n1 else n2)
                currVertexId = Some(nextV._1)
            }
        } //main for loop끝!

        //currVertexId == destID니까 되돌아가면서 각 정점을 resbuf에 추가
        if(currVertexId.isDefined && currVertexId.get == dest) {
            var currId:Option[VertexId] = Some(dest)
            var it = lastIter
            while(currId.isDefined && it >= 0) {
                val v = gwork.vertices.filter(x => x._1 == currId.get).collect()(0)
                resbuf += v._2.origNode
                currId = v._2.predec
                it = it - 1
            }
        }
        else
            println("Path not found!")
        gwork.unpersist()

        //resbuf 순서를 뒤집고 최단 경로 반환
        resbuf.toArray.reverse
    }//run
}
```

### 9.3.3 테스트
- 3차원 공간의 점을 연결한 그래프. X,Y,Z좌표 저장
- Vertex 1 부터 Vertex 10으로 가는 최단경로를 계산

```scala
case class Point(x:Double, y:Double, z:Double)

val vertices3d = sc.parallelize(Array((1L, Point(1,2,4)), (2L, Point(6,4,4)), (3L, Point(8,5,1)), (4L, Point(2,2,2)),
    (5L, Point(2,5,8)), (6L, Point(3,7,4)), (7L, Point(7,9,1)), (8L, Point(7,1,2)), (9L, Point(8,8,10)),
    (10L, Point(10,10,2)), (11L, Point(8,4,3)) ))
val edges3d = sc.parallelize(Array(Edge(1, 2, 1.0), Edge(2, 3, 1.0), Edge(3, 4, 1.0),
    Edge(4, 1, 1.0), Edge(1, 5, 1.0), Edge(4, 5, 1.0), Edge(2, 8, 1.0),
    Edge(4, 6, 1.0), Edge(5, 6, 1.0), Edge(6, 7, 1.0), Edge(7, 2, 1.0), Edge(2, 9, 1.0),
    Edge(7, 9, 1.0), Edge(7, 10, 1.0), Edge(10, 11, 1.0), Edge(9, 11, 1.0) ))
val graph3d = Graph(vertices3d, edges3d)

//H값 계산
val calcDistance3d = (p1:Point, p2:Point) => {
    val x = p1.x - p2.x
    val y = p1.y - p2.y
    val z = p1.z - p2.z
    Math.sqrt(x*x + y*y + z*z)
}

// mapTriplets: mapEdges메서드와 마찬가지로 edge attr를 매핑, 다른점은 함수에 EdgeTriplet객체 전달
//EdgeTriplet객체 = Edge객체(srcId, dstId, attr) + 출발Vertex attr(srcAttr) + 도착Vertex attr(destAttr)
val graph3dDst = graph3d.mapTriplets(t => calcDistance3d(t.srcAttr, t.dstAttr))

sc.setCheckpointDir("/tmp/sparkCheckpoint")
/*
* @graph [필수] A* 알고리즘을 실행할 그래프
* @origin [필수]시작 VID
* @dest 종료 VID
* @maxIterations 최대 반복 횟수(default: 100)
* @estimateDistance [필수]두 vertex 거리 계산하는 함수
* @edgeWeight [필수]간선의 가중치
* @shouldVisitSource 간선의 출발정점을 방문할지여부(default true)
* @shouldVisitDestination 간선의 도착정점을 방문할지여부(default true)
*/
//graph3dDst에 edgeWeight를 계산해 넣어서 있으니까 그대로 반환
AStar.run(graph3dDst, 1, 10, 50, calcDistance3d, (e:Double) => e)
```


**RESULT**
```
Array[Point] = Array(Point(1.0,2.0,4.0), Point(6.0,4.0,4.0), Point(7.0,9.0,1.0), Point(10.0,10.0,2.0))
```
