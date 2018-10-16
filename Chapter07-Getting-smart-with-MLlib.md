# 7장 MLlib로 더 똑똑해지자
머신 러닝: 프로그램을 일일이 작성하지 않아도 컴퓨터가 주어진 작업을 해결할 수 있는 알고리즘을 개발하고 연구하는 과학 분야. 즉, *주어진 작업을 해결하는 방법을 스스로 학습*

## 7.1 머신 러닝의 개요
일반적인 머신 러닝 프로젝트의 단계
1. **데이터 수집**: 로그 파일, 데이터베이스 레코드, 센서가 수집한 신호 등 다양한 소스에서 데이터 수집. 스파크를 이용해 RDB, CSV 파일, 원격 서비스 HDFS 같은 데이터 저장소나 스파크 스트리밍으로 데이터를 가져올 수 있다.
2. **데이터 정제 및 준비**: 다양한 비정형 포맷(텍스트, 이미지, 음성, 이진 데이터 등) 데이터의 경우 수치형 특징 변수로 변환하고, 결측 데이터를 처리하고, 같은 데이터의 형식을 통일(VW와 폭스바겐은 같은 제조사)하는 등 데이터 정제가 필요하다. 거기에 특징 변수에 스케일링을 적용해 데이터의 모든 차원을 비교 가능한 범위로 변환하기도 한다.
3. **데이터 분석 및 특징 변수 추출**: 데이터 내 상관관계를 조사하고, 필요하다면 데이터를 시각화해 추가적인 데이터 정제 과정(중복되는 피쳐의 경우 차원을 축소)을 거친 후 적절한 머신 러닝 알고리즘을 선택하고, 데이터를 *훈련 데이터셋*과 *검증 데이터셋*으로 나눈다. 혹은 *교차 검증 기법*을 활용해 데이터를 여러 훈련 데이터셋과 검증 데이터셋으로 계속 나누어 테스트하고 결과들의 평균을 계산한다.
4. **모델 훈련**: 머신 러닝의 학습 알고리즘에 훈련 데이터를 입력하면서 *알고리즘이 매개변수들을 학습*하도록 모델을 훈련시킨다.
5. **모델 평가**: *검증 데이터셋*을 적용하고 몇 가지 기준으로 모델 성능을 평가한다. 성능이 만족스럽지 않은 경우 데이터를 더 입력해서 학습을 더 하거나 학습에 사용되는 특징 변수를 바꾸는 등의 방법을 사용할 수 있다.
6. **모델 적용**: 완성된 모델을 운영 환경에 배포한다.

### 7.1.1 머신 러닝의 정의
> 머신 러닝은 데이터를 학습하고, 데이터를 예측하는 알고리즘을 구성하고 연구하는 과학 분야

머신 러닝은 알고리즘이 해야 할 일을 명시적으로 작성하는 일반 프로그램과는 대조된다. 문제 영역 지식을 프로그램에 명시적으로 작성하는 대신, 확률 통계와 정보 이론 분야의 여러 기법을 사용해 데이터에 내재된 지식을 찾아낸다. 그리고 이런 지식을 활용해 상황이나 입력에 맞게 *프로그램의 행동을 변경*하면서 유연하게 문제를 해결할 수 있다.

### 7.1.2 머신 러닝 알고리즘의 유형
* 지도 학습: *레이블이 포함된 데이터셋*을 사용. 즉, 예측 결과의 예상 정보 다시 말해 답을 알려준다. ex) 스팸 탐지, 음성 및 필기 인식, 컴퓨터 비전 등에서 많이 활용
* 비지도 학습: *레이블이 주어지지 않으며 알고리즘이 스스로 판단*한다. 군집화(clustering)나 이상 탐지(anomaly detection), 이미지 세분화(image segmentation) 등에 사용된다.

#### 7.1.2.1 지도 학습 및 비지도 학습 알고리즘의 세부 유형
지도 학습 알고리즘은 입력과 그 입력에 대한 결과 출력을 쌍으로 입력받아 *새롭게 주어진 입력을 출력으로 반환하는 함수*를 찾아내 미래에 관찰할 입력의 출력값을 예측한다. 지도 학습은 크게 *회귀(regression)* 와 *분류(classification)* 알고리즘으로 나눈다.

비지도 학습은 결과 출력 값을 모르므로 알고리즘이 *데이터에 숨겨진 구조를 스스로 찾아*야 한다. 전형적인 예 중 하나인 군집화는 *입력 데이터 간의 유사성을 분석해 데이터가 밀집된 지역, 즉 군집을 발견하는 것*을 목표로 한다.

붓꽃(iris) 데이터셋을 예제로 들어보자. 머신 러닝에서는 꽃받침의 길이와 너비를 입력의 *특징 변수(feature) or 차원(dimension)* 이라고 하며 붓꽃의 품종을 *출력 or 목표 변수(target variable), 레이블*이라고 한다. 여기서 머신러닝 알고리즘의 목표는 꽃받침의 길이와 너비를 입력 받아 붓꽃의 품종을 출력하는 함수를 찾아내는 것이다.

지도 학습의 경우 학습 데이터셋에 이미 붓꽃의 품종이 포함되어 있다. 꽃받침의 길이와 너비를 붓꽃의 품종으로 매핑하는 함수를 찾아낸다. 이렇게 알고리즘을 학습하는 데 사용되는 데이터셋을 *훈련 데이터셋*이라고 하며, 훈련된 모델에 또 다른 feature와 label 쌍으로 이루어진 데이터를 이용해 정확도를 평가한다. 이런 데이터는 *테스트 데이터셋*이라고 한다.

붓꽃 데이터셋을 이용해 비지도 학습의 예시를 들어보면, 비지도 학습은 붓꽃의 품종을 입력받지 않기 때문에 이런 경우 데이터세 숨겨진 범주와 각 입력 변수를 범주로 매핑하는 함수를 한번에 찾아야 한다. 군집화 알고리즘은 입력된 데이터를 가장 그럴싸하게 묶을 방법을 찾는 알고리즘이다.

#### 7.1.2.2 목표 변수의 유형에 따른 알고리즘 분류
* 회귀: 입력 변수셋을 바탕으로 *연속 출력 변수 값*을 예측하려고 시도. 연속(continuous) 또는 정량(quantitiative) 변수(실수)를 목표 변수로 설정. *특징 변수와 목표 변수 간의 관계를 최대한 가깝게 추정하는 수학 함수*를 찾는 것을 목표로 한다. 만약 특징 변수가 하나라면 특징 변수와 목표 변수의 관계는 이차원 평면에 이어진 선의 형태로 표현할 수 있을 것이다. 만약 특징 변수가 두 개라면 추정 함수는 3차원 공간 내 평면이며, 특징 변수가 더 많아지면 추정 함수는 *초평면(hyperplane)* 이 된다.
* 분류: 입력 변수셋을 클래스 두 개 이상 즉, *이산(discrete)값*으로 구분. 제한된 개수의 목표값을 가진다는 말이다. 범주형(categorical) 또는 정성(qualitative) 변수를 목표 변수로 설정. 목표 변수를 레이블(label), 클래스(class), 범주(category) 등으로 칭하며, 알고리즘을 분류기(classifier, categorizer), 인식기(recognizer) 라고도 한다.

### 7.1.3 스파크를 활용한 머신 러닝
머신 러닝 작업에서의 스파크의 장점
1. *주요 머신 러닝 알고리즘들을 분산 방식으로 구현*하여 제공. 분산 처리 방식이므로 *비교적 빠른 속도*를 자랑
2. 머신 러닝 작업의 대부분을 수행할 수 있는 통합 플랫폼을 제공하므로 *여러가지 작업(데이터 수집, 데이터 준비, 분석, 모델 훈련, 모델 평가 등)을 단일 시스템과 단일 API로 수행*할 수 있다.

스파크 머신 러닝의 핵심 API는 UC 버클리의 MLBase 프로젝트를 기반으로 한 MLlib다. 그리고 스파크 버전 1.2에서는 머신 러닝 API를 더욱 일반화해 여러 알고리즘을 동일한 방법으로 학습하고 튜닝할 수 있도록 지원하는 스파크 ML이라는 새로운 머신 러닝 API를 공개했다. 또한 스파크 ML은 머신 러닝과 관련된 모든 연산 작업을 시퀀스 하나로 모아 단일 연산처럼 한 번에 처리하는 파이프라인 기능을 제공한다. 스파크 ML과 MLlib 계속해서 별도의 API로 지원될 예정이다.

## 7.2 스파크에서 선형 대수 연산 수행
선형 대수학(linear algebra): 벡터 공간과 벡터 공간 사이의 (주로 행렬로 표현되는) 선형 연산 및 매핑을 다루는 수학의 한 분야. 부록 C 참조

스파크는 로컬 환경과 분산 환경에서 사용할 수 있는 벡터 및 행렬 클래스를 제공한다. 스파크의 분산 행렬을 사용하면 대규모 데이터의 선형 대수 연산을 분산 처리할 수 있다. 여기엔 `Breeze`와 `jblass`, Python의 경우 `NumPy`를 사용한다.

스파크는 아래 네 가지 종료의 데이터를 모두 지원
* 희소 벡터(or 행렬): 원소의 대다수가 0인 데이터. 따라서 0이 아닌 원소의 위치와 값의 쌍으로 표현한다.
* 밀집 벡터(or 행렬): 모든 데이터를 저장

### 7.2.1 로컬 벡터와 로컬 행렬
`org.apache.spark.mllib.linalg` 패키지가 로컬 벡터와 로컬 행렬 제공

#### 7.2.1.1 로컬 벡터 생성
`DenseVector` 클래스와 `SparseVector` 클래스로 생성할 수 있다. 이 두 클래스는 `Vector`라는 공통 인터페이스를 상속받아 정확히 동일한 연산을 지원한다. 로컬 벡터는 주로 `Vectors` 클래스의 `dense`나 `sparse` 메서드로 생성한다.

```scala
import org.apache.spark.mllib.linalg.{Vectors, Vector}

// dense는 모든 원소 값을 인라인(inline) 인수로 전달하거나 원소 값의 배열을 전달
val dv1 = Vectors.dense(5.0, 6.0, 7.0, 8.0)
val dv2 = Vectors.dense(Array(5.0, 6.0, 7.0, 8.0))
// sparse는 벡터 크기, 위치 배열, 값 배열을 전달
// 위치 배열은 정렬해서 전달해야 함
val sv = Vectors.sparse(4, Array(0, 1, 2, 3), Array(5.0, 6.0, 7.0, 8.0))
// 위 세 개의 벡터는 동일

// 특정 위치의 원소 가져오기
dv2(2) // 7.0

// 벡터 크기 조회
dv1.size // 4

// 벡터의 모든 원소를 배열 형태로 가져오기
dv2.toArray // Array(5.0, 6.0, 7.0, 8.0)
```

#### 7.2.1.2 로컬 벡터의 선형 대수 연산
스파크는 선형 대수 연산을 위해 내부적으로 *Breeze* 라이브러리를 활용하며, 로컬 벡터 및 로컬 행렬 클래스에는 `toBreeze` 함수가 있지만 `private` 이다. 하지만 선형 대수 연산을 수행하려면 *Breeze* 라이브러리가 필요하므로 아래와 같이 직접 변환 함수를 구현해 사용할 수 있다.

```scala
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
def toBreezeV(v: Vector): BV[Double] = v match {
    case dv: DenseVector => new BDV(dv.values)
    case sv: SparseVector => new BSV(sv.indices, sv.values, sv.size)
}

toBreezeV(dv1) + toBreezeV(dv2) // DenseVector(10.0, 12.0, 14.0, 16.0)
toBreezeV(dv1).dot(toBreezeV(dv2)) // 25.0 + 36.0 + 49.0 + 64.0 = 174.0
```

#### 7.2.1.3 로컬 밀집 행렬 생성
`Metrics` 클래스에서도 `dense`나 `sparse` 메서드를 사용해 *행렬을 생성*할 수 있다.

```scala
import org.apache.spark.mllib.linalg.{DenseMatrix, SparseMatrix, Matrix, Matrices}
import breeze.linalg.{DenseMatrix => BDM, CSCMatrix => BSM, Matrix => BM}
// 행(row) 개수, 열(column) 개수, 첫 번재 열부터 마지막 열까지의 순차 데이터
val dm = Matrices.dense(2, 3, Array(5.0, 0.0, 0.0, 3.0, 1.0, 4.0))
/*
dm: org.apache.spark.mllib.linalg.Matrix =
5.0  0.0  1.0
0.0  3.0  4.0
*/
```

`Metrices` 클래스가 제공하는 여러가지 종류의 행렬을 생성할 수 있는 단축 메서드
* `eye(n)`: *n x n*인 밀집 단위 행렬
* `speye(n)`: *n x n*인 희소 단위 행렬
* `ones(m, n)`: *m x n*이며 모든 원소가 1인 밀집 행렬
* `zeros(m, n)`: *m x n*이며 모든 원소가 0인 밀집 행렬
* `diag`: `Vector`를 인수로 받아 벡터의 원소를 행렬의 대각선에 배치한 대각 행렬(대각선 위치의 원소를 제외한 모든 원소가 0인 행렬)
* `rand`와 `randn`: 0~1 사이의 난수로 채운 `DenseMatrix` 생성
  * `rand`: 균등 분포에 따라 난수 생성
  * `randn`: 가우스 분포(정규 분포)에 따라 난수 생성
* `sprand` 및 `sprandn`: `rand`와 `randn`과 동일한 방식으로 `SparseMatrix` 객체 생성

#### 7.2.1.4 로컬 희소 행렬 생성
희소 행렬은 밀집 행렬보다 조금 더 복잡하다. 행과 열의 개수를 전달하는 것 까지는 같지만 원소 값을 CSC(Compressed Sparse Column) 포맷으로 전달해야 한다.

***CSC 포맷의 예***
```scala
colPtrs = [0 1 2 4], rowIndices = [0 1 0 1], elements = [5 3 1 4]
```

CSC의 구성
* 열지정 배열(colPtrs): 같은 열에 위치한 원소들의 범위  
`elements`의 index를 기준으로 구간을 나눠 열에 들어갈 항목들을 지정한다. 예시의 `colPtrs`의 `0 1 2 4` 값은 `elements`의 0번(포함)부터 1번째(미포함) 인덱스 사이에 있는 원소는 0번째 열, 1번부터 2번째 인덱스 사이에 있는 원소는 1번째 열, 2번부터 4번째 인덱스 사이에 있는 원소는 2번째 열이라는 뜻이다.
* 행위치 배열(rowIndices): 원소 배열의 각 요소가 위치할 행 번호  
`elements`의 각 원소의 행위치 즉 5는 0번째 행, 3은 1번째 행, 1은 0번째 행, 4는 1번째 행
* 원소 배열(elements): 희소 행렬의 각 원소

```scala
import org.apache.spark.mllib.linalg.{DenseMatrix, SparseMatrix}
val sm = Matrices.sparse(2, 3, Array(0, 1, 2, 4), Array(0, 1, 0, 1), Array(5.0, 3.0, 1.0, 4.0))
sm.asInstanceOf[SparseMatrix].toDense
/*
org.apache.spark.mllib.linalg.DenseMatrix =
5.0  0.0  1.0
0.0  3.0  4.0
*/
dm.asInstanceOf[DenseMatrix].toSparse
/*
org.apache.spark.mllib.linalg.SparseMatrix =
(0,0) 5.0
(1,1) 3.0
(0,2) 1.0
(1,2) 4.0
*/
```

#### 7.2.1.5 로컬 행렬의 선형 대수 연산
```scala
// 행렬 내 특정 위치의 원소 가져오기
dm(1,1) // 3.0

// 전치 행렬 (transposed matrix)
dm.transpose
/*
org.apache.spark.mllib.linalg.Matrix
5.0  0.0
0.0  3.0
1.0  4.0
*/

// 로컬 행렬 연산을 사용하기 위해 스파크의 로컬 행렬(또는 분산 행렬)을 Breeze 객체로 변환하는 메서드
import org.apache.spark.mllib.linalg.{DenseMatrix, SparseMatrix, Matrix, Matrices}
import breeze.linalg.{DenseMatrix => BDM,CSCMatrix => BSM,Matrix => BM}
def toBreezeM(m:Matrix):BM[Double] = m match {
    case dm:DenseMatrix => new BDM(dm.numRows, dm.numCols, dm.values)
    case sm:SparseMatrix => new BSM(sm.values, sm.numCols, sm.numRows, sm.colPtrs, sm.rowIndices)
}

import org.apache.spark.mllib.linalg.distributed.{RowMatrix, CoordinateMatrix, BlockMatrix, DistributedMatrix, MatrixEntry}
def toBreezeD(dm:DistributedMatrix):BM[Double] = dm match {
    case rm:RowMatrix => {
      val m = rm.numRows().toInt
       val n = rm.numCols().toInt
       val mat = BDM.zeros[Double](m, n)
       var i = 0
       rm.rows.collect().foreach { vector =>
         for(j <- 0 to vector.size-1)
         {
           mat(i, j) = vector(j)
         }
         i += 1
       }
       mat
     }
    case cm:CoordinateMatrix => {
       val m = cm.numRows().toInt
       val n = cm.numCols().toInt
       val mat = BDM.zeros[Double](m, n)
       cm.entries.collect().foreach { case MatrixEntry(i, j, value) =>
         mat(i.toInt, j.toInt) = value
       }
       mat
    }
    case bm:BlockMatrix => {
       val localMat = bm.toLocalMatrix()
       new BDM[Double](localMat.numRows, localMat.numCols, localMat.toArray)
    }
}
```

### 7.2.2 분산 행렬
대규모 데이터셋에 머신 러닝 알고리즘을 적용하려면 여러 머신에 걸쳐 저장할 수 있고, 대량의 행과 열로 구성할 수 있는 분산 행렬이 필요하다. 분산 행렬은 행과 열의 번호에 `Int`가 아닌 `Long`을 사용한다. 스파크의 `org.apache.spark.mllib.linalg.distributed` 패키지는 아래 네 가지 유형의 분산 행렬을 제공한다.

#### 7.2.2.1 RowMatrix
행렬의 *각 행을 `Vector` 객체에 저장해 RDD를 구성.* 다른 분산 행렬 클래스에는 `RowMatrix` 클래스로 변환하는 `toRowMatrix` 메서드가 제공된다. 하지만 `RowMatrix` 클래스를 다른 클래스로 변환하는 메서드는 제공되지 않는다.
* `rows` 멤버 변수: RDD를 가져온다
* `numRows`, `numCols` 메서드: 행렬의 행 개수와 열 개수
* `multiply` 메서드: `RowMatrix`에 로컬 행렬을 곱하여 새로운 `RowMatrix` 반환

#### 7.2.2.2 IndexedRowMatrix
`IndexedRow` 객체의 요소로 구성된 RDD 형태로 행렬을 저장. `IndexedRow` 객체는 *행의 원소들을 담은 `Vector` 객체와 이 행의 행렬 내 위치*를 저장.

```scala
// RowMatrix를 IndexedRowMatrix로 변환할 수 있음
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
val rmind = new IndexedRowMatrix(rm.rows.zipWithIndex().map(x => IndexedRow(x._2, x._1))
```

#### 7.2.2.3 CoordinateMatrix
`MatrixEntry` 객체의 요소로 구성된 RDD 형태로 행렬 값을 나누어 저장. `MatrixEntry`는 *개별 원소 값과 해당 원소의 행렬 내 위치(i, j)* 를 저장. 메모리를 효율적으로 사용하려면 희소 행렬을 저장할 때만 사용해야 한다.

#### 7.2.2.4 BlockMatrix
`((i, j), Matrix)` 튜플의 RDD 형태로 행렬을 저장. 다시 말해 행렬을 *블록 여러 개로 나누고,* 각 블록의 원소들을 로컬 행렬의 형태로 저장한 후 이 블록의 *전체 행렬 내 위치와 로컬 행렬 객체를 튜플로 구성.* 마지막에 위치한 서브 행렬들을 제외하고선 각 블록의 행 개수와 열 개수는 동일하다.(`validate` 메서드로 행/열 개수가 동일한지 검사 가능) 분산 행렬 간 덧셈 및 곱셈 연산을 지원.

#### 7.2.2.5 분산 행렬의 선형 대수 연산
스파크가 제공하는 분산 행렬의 선형 대수 연산은 제한적이다.

## 7.3 선형 회귀
### 7.3.1 선형 회귀 소개
가장 기본적인 통계 분석 기법 중 하나로 현재까지도 가장 널리 사용된다. 다른 회귀 기법들과 마찬가지로 *독립 변수셋을 사용해 목표 변수를 예측하고 이들 간의 관계를 계량화*한다. 이름에서 알 수 있듯이 *독립 변수와 목표 변수 사이에 **선형 관계**가 있다고 가정*한다.

### 7.3.2 단순 선형 회귀
단순 선형 회귀는 독립 변수 하나와 목표 변수 하나만 사용한다.

예제로 사용할 데이터는 보스턴 교외에 위치한 자가 거주 주택의 평균 가격와 이 가격을 예측하는 데 사용할 수 있는 특징 변수 13개로 구성된 데이터셋이다. 단순 선형 회귀 예제이므로 거주인당 평균 방 개수만 사용해 주택 가격을 예측해보자.

방이 많을수록 주택 가격이 더 비싸다는 건 굳이 선형 회귀 분석을 하지 않더라도 누구나 알 수 있다. 하지만 *선형 회귀는 방 개수와 주택 가격의 관계를 계량화*한다. 즉, 방 개수로 예상 주택 가격을 계산할 수 있다. X축에 특징 변수(평균 방 개수), Y축에 목표 변수(평균 주택 가격)를 설정해 그래프를 그려보면 선형 회귀는 *그 사이의 관계를 나타내는 직선*을 찾아낼 수 있다.

2차원 공간에 직선을 그리려면 *선의 기울기와 y절편 값*이 있어야 한다. 직선을 표현하는 공식은 아래와 같이 표현할 수 있다. 방 개수는 x, 주택 가격을 계산하는 함수는 h(hypothesis), y절편은 w0, 기울기는 w1

![hypothesis](https://latex.codecogs.com/gif.latex?h%28x%29%3Dw_0&plus;w_1x)

이제 우리는 h함수를 완성하기 위해 y절편(w0)와 기울기(w1)값을 찾아야 한다. 그러기 위해서 선형 회귀는 ***비용 함수(cost function)** 를 최소화*하는 방법을 사용한다. 비용 함수는 *직선이 모든 예제에 얼마나 잘 맞는지 측정하는 데 사용할 수 있는 단일 값*을 계산한다. 여러가지 방법이 있는데 선형 회귀는 *평균 제곱 오차(mean squared error)* 를 사용한다. 평균 제곱 오차는 *모든 m개의 예제에 대해 목표 변수의 예측값과 실제 값 차이를 제곱한 값의 평균*이다. 따라서 비용 함수 C를 다음과 같이 정의할 수 있다.

![cost_function](https://latex.codecogs.com/gif.latex?C%28w_0%2Cw_1%29%3D%5Cfrac%7B1%7D%7B2m%7D%5Csum_%7Bi%3D1%7D%5Em%28h%28x%5E%7B%28i%29%7D%29-y%5E%7B%28i%29%7D%29%5E2%3D%5Cfrac%7B1%7D%7B2m%7D%5Csum_%7Bi%3D1%7D%5Em%28w_0&plus;w_1x%5E%7B%28i%29%7D-y%5E%7B%28i%29%7D%29%5E2%29)

계속해서 가중치 w를 업데이트 하면서 단일 오차값을 계산했을 때, 특정 가중치에서 계산한 오차값이 이전 가중치의 오차값보다 작다면 새로운 가중치 즉, 새로운 모델이 데이터셋에 더 잘 맞는 모델이라고 할 수 있다. 따라서 우리는 *비용 함수의 결과 값이 최저인 가중치*를 찾아내야 한다.

선형 회귀 분석에 비용 함수로 평균 제곱 오차를 사용했을 때의 이점
1. 개별 편차가 음수여도 제곱해 양수 값으로 만들기 때문에 편차들이 서로 상쇄되지 않음
2. 비용 함수가 볼록 함수(convex function)다. 즉 지역 최저점 없이 전역 최저점만 존재
3. 최저점을 찾는 해석적 해가 존재

### 7.3.3 다중 선형 회귀로 모델 확장
모델에 더 많은 차원(독립 변수)를 활용해 다중 선형 회귀 모델로 확장해보자. 이제 주택 가격 예측 모델은 2차원 평면에 그래프로 그릴 수 없는 13차원 공간의 초평면이다. 다중 선형 회귀 모델로 확장해 비용함수 C의 최저점을 찾는 해법들을 살펴보자.

새로운 가설 함수는 아래와 같다. n값은 13이며, 대문자는 벡터값이다.

![multiple_linear_regression_hypothesis](https://latex.codecogs.com/gif.latex?h%28X%29%3Dw_0&plus;w_1x_1&plus;...&plus;w_nx_n%3DW%5ETX)

다만 우변의 벡터곱으로 나타낸 식에서 *W*와 *X*의 차원이 동일해야 하므로 X에 x0값 1을 대입하여 w0와 곱하도록 표현해야 한다.

![x_vector](https://latex.codecogs.com/gif.latex?X%5ET%3D%5Cbegin%7Bbmatrix%7D%201%20%26%20x_1%20%26%20...%20%26%20x_n%20%5Cend%7Bbmatrix%7D)

다중 선형 회귀 모델의 비용함수는 아래와 같다.

![multiple_linear_regression_cost_function](https://latex.codecogs.com/gif.latex?C%28W%29%3D%5Cfrac%7B1%7D%7B2m%7D%5Csum_%7Bi%3D1%7D%5Em%28W%5ETX%5E%7B%28i%29%7D-y%5E%7B%28i%29%7D%29%5E2)

#### 7.3.3.1 정규 방정식으로 최저점 찾기
X는 m개의 행과 n+1개의 열로 이루어진 행렬이다. W는 가중치 n+1개를 가진 벡터이며, y는 목표 변수 m개로 구성된 벡터다.

![normal_equation](https://latex.codecogs.com/gif.latex?XW%3Dy%5Cnewline%20X%5ETXW%3DX%5ETy%5Cnewline%20%28X%5ETX%29%5E%7B-1%7D%28X%5ETX%29W%3D%28X%5ETX%29%5E%7B-1%7DX%5ETy%5Cnewline%20W%3D%28X%5ETX%29%5E%7B-1%7DX%5ETy%5Cnewline)

#### 7.3.3.2 경사 하강법으로 최저점 찾기
하지만 정규 방정식을 사용하려면 업청난 행렬곱과 역행렬을 계산해야 하므로 많은 계산량이 필요하다. 특히 데이터셋이 클수록 기하급수적으로 늘어난다. 따라서 정규 방정식보다는 경사 하강법을 더 널리 사용한다.

경사하강법은 반복적으로 동작하는 알고리즘이다. 반복해서 값을 계산하다가 비용 함숫값이 일정 허용치(tolerance value)보다 작으면 수렴(converged)했다고 판단하고 종료한다.

0. 임의의 지점에서 시작
1. 각 가중치 매개변수 별로 *해당 가중치에 대한 비용함수의 편도함수를 계산*
2. 편도함수 값에 따라 비용 함수의 최저점으로 내려가기 위해 어떻게 가중치 매개 변수를 변경해야 할지 파악해서 가중치 갱신
3. 갱신된 지점(현재 시점에서 최적이라고 추정되는 가중치 값)에서 1번부터 반복

임의의 가중치 w_j에 대한 비용 함수 C의 편도함수

![partial_c](https://latex.codecogs.com/gif.latex?%5Cfrac%7B%5Cpartial%7D%7B%5Cpartial%20w_j%7DC%28W%29%3D%5Cfrac%7B1%7D%7Bm%7D%5Csum_%7Bi%3D1%7D%5Em%28h%28X%5E%7B%28i%29%7D%29-y%5E%7B%28i%29%7D%29x_j%5E%7B%28i%29%7D)

편도함수의 값이 음수일 때는 해당 가중치 매개변수가 *증가하는 방향*이 곧 비용함수가 *낮아지는 방향*

![gradient_descent](https://latex.codecogs.com/gif.latex?w_j%3A%3Dw_j-%5Cgamma%5Cfrac%7B%5Cpartial%7D%7B%5Cpartial%20w_j%7DC%28W%29%3Dw_j-%5Cgamma%5Cfrac%7B1%7D%7Bm%7D%5Csum_%7Bi%3D1%7D%5Em%28h%28X%5E%7B%28i%29%7D%29-y%5E%7B%28i%29%7D%29x_j%5E%7B%28i%29%7D%2C%20%5Ctext%7Bfor%20every%20j%7D)

모든 가중치에 대해 이 작업을 수행(모든 가중치 매개변수를 갱신)한 후 다시 비용 함수를 계산한다. 여기서 gamma는 step-size 매개변수이다.

## 7.4 데이터 분석 및 준비
```scala
import org.apache.spark.mllib.linalg.Vectors
val housingLines = sc.textFile("first-edition/ch07/housing.data", 6) // RDD partion 6개
val housingVals = housingLines.map(x => Vectors.dense(x.split(",").map(_.trim().toDouble)))

```

### 7.4.1 데이터 분포 분석
`MultivariateStatisticalSummary` 객체는 행렬의 각 열별 평균값, 최댓값, 최솟값 등을 제공한다.

```scala
import org.apache.spark.mllib.linalg.distributed.RowMatrix
val housingMat = new RowMatrix(housingVals)
val housingStats = housingMat.computeColumnSummaryStatistics()
// org.apache.spark.mllib.stat.MultivariateStatisticalSummary

// or

import org.apache.spark.mllib.stat.Statistics
val housingStats = Statistics.colStats(housingVals)
// org.apache.spark.mllib.stat.MultivariateStatisticalSummary

housingStats.count // 전체 열 개수
housingStats.numNonzeros // 각 컬럼에서 0이 아닌 원소의 수

housingStats.mean // 각 열의 평균값
housingStats.max // 각 열의 최댓값
housingStats.min // 각 열의 최솟값
housingStats.variance // 각 열의 분산

housingStats.normL1 // 각 열의 L1 norm (각 열별로 원소의 절댓값을 모두 더한 값)
housingStats.normL2 // 각 열의 L2 norm (각 열의 벡터 길이, 유클리디안 놈)
```

### 7.4.2 열 코사인 유사도 분석
*Column cosine similarity*는 두 열을 벡터 두 개로 간주하고 이들 사이의 각도를 구한 값이다. `RowMatrix`의 `columnSimilarities` 메서드는 상삼각 행렬(upper-triangular matrix)을 저장한 분산 `CoordinateMatrix` 객체를 반환한다.

> 상삼각 행렬: 행렬의 대각선 위쪽에 위치한 원소에만 값이 있고 대각선 아래쪽은 모두 0으로 채워진 행렬.

```scala
// housingColSims의 i번째 행과 j번째 열에 위치한 원소는 housingMat의 i번째 열과 j번째 열 사이의 유사도를 의미하며 -1에서 1 사이 값을 가진다.
val housingColSims = housingMat.columnSimilarities()
// -1: 두 열(벡터)의 방향이 완전히 반대
// 0: 두 열(벡터)이 직각
// 1: 두 열(벡터)의 방향이 같음

def printMat(mat:BM[Double]) = {
   print("            ")
   for(j <- 0 to mat.cols-1) print("%-10d".format(j));
   println
   for(i <- 0 to mat.rows-1) { print("%-6d".format(i)); for(j <- 0 to mat.cols-1) print(" %+9.3f".format(mat(i, j))); println }
}

printMat(toBreezeD(housingColSims))
```

위와 같이 매트릭스를 찍어보면 13번째 열 즉, 목표 변수(평균 주택 가격)과 나머지 특징 변수 열 간의 유사도를 확인할 수 있다. 이 예제 데이터에서는 6번째 행(평균 방 개수)의 값이 0.949로 가장 크며 따라서 *평균 주택 가격과 평균 방 개수의 유사도가 가장 큰 것*을 확인할 수 있다. 따라서 평균 방 개수가 *단순 선형 회귀에 가장 적합한 특징 변수*라고 할 수 있다.

### 7.4.3 공분산 행렬 계산
Covariance matrix를 사용해 입력 데이터셋의 여러 칼럼(차원 또는 특징 변수) 간 유사도를 알아볼 수 있다.

> 공분산 행렬: 변수 간에 선형 연관성을 모델링하는 통계 기법

```scala
val housingCovar = housingMat.computeCovariance()

printMat(toBreezeM(housingCovar))
```

결과는 대칭 행렬이며, 대각선에는 각 열의 분산 값이 존재한다. 그리고 나머지 부분에는 두 변수의 공분산 값이 기입된다.

공분산 값 해석
* 0: 두 변수 사이에 선형 관계가 없음
* 음수: 두 변수 값이 각각 평균에서 서로 반대 방향으로 이동한다는 의미
* 양수: 두 변수 값이 각각 평균에서 서로 같은 방향으로 이동한다는 의미

스파크는 또한 스피어만 상관 계수(Spearman correlation coefficient)와 피어슨 상관 계수(Pearson correlation coefficient)를 사용해 일련의 데이터 간 상관관계를 조사할 수 있는 `corr` 메서드를 제공한다.

### 7.4.4 레이블 포인트로 변환
`LabeledPoint` 구조체는 *목표 변수 값과 특징 변수 벡터로 구성*된다. 거의 모든 스파크 머신 러닝 알고리즘에 사용된다. `LabeledPoint` 클래스에 목표변수의 특징 변수를 분리해서 집어넣자.

```scala
import org.apache.spark.mllib.regression.LabeledPoint
val housingData = housingVals.map(x => {
  val a = x.toArray
  LabeledPoint(a(a.length-1), Vectors.dense(a.slice(0, a.length-1))) // label, features
})
```

### 7.4.5 데이터 분할
훈련 데이터셋과 검증 데이터셋으로 분할
* 훈련(training) 데이터셋: 모델 훈련에 사용 (일반적으로 전체 데이터의 80%)
* 검증(validation) 데이터셋: 훈련에 사용하지 않은 데이터에서도 모델이 얼마나 잘 작동하는지 확인하는 용도로 사용 (일반적으로 전체 데이터의 20%)

```scala
val sets = housingData.randomSplit(Array(0.8, 0.2))
// 지정된 비율에 근사하게 원본 데이터를 분할하고 RDD 배열로 반환
val housingTrain = sets(0)
val housingValid = sets(1)
```

### 7.4.6 특징 변수 스케일링 및 평균 정규화
하지만 여전히 데이터의 분포를 살펴 보면 특징 변수 값의 범위가 변수마다 크게 다르다는 사실을 알 수 있다. 이 데이터를 그대로 사용하면 모델 결과를 해석하기 어렵고, 데이터 변환 과정에 문제가 생길 수 있으므로 표준화를 한다. 데이터 표준화는 모델 정확도에 영향을 주지 않는다.

데이터 표준화 기법
* 특징 변수 스케일링(feature scaling): 데이터 범위를 비슷한 크기로 조정
* 평균 정규화(mean normalization): 평균이 0에 가깝도록 데이터를 옮기는 작업

```scala
import org.apache.spark.mllib.feature.StandardScaler
// 각 표준화 기법의 사용 여부를 전달한 후, 훈련 데이터셋에 맞춰 scaler 학습
// scaler는 입력 받은 데이터의 칼럼 요약 통계를 찾아낸 후 이 통계를 스케일링 작업에 활용
val scaler = new StandardScaler(true, true).fit(housingTrain.map(x => x.features))

val trainScaled = housingTrain.map(x => LabeledPoint(x.label, scaler.transform(x.features)))
val validScaled = housingValid.map(x => LabeledPoint(x.label, scaler.transform(x.features)))
```

## 7.5 선형 회귀 모델 학습 및 활용
`org.apache.spark.mllib.regression` 패키지의 `LinearRegressionModel` 클래스가 스파크의 선형 회귀 모델이며 이 객체는 선형 회귀 모델의 학습 알고리즘을 구현한 `LinearRegressionWithSGD` 클래스로 만들 수 있다. `LinearRegressionModel` 객체는 학습이 완료된 선형 회귀 모델의 배개변수가 저장된다.

`LinearRegressionWithSGD` 활용법 두 가지
1. `train` 정적 메서드 호출. 모델이 가중치만 학습하고 y절편을 학습할 수 없음
```scala
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
val model = LinearRegressionWithSGD.train(trainScaled, 200, 1.0)
```

2. 비표준 방식
```scala
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
val alg = new LinearRegressionWithSGD() // 객체 초기화
alg.setIntercept(true) // y절편을 학습하도록 설정
alg.optimizer.setNumInterations(200) // 반복 실행 횟수 설정
trainScaled.cache() // 캐시하는 것이 중요. 머신 러닝 알고리즘을 포함한 반복 알고리즘들은 같은 데이터를 여러 번 재사용하기 때문
validScaled.cache()
val model = alg.run(trainScaled) // train 시작
```

### 7.5.1 목표 변수 값 예측
학습된 모델이 있으면 *`predict` 메서드에 특징 변수들을 전달해 목표 변수 값을 예측*할 수 있다. 검증 데이터셋의 `LabeledPoint`를 실제 목표 변수 값과 예측 값으로 매핑해보자.

모델의 전반적인 성공 여부를 정량화하는 방법으로 *평균 제곱근 오차(Root Mean Squared Error, RMSE)* 를 계산할 수 있다.

```scala
val validPredicts = validScaled.map(x => (model.predict(x.features), x.label))
validPredicts.collect()

math.sqrt(validPredicts.map{case(p, l) => math.pow(p-l,2)}.mean())
```

### 7.5.2 모델 성능 평가
`RegressionMetrics` 클래스를 사용해 다각도로 회귀 모델을 평가할 수 있다.

```scala
import org.apache.spark.mllib.evaluation.RegressionMetrics
val validMetrics = new RegressionMetrics(validPredicts)
validMetrics.rootMeanSquaredError
validMetrics.meanSquaredError
```

* `meanAbsoluteError`: 실제 값과 예측 값 사이의 절대 오차 평균
* `r2`: 결정 계수(coefficient of determination). 0~1의 값을 가지며 *모델이 예측하는 목표 변수의 변화량을 설명하는 정도와 설명하지 못하는 정도를 나타내는 척도*다. 1에 가깝다면 목표 변수가 가진 분산의 많은 부분을 설명할 수 있다는 의미다. 하지만 목표 변수와 상관성이 적은 특징 변수를 추가해도 지표 값이 커지는 경향이 있다.
* `explainedVariance`: r2와 유사한 지표

### 7.5.3 모델 매개변수 해석
모델이 학습한 각 가중치 값은 개별 차원이 목표 변수에 미치는 영향력을 의미하며, 따라서 특정 가중치가 0에 가깝다면 *해당 차원은 목표 변수에 크게 기여하지 못한다*는 의미이다.

```scala
println(model.weights.toArray.map(x => x.abs).zipWithIndex.sortBy(_._1).mkString(", "))
```

위 결과는 영향력이 가장 작은 차원부터 가장 큰 차원을 오름차순으로 보여준다.

### 7.5.4 모델의 저장 및 불러오기
스파크 MLlib 모델들은 대부분 `save` 메서드로 저장할 수 있다. 스파크는 지정된 경로에 새 디렉터리를 생성하고 *모델의 데이터 파일과 메타데이터 파일을 Parquet 포맷으로 저장*한다.

선형 회귀 모델은 메타데이터 파일에 모델을 구현한 클래스 이름, 모델 파일의 버전, 모델에 사용된 특징 변수 개수를 저장한다. 데이터 파일에는 가중치와 y절편을 저장한다.

```scala
// save
model.save(sc, "ch07output/model)

// load
import org.apache.spark.mllib.regression.LinearRegressionModel
val model = LinearRegressionModel.load(sc, "ch07output/model")
```
