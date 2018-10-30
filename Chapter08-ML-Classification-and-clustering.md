# 8장 스파크 ML로 만드는 분류와 군집화
* 분류: 지도 학습 알고리즘의 일종으로 **범주형(categorical) 목표 변수**, 즉 *일정 집합으로 한정된 변수 값을 예측*하는 기법
* 군집화: 입력 데이터를 클래스(군집)으로 그루핑한다는 점은 유사하나, 레이블 데이터가 없어도 *스스로 군집을 구성하는 데이터를 도출하는 비지도 학습 기법.*

최근에는 MLlib보다는 ML 라이브러리를 집중적으로 개발하는 추세이며, 이 장에서도 ML 라이브러리를 소개하고 MLlib와 차이점을 소개한다.

## 8.1 스파크 ML 라이브러리
ML 라이브러리는 머신 러닝에 사용하는 연산들을 일반화해 머신 러닝 과정을 간소하게 만드는 것이다. 이를 위해 *scikit-learn* 라이브러리에서 영향을 받아 *추정자, 변환자, 평가자*라는 개념을 도입했다. 이 객체들을 결합해 파이프라인을 구성할 수 있으며, 각 객체에 필요한 머신 러닝 매개변수들을 일관된 방법으로 적용할 수 있다. 스파크 ML의 모든 클래스는 DataFrame 객체를 사용해 데이터셋을 표현한다.

### 8.1.1 변환자, 추정자, 평가자
* 변환자(transformer): *한 데이터셋을 다른 데이터셋으로 변환.* 스파크 ML의 머신러닝 모델도 데이터셋에 예측 결과를 더하는 변환 작업을 수행하므로 변환자에 해당한다. 변환자의 핵심 메서드인 `transform`은 DataFrame을 필수 인수로 받고, 선택 인수로 매개변수 집합을 받는다.
* 추정자(estimator): *주어진 데이터셋을 학습해 변환자를 생성.* 추정자의 결과는 학습된 선형 회귀 모델(변환자)이라고 할 수 있다. 추정자의 핵심 메서드인 `fit`은 DataFrame을 필수 인수로 받고, 선택 인수로 매개변수 집합을 받는다.
* 평가자(evaluator): *모델 성능을 단일 지표로 평가.* RMSE나 R^2등을 평가 지표의 예로 볼 수 있다.

### 8.1.2 ML 매개변수
스파크 ML에서는 `Param`, `ParamPair`, `ParamMap` 클래스를 사용해 모든 추정자와 변환자의 매개변수를 동일한 방식으로 지정할 수 있다.
* `Param`: 매개변수 유형 정의. 매개변수 이름, 클래스 타입, 매개변수 설명, 매개변수를 검증할 수 있는 함수, 매개변수의 기본 값
* `ParamPair`: 매개변수 유형(즉, `Param` 객체)과 변수 값의 쌍을 저장
* `ParamMap`: 여러 `ParamPair` 객체 저장

ML 매개변수를 지정하는 두 가지 방법
* `ParamPair` 또는 `ParamMap` 객체를 추정자의 `fit` 메서드 또는 변환자의 `transform` 메서드에 전달
    ```scala
    linreg.fit(training, ParamMap(linreg.regParam -> 0.1))
    ```
* 각 매개변수의 `set` 메서드 호출
    ```scala
    linreg.setRegParam(0.1)
    ```

### 8.1.3 ML 파이프라인
머신 러닝에서는 최적의 매개변수 값을 찾기 위해 *변수 값을 조금씩 바꾸어 가며 동일한 계산 단계를 같은 순서로 반복*할 때가 잦다. 스파크 ML에서는 모델 학습 단계를 단일 파이프라인으로 구성할 수 있다. 파이프라인 또한 하나의 추정자처럼 작업을 수행하며, 그 결과로 `PipelineModel` 변환자를 생성한다.

## 8.2 로지스틱 회귀
7장의 주택 가격을 예측하는 문제는 전형적인 회귀 분석이다. 하지만 분류 분석의 목표는 *입력 예제들을 두 개 또는 더 많은 클래스로 구별*하는 것이다. 이제 이 문제를 분류 문제로 만들면 우리는 주택 평균 가격이 특정 금액보다 비싼지 여부를 예측하는 모델을 만들 수 있다. 이 경우 목표 변수 값은 특정 가격보다 높은 경우(목표 변수 1) 혹은 낮은 경우(목표 변수 0) 단 두 가지로 나뉠 수 있다. 이렇게 *목표 변수의 클래스가 단 두 개인 문제를 이진 반응(binary response) 분석*이라고 한다.

**로지스틱 회귀**는 특정 예제가 *특정 클래스에 속할 확률*을 계산한다.

### 8.2.1 이진 로지스틱 회귀 모델
로지스틱 회귀같은 분류 알고리즘은 예제 `x`가 특정 범주에 속할 확률 `p(x)` 를 계산할 수 있다. 기본적으로 확률 값은 0~1 사이의 값을 갖지만, 선형 회귀의 계산 결과는 이 범위를 벗어난다. 로지스틱 회귀는 대신 *로지스틱 함수(logistic function)* 을 사용해 확률을 모델링한다. 로지스틱 함수의 결과 값은 항상 0~1 사이에 있기 때문에 확률을 모델링하는 데 더 적합하다.

![logistic_function](https://latex.codecogs.com/gif.latex?p%28X%29%3D%5Cfrac%7Be%5E%7BW%5ETX%7D%7D%7B1&plus;e%5E%7BW%5ETX%7D%7D%3D%5Cfrac%7B1%7D%7B1&plus;e%5E%7B-W%5ETX%7D%7D)

여기서 아래 공식을 도출할 수 있다. 이 공식의 좌변을 오즈(odds) 또는 승산이라고 한다. 승산을 어떤 사건이 발생할 확률을 그 반대 경우의 확률로 나눈 값이다.

![odds](https://latex.codecogs.com/gif.latex?%5Cfrac%7Bp%28X%29%7D%7B1-p%28X%29%7D%3De%5E%7BW%5ETX%7D)

여기서 양변에 자연로그를 취하면 아래와 같다. 이 공식의 좌변은 로지스틱 함수의 로짓(logit) or 로그 오즈(log-odds)라고 한다.

![logit](https://latex.codecogs.com/gif.latex?ln%5Cfrac%7Bp%28X%29%7D%7B1-p%28X%29%7D%3DW%5ETX)

이 공식을 해석해 보면 p(X)는 입력 예제의 벡터 X가 1이라는 범주에 속할 확률이며, 1-p(X)는 그 반대의 경우의 확률이다.

![conditional-probability](https://latex.codecogs.com/gif.latex?p%28X%29%3Dp%28y%3D1%7CX%3BW%29%5Cnewline%201-p%28X%29%3Dp%28y%3D0%7CX%3BW%29)

로지스틱 회귀는 *우도 함수(likelihood function)의 결과값이 최대가 되는 지점을 가중치 매개변수의 최적 값*으로 사용한다. 여기서 우도 함수의 결과 값은 데이터셋 내 모든 예제의 레이블을 올바르게 예측할 결합 확률(joint probability)이다. 

우도 함수 공식에 자연 로그를 취하면 로그 우도(log-likelihood) 함수 또는 로그 손실(log-loss) 함수를 얻을 수 있다. 이 함수는 더 쉽게 최적화할 수 있어 로지스틱 회귀의 비용 함수로 자주 사용한다.

### 8.2.2 로지스틱 회귀에 필요한 데이터 준비
예제 데이터셋은 성인 인구 통계 데이터셋이다. 우리는 이 데이터셋의 성별, 연령, 학력, 결혼 여부, 인종, 출생 국가를 포함한 속성 13개를 활용해 인구 조사 대상의 연간 수입이 5만 달러를 넘는지 여부를 예측해본다.

```bash
# 로컬 클러스터의 모든 CPU코어를 할당
$ spark-shell --master local[*]
```

```scala
val census_raw = sc.textFile("first-edition/ch08/adult.raw", 4).map(x => x.split(", ").map(_.trim)).map(row => row.map(x => try { x.toDouble } catch { case _ : Throwable => x}))

import spark.implicits._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}

val adultschema = StructType(Array(
    StructField("age", DoubleType, true),
    StructField("workclass", StringType, true),
    StructField("fnlwgt", DoubleType, true),
    StructField("education", StringType, true),
    StructField("marital_status", StringType, true),
    StructField("occupation", StringType, true),
    StructField("relationship", StringType, true),
    StructField("race", StringType, true),
    StructField("sex", StringType, true),
    StructField("capital_gain", DoubleType, true),
    StructField("capital_loss", DoubleType, true),
    StructField("hours_per_week", DoubleType, true),
    StructField("native_country", StringType, true),
    StructField("income", StringType, true)
))

import org.apache.spark.sql.Row
val dfraw = spark.createDataFrame(census_raw.map(Row.fromSeq(_)), adultschema)
```

#### 8.2.2.1 결측 값 다루기
예제 데이터셋에는 일부 결측값(?로 표시)이 있다. 결측값들을 아래와 같은 다양한 방법으로 처리할 수 있다.

* 특정 칼럼(특징 변수)의 데이터가 과도하게 누락되어 있다면, 이 칼럼의 모든 데이터를 데이터셋에서 제거할 수 있다.
* 개별 예제(로우)에 데이터가 누락된 칼럼이 많다면, 이 로우를 데이터셋에서 제거할 수 있다.
* 각 칼럼의 결측 값을 해당 칼럼의 가장 일반적인 값으로 대체할 수 있다.
* 별도의 분류 또는 회귀 모델을 학습해 결측 값을 예측할 수 있다.

여기서는 세번째 방법을 사용하자. 예제 데이터셋에서 결측 값을 가진 칼럼들은 `workclass`, `occupation`, `native_country` 칼럼이다.

```scala
// workclass 칼럼의 최빈값 찾아보기
dfraw.groupBy(dfraw("workclass")).count().rdd.foreach(println)
/*
Private이 가장 많음
[Self-emp-not-inc,3862]
[Local-gov,3136]
[State-gov,1981]
[Private,33906]
[Without-pay,21]
[Federal-gov,1432]
[Never-worked,10]
[?,2799]
[Self-emp-inc,1695]
*/

// 얻어낸 최빈값 정보로 보간(impute) - 결측 값을 메꾼다는 뜻의 공식 용어
val dfrawrp = dfraw.na.replace(Array("workclass"), Map("?" -> "Private"))
val dfrawrpl = dfrawrp.na.replace(Array("occupation"), Map("?" -> "Prof-specialty"))
val dfrawnona = dfrawrpl.na.replace(Array("native_country"), Map("?" -> "United-States"))
```

#### 8.2.2.2 범주형 변수 다루기
분류 알고리즘은 범주형 변수를 다룰 수 없기 때문에 문자열 값을 가진 칼럼을 숫자로 변환해야 한다. 하지만 단순히 숫자(0, 1, 2, 3...)으로 매기면 숫자 값에 따라 순위를 매기므로 다른 방법을 사용해야 하는데 일반적으로 *원-핫 인코딩(one-hot encoding)* 기법을 사용한다.

원-핫 인코딩은 범주형 칼럼을 *다수의 이진 칼럼으로 확장*하는 기법이다. 이진 칼럼의 개수는 *원래 범주형 칼럼이 가진 **범주 개수**와 동일*하다. 원-핫 인코딩을 적용하면 여러 이진 칼럼 중 원래 범주형 칼럼 값에 해당하는 칼럼만 1이고 나머지 칼럼은 0이 된다.

범주형 변수를 다룰 수 있는 스파크 ML 라이브러리에서 제공하는 클래스
* `StringIndexer`
* `OneHotEncoder`
* `VectorAssembler`

#### 8.2.2.3 StringIndexer 사용
*`String` 타입의 범주 값을 해당 값의 정수 번호로 변환.* DataFrame을 바탕으로 `StringIndexerModel`을 변환하려는 범주형 칼럼 별로 각각 학습하고 사용해야 한다. `StringIndexerModel`은 칼럼을 변환하면서 해당 칼럼의 다양한 정보를 가진 메타데이터(이진형, 이름형, 숫자형 등)를 추가한다. 이 정보는 일부 알고리즘이 사용한다.

```scala
import org.apache.spark.sql.DataFrame
def indexStringColumns(df: DataFrame, cols: Array[String]): DataFrame = {
    import org.apache.spark.ml.feature.StringIndexer
    import org.apache.spark.ml.feature.StringIndexerModel

    var newdf = df
    for(col <- cols) {
        val si = new StringIndexer().setInputCol(col).setOutputCol(col+"-num")
        val sm: StringIndexerModel = si.fit(newdf)

        newdf = sm.transform(newdf).drop(col)
        newdf = newdf.withColumnRenamed(col+"-num", col)
    }

    newdf
}

val dfnumeric = indexStringColumns(dfrawnona, Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country", "income"))
```

#### 8.2.2.4 OneHotEncoder로 데이터 인코딩
`OneHotEncoder` 클래스는 지정된 칼럼에 원-핫 인코딩을 적용하고, 원-핫 인코딩이 적용된 희소 벡터를 새로운 칼럼에 저장한다.

```scala
def oneHotEncodeColumns(df: DataFrame, cols: Array[String]): DataFrame = {
    import org.apache.spark.ml.feature.OneHotEncoder
    var newdf = df
    for (c <- cols) {
        val onehotenc = new OneHotEncoder().setInputCol(c)
        onehotenc.setOutputCol(c + "-onthot").setDropLast(false)
        newdf = onehotenc.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c + "-onehot", c)
    }
    newdf
}

val dfhot = oneHotEncodeColumns(dfnumeric, Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "native_country"))
```

#### 8.2.2.5 VectorAssembler로 데이터 병합
* MLlib: `LabeledPoint` 타입으로 구성된 RDD 기반으로 동작
* 스파크 ML: `features` 칼럼과 `label` 칼럼을 기반으로 동작

다만, `LabeledPoint`의 RDD를 `toDF` 해서 DataFrame으로 변환하면 `features` 칼럼과 `label` 칼럼이 자동으로 생성된다.

데이터를 사용하려면 Vector 칼럼들과 다른 칼럼들을 모아 *모든 특징 변수를 포함하는 단일 `Vector` 칼럼으로 병합*해야 한다. `VectorAssembler`에 `inputCols`(입력 칼럼 이름 목록), `outputCol`(출력 칼럼 이름), DataFrame을 넘겨 단일 `Vector`를 만들 수 있다. `VectorAssembler`는 `inputCols`에 입력된 모든 칼럼 값을 `outputCol` 하나로 병합한다. 그 과정에서 특징 변수의 메타데이터를 칼럼에 추가한다.(몇몇 알고리즘이 이 정보를 사용함)

```scala
import org.apache.spark.ml.feature.VectorAssembler
val va = new VectorAssembler().setOutputCol("features").setInputCols(dfhot.columns.diff(Array("income")))
val lpoints = va.transform(dfhot).select("features", "income").withColumnRenamed("income", "label")
```

### 8.2.3 로지스틱 회귀 모델 훈련
MLlib에서는 `LogisticRegressionWithSGD` 클래스나 `LogisticRegressionWithLBGFS` 클래스를 사용해 `LogisticRegressionModel` 객체를 받는다. 스파크 ML에서는 `LogisticRegression` 클래스를 사용해 `LogisticRegressionModel` 객체를 받는다.

스파크 ML의 로지스틱 회귀는 loss function을 최소화하는데 LBFGS 알고리즘을 사용한다. 또 *특징 변수의 스케일링을 자동으로 수행*한다.

```scala
val splits = lpoints.randomSplit(Array(0.8, 0.2)) // DataFrame도 RDD처럼 데이터를 무작위로 분할하는 randomSplit 메서드 제공
// 머신 러닝 알고리즘은 동일한 작업을 반복하면서 동일한 데이터를 재사용하므로 데이터를 캐시하는 게 좋음
val adulttrain = splits(0).cache()
val adultvalid = splits(1).cache()

import org.apache.spark.ml.classification.LogisticRegression
val lr = new LogisticRegression
lr.setRegParam(0.01).setMaxIter(500).setFitIntercept(true)
val lrmodel = lr.fit(adulttrain)

// OR

import org.apache.spark.ml.param.ParamMap
val lrmodel = lr.fit(adulttrain, ParamMap(lr.regParam -> 0.01, lr.maxIter -> 500, lr.fitIntercept -> true))
```

#### 8.2.3.1 모델의 가중치 매개변수 해석
선형 회귀 모델에서 특징 변수의 가중치 값은 각 특징 변수의 중요도(목표 변수에 미치는 영향력) 였지만, 로지스틱 회귀에서는 가중치가 선형적 영향을 뜻하진 않는다. 로지스틱 회귀는 결국 각 *데이터가 특정 범주에 속할 확률*을 뜻하며, *모델의 가중치는 로그 오즈(log-odds)에 선형적인 영향*을 미친다.
```scala
lrmodel.coefficients
lrmodel.intercept
```

### 8.2.4 분류 모델의 평가
로지스틱 회귀 모델을 사용해 검증 데이터셋을 변환(모델 객체 또한 일종의 변환자)한 후, `BinaryClassificationEvaluator`로 모델 성능 평가

* `probability`: 해당 데이터가 관심 범주에 속하지 않을 확률과 속할 확률. 따라서 두 값을 더하면 항상 1
* `rawPrediction`: 해당 데이터가 관심 범주에 속하지 않을 로그 오즈와 속할 로그 오즈. 절댓값은 같지만 부호는 반대 즉, 두 값을 더하면 항상 0
* `prediction`: 1 or 0. 즉 해당 데이터셋이 관심 범주에 속할 가능성이 높은지 의미. 로지스틱 회귀는 이 값이 특정 임계치(기본값: 0.5)보다 클 때 관심 범주에 속할 가능성이 높다고 판단.

```scala
val validpredicts = lrmodel.transform(adultvalid) // outputCol, rawPredictionCol, probabilityCol등을 사용해 칼럼 이르 변경 가능
validpredicts.show()
/*
+--------------------+-----+--------------------+--------------------+----------+
|            features|label|       rawPrediction|         probability|prediction|
+--------------------+-----+--------------------+--------------------+----------+
|(103,[0,1,2,4,5,6...|  1.0|[-0.1239846119024...|[0.46904349262875...|       1.0|
|(103,[0,1,2,4,5,6...|  0.0|[0.40957636182511...|[0.60098629401234...|       0.0|
|(103,[0,1,2,4,5,6...|  1.0|[-1.0316677723977...|[0.26276089948051...|       1.0|
|(103,[0,1,2,4,5,6...|  0.0|[5.23312902506940...|[0.99469153007508...|       0.0|
|(103,[0,1,2,4,5,6...|  0.0|[1.01954030984983...|[0.73488304772654...|       0.0|
|(103,[0,1,2,4,5,6...|  1.0|[0.01639206641123...|[0.50409792484390...|       0.0|
|(103,[0,1,2,4,5,6...|  0.0|[0.45933085839352...|[0.61285542515160...|       0.0|
|(103,[0,1,2,4,5,6...|  1.0|[1.64331233465880...|[0.83798514359121...|       0.0|
|(103,[0,1,2,4,5,6...|  0.0|[2.55385922441372...|[0.92783235333614...|       0.0|
|(103,[0,1,2,4,5,6...|  0.0|[2.96290615954211...|[0.95086993696731...|       0.0|
|(103,[0,1,2,4,5,6...|  1.0|[1.13427307486968...|[0.75662661757830...|       0.0|
|(103,[0,1,2,4,5,6...|  1.0|[-0.3751364485730...|[0.40730046001659...|       1.0|
|(103,[0,1,2,4,5,6...|  0.0|[2.56910220110305...|[0.92884638252685...|       0.0|
|(103,[0,1,2,4,5,6...|  0.0|[1.94693606627861...|[0.87511216653386...|       0.0|
|(103,[0,1,2,4,5,6...|  1.0|[-0.4402088971794...|[0.39169119436311...|       1.0|
|(103,[0,1,2,4,5,6...|  1.0|[-0.0590638963200...|[0.48523831706866...|       1.0|
|(103,[0,1,2,4,5,6...|  1.0|[-0.2841086722733...|[0.42944676947554...|       1.0|
|(103,[0,1,2,4,5,6...|  1.0|[0.93123224988453...|[0.71732521539427...|       0.0|
|(103,[0,1,2,4,5,6...|  1.0|[1.84581714193666...|[0.86363523911176...|       0.0|
|(103,[0,1,2,4,5,6...|  0.0|[2.53101707460231...|[0.92628782803253...|       0.0|
+--------------------+-----+--------------------+--------------------+----------+
only showing top 20 rows
*/
```

#### 8.2.4.1 BinaryClassificationEvaluator 사용
```scala
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
val bceval = new BinaryClassificationEvaluator()
bceval.evaluate(validpredicts) // 0.904747792372501
bceval.getMetricName // areaUnderROC
```

위에서 사용한 지표는 *ROC 곡선의 밑면적(Area Under Receiver Operating Characteristic curve, AUROC)*

`bceval.setMetricName("areaUnderPR")`을 호출해 정밀도-재현율 곡선의 밑면적(Area Under Precision-Recall Curve, AUPRC)를 계산할 수도 있다.

#### 8.2.4.2 정밀도와 재현율
선형 회귀에서는 RMSE를 사용해 성능을 평가했지만, 분류 모델의 결과는 범주형이므로 RMSE를 사용할 수 없다.

분류 모델의 성능을 평가하려면 아래 항목들의 개수를 집계해야 한다.

* 진양성(True Positive, TP): 모델이 올바르게 양성으로 분류한 예제
* 위양성(False Positive, FP): 모델이 양성으로 분류했지만 실제로는 음성인 예제
* 진음성(True Negative, TN): 모델이 올바르게 음성으로 분류한 예제
* 위음성(False Negative, FN): 모델이 음성으로 분류했지만 실제로는 양성인 예제

위 항목들을 이용해 아래 지표들을 계산할 수 있다.

정밀도(Precision, P): 모델이 양성이라고 판단한 예제(TP+FP) 중 진양성 예제(TP)가 차지하는 비율

![P](https://latex.codecogs.com/gif.latex?P%20%3D%20%5Cfrac%7BTP%7D%7BTP%20&plus;%20FP%7D)

재현율(Recall, R): 모든 양성 예제(TP+FN) 중 모델이 양성으로 식별한(상기시킨 Recalled) 예제(TP)의 비율(민감도 *sensitivity*, 진양성률 *True Positive Rate (TPR)*, 적중률 *hit rate* 등으로도 부른다.)

![R](https://latex.codecogs.com/gif.latex?R%20%3D%20%5Cfrac%7BTP%7D%7BTP%20&plus;%20FN%7D)

극단적인 경우, 즉 모델의 예측 결과가 모두 0일 경우 진양성과 위양성이 없으므로 정밀도와 재현율도 둘 다 0이 되며, 모델이 모든 예제를 1로 예측하면 재현율은 1이 되고 정밀도는 데이터셋 중 양성 데이터의 비율이 크면 1에 가까워진다. 그러므로 분류 모델의 평가에는 F 점수(F-measure) or F1 점수(F1-score)지표를 주로 사용한다.

F1 점수: 정밀도와 재현율의 조화 평균

![F1](https://latex.codecogs.com/gif.latex?f_1%20%3D%20%5Cfrac%7B2PR%7D%7BP%20&plus;%20R%7D)

정밀도와 재현율이 모두 0일 때 0에 가까워지며 1에 가깝다면 F1 점수도 1에 가까워짐

#### 8.2.4.3 정밀도-재현율 곡선
확률 임계치(데이터 샘플이 관심 범주에 속하는지 여부를 판단)를 조금씩 변경하면서 임계치마다 모델의 정밀도(Y축)와 재현율(X축)을 계산해 그래프로 그리면 정밀도-재현율 곡선을 얻을 수 있다. 확률 임계치는 `setThreshold` 메서드로 변경 가능.

* 확률 임계치를 높이면 위양성이 줄어들면서 정밀도 상승, 대신 재현율 하락
* 확률 임계치를 낮추면 진양성과 위양성이 높아지면서 정밀도 하락, 재현율 상승

#### 8.2.4.4 ROC 곡선
AUROC는 스파크 분류 모델의 기본 지표로, PR 곡선과 유사하지만, 재현율을 Y축, 위양성률을 X축에 그린다.

위양성률(FPR): 모든 음성 예제(FP+TN) 중 모델이 양성으로 잘못 식별한 예제(FP)의 비율

![FPR](https://latex.codecogs.com/gif.latex?FPR%20%3D%20%5Cfrac%7BFP%7D%7BFP&plus;TN%7D)

이상적인 모델은 위양성률(FPR)이 낮고, 진양성률(TPR)이 높아야 한다. 따라서 ROC 곡선은 왼쪽 위 모서리에 가깝게 그려져야 한다.

데이터셋 중 양성 예제의 비율이 낮을 때는 PR 곡선을 사용하는 것이 적절하다.

### 8.2.5 k-겹 교차 검증 수행
교차 검증은 모델을 여러 차례 검증하고 검증 결과의 평균을 최종 결과로 사용하므로, 안정적으로 모델 성능을 파악할 수 있으며 데이터의 특정 부분에 모델이 과적합될 가능성도 줄일 수 있다.

k-겹 교차 검증은 전체 데이터셋을 *부분 집합 k개로 균등하게 나눈 후 전체 데이터셋에서 각 부분 집합만 제외한 훈련 데이터셋 k개를 사용해 모델 k개를 훈련*시킨다. 제외한 부분은 검증 데이터셋으로 사용한다.

최적 매개변수 값을 찾는 것도, 매개 변수 조합 별로 모델 k개를 학습해 가장 낮은 값을 기록한 조합을 선택하면 된다.

`CrossValidator`로 k-겹 교차 검증을 수행할 수 있다.

```scala
import org.apache.spark.ml.tuning.CrossValidator
val cv = new CrossValidator().setEstimator(lr).setEvaluator(bceval).setNumFolds(5)

// setEstimatorParamMaps 메서드에 매개변수 값의 조합 ParamMap 배열 전달
// 혹은 매개변수 값의 조합을 ParamGridBuilder 클래스로 생성
import org.apache.spark.ml.tuning.ParamGridBuilder
val paramGrid = new ParamGridBuilder().addGrid(lr.maxIter, Array(1000)).addGrid(lr.regParam, Array(0.0001, 0.001, 0.005, 0.01, 0.05, 0.1)).build()

cv.setEstimatorParamMaps(paramGrid)
// 매개변수 조합별로 모델을 학습하고, 앞서 전달한 평가자 bceval의 측정 지표를 기준으로 가장 좋은 성능을 기록한 모델 반환
val cvmodel = cv.fit(adulttrain) // CrossValidatorModel 타입 객체

import org.apache.spark.ml.classification.LogisticRegressionModel
// 가장 좋은 성능을 기록한 로지스틱 회귀 모델 가져오기
cvmodel.bestModel.asInstanceOf[LogisticRegressionModel].coefficients
// 가장 좋은 성능을 기록한 일반화 매개변수 값 가져오기 (파이썬은 안 됨)
cvmodel.bestModel.parent.asInstanceOf[LogisticRegression].getRegParam

// 검증 데이터셋으로 최적의 모델 성능 테스트
new BinaryClassificationEvaluator().evaluate(cvmodel.bestModel.transform(adultvalid))
```

### 8.2.6 다중 클래스 로지스틱 회귀
다중 클래스 분류는 세 개 이상의 클래스로 식별하는 분류 문제를 의미한다. MLlib의 `LogisticRegressionWithLBFGS`나 스파크 ML의 `LogisticRegression`을 사용할 수 있다.

하지만 여기선 이진 분류 모델을 사용해 다중 클래스 분류를 수행하는 *One Vs. Rest(OVR)* 전략을 소개한다. OVR은 *각 클래스당 모델을 하나씩 훈련시키면서 다른 클래스들(Rest)을 음성으로 간주하는 방식*을 사용한다. 예측할 때는 *모든 모델 중 가장 높은 확률을 출력하는 모델의 클래스*를 레이블이라고 예측한다.

여기선 손으로 쓴 숫자 데이터를 분류하는 예제를 살펴본다.

```scala
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
// 각 로우에는 밝기 값이 0~100 사이인 픽셀 정보 16개가 저장되어 있음
val penschema = StructType(Array(
    StructField("pix1", IntegerType, true), StructField("pxi2", IntegerType, true),
    StructField("pix3", IntegerType, true), StructField("pxi4", IntegerType, true),
    StructField("pix5", IntegerType, true), StructField("pxi6", IntegerType, true),
    StructField("pix7", IntegerType, true), StructField("pxi8", IntegerType, true),
    StructField("pix9", IntegerType, true), StructField("pxi10", IntegerType, true),
    StructField("pix11", IntegerType, true), StructField("pxi12", IntegerType, true),
    StructField("pix13", IntegerType, true), StructField("pxi14", IntegerType, true),
    StructField("pix15", IntegerType, true), StructField("pxi16", IntegerType, true),
    StructField("label", IntegerType, true)
))
val pen_raw = sc.textFile("first-edition/ch08/penbased.dat", 4).map(x => x.split(", ")).map(row => row.map(x => x.toDouble.toInt))

import org.apache.spark.sql.Row
val dfpen = spark.createDataFrame(pen_raw.map(Row.fromSeq(_)), penschema)

import org.apache.spark.ml.feature.VectorAssembler
val va = new VectorAssembler().setOutputCol("features")
va.setInputCols(dfpen.columns.diff(Array("label")))
val penlpoints = va.transform(dfpen).select("features", "label")

val pensets = penlpoints.randomSplit(Array(0.8, 0.2))
val pentrain = pensets(0).cache()
val penvalid = pensets(1).cache()

import org.apache.spark.ml.classification.OneVsRest
val penlr = new LogisticRegression().setRegParam(0.01)
val ovrest = new OneVsRest()
ovrest.setClassifier(penlr) // ovr에 사용할 분류 알고리즘 지정

// 각 숫자당 하나씩 총 열 개의 로지스틱 회귀 모델 저장
val ovrestmodel = ovrest.fit(pentrain) // train model

val penresult = ovrestmodel.transform(penvalid)// validation

/* MulticlassMetrics를 사용해 모델 정확도 평가 */
val penPreds = penresult.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getInt(1).toDouble))

import org.apache.spark.mllib.evaluation.MulticlassMetrics
val penmm = new MulticlassMetrics(penPreds)

penmm.precision(3) // 개별 클래스의 정밀도
penmm.recall(3) // 개별 클래스의 재현율
penmm.fMeasure(3) // 개별 클래스의 F 점수
penmm.confusionMatrix // 혼동 행렬(confusion matrix) 출력
// 행은 데이터셋의 실제 클래스, 열은 예측된 클래스
// 즉, i, j 원소는 i번 클래스이지만 모델이 j클래스라고 예측한 예제 개수
// 대각선 원소는 정확하게 분류한 예제 개수
```

## 8.3 의사 결정 트리와 랜덤 포레스트
* 의사 결정 트리: 트리 형태의 규칙 집합을 특징 변수에 적용해 입력 예제를 분류하며 규칙 집합은 알고리즘이 훈련 데이터셋에서 학습하거나 사용자가 직접 작성할 수 있다. 사용이 간편. 분류 및 회귀 분석 수행 가능. 의사 결정 규칙을 그림으로 표현할 수 있으므로 이해하기가 쉽고, 내부 동작을 직관적으로 이해 가능. 데이터 정규화가 필요 없고, 숫자형 데이터, 범주형 데이터 모두 처리 가능. 결측값에 큰 영향 받지 않음. But, 과적합 현상에 빠지기 쉬우며, 최적의 의사 결정 트리를 학습하는 문제는 NP-완전이다.
* 랜덤 포레스트: 원본 데이터셋을 무작위로 샘플링한 데이터를 사용해 의사 결정 트리 여러 개를 한꺼번에 학습하는 알고리즘(앙상블 학습) 데이터를 무작위로 샘플링하고 모델들의 결과를 평균해 사용하는 기법을 배깅이라고 함

### 8.3.1 의사 결정 트리
특징 변수가 전체 훈련 데이터셋을 얼마나 잘 분류하는지 *불순도(impurity)* 와 *정보 이득(information gain)* 을 사용해 분류. 최적의 특징 변수(가장 정보 이득이 큰 특징 변수)를 이용해 트리 노드를 하나 생성하고, 계속해서 분기를 만들어 가는 방식. 스파크는 이진 의사 결정 트리만 생성 가능.

분기에 할당된 데이터셋이 단일 클래스로 구성되어 있거나 분기의 깊이가 일정 이상일 때, 해당 분기 노드를 리프 노드로 만든다.

#### 8.3.1.1 알고리즘의 동작 예제
예를 들어 이전에 사용한 소득 예상 데이터셋은 나이가 46세보다 많으면 소득이 5만불 이상일 거라고 예측, 46세보다 적으면 성별이라는 특징 변수를 이용해 남성이면 5만불 이상일 거라고 예측, 여성이라면 교육 수준이라는 특징 변수를 선택... 과 같이 계속해서 분기를 따라 결과값을 예측하게 된다.

#### 8.3.1.2 불순도와 정보 이득
불순도 지표에는 엔트로피(entropy)와 지니 불순도(Gini impurity)가 있는데 스파크를 포함한 대부분의 머신 러닝 라이브러리는 지니 불순도를 기본 지표로 사용한다.

* 엔트로피: 메시지에 포함된 정보량을 측정하는 척도로, 만약 클래스 하나로만 구성된 데이터셋이 있다면 엔트로피는 0이며 모든 클래스가 균일하게 분포된 데이터셋일수록 엔트로피가 높다. 이진 분류 문제에서 엔트로피의 최댓값은 1이다.
* 지니 불순도: 레이블 분포에 따라 데이터셋에서 무작위로 고른 요소의 레이블을 얼마나 자주 잘못 예측하는지 계산한 척도. 이 또한 모든 클래스가 균일하게 분포되어 있을 때 가장 큰 값(이진 분류의 경우 0.5)이며, 하나의 클래스로만 구성된 데이터셋에서는 0이 된다.

정보 이득은 특징 변수 F로 데이터셋 D를 나누었을 때 예상되는 불순도 감소량이다. 알고리즘은 이 정보 이득 값을 바탕으로 데이터셋을 분할한다.

#### 8.3.1.3 의사 결정 트리 학습
손글씨 숫자 데이터셋으로 의사 결정 트리 모델 훈련하는 예제 실습. 다만, 의사 결정 트리 알고리즘이 데이터셋에 있는 모든 클래스 개수를 알 수 있도록 칼럼에 메타 데이터를 추가해야 한다.

```scala
import org.apache.spark.ml.features.StringIndexer
import org.apache.spark.ml.features.StringIndexerModel

val dtsi = new StringIndexer().setInputCol("label").setOutputCol("label-i")
val dtsm: StringIndexerModel = dtsi.fit(penlpoints)
val pendtlpoints = dtsm.transform(penlpoints).drop("label").withColumnRenamed("label-i", "label")

val pendtsets = pendtlpoints.randomSplit(Array(0.8, 0.2))
val pendttrain = pendtsets(0).cache()
val pendtvalid = pendtsets(1).cache()

import org.apache.spark.ml.classification.DecisionTreeClassifier
val dt = new DecisionTreeClassifier()
dt.setMaxDepth(20)
val dtmodel = dt.fit(pendttrain)
```

`DecisionTreeClassifier` 클래스의 매개변수
* `maxDepth`: 트리의 최대 깊이(기본: 5)
* `maxBins`: 연속적인 값을 가지는 특징 변수를 분할할 때 생성할 bin의 최대 개수(기본: 32)
* `minInstancesPerNode`: 데이터셋을 분할할 때 각 분기에 반드시 할당해야 할 데이터 샘플의 최소 개수(기본: 1)
* `minInfoGain`: 데이터셋을 분할할 때 고려할 정보 이득의 최저 값. 만약 분기의 정보 이득이 최저 값보다 낮으면 해당 분기는 버림(기본: 0)

#### 8.3.1.4 의사 결정 트리 살펴보기
의사 결정 트리의 장점 중 하나는 *트리 구조를 시각화해서 알고리즘의 동작을 직관적으로 이해할 수 있다는 점!*

```scala
dtmodel.rootNode // 트리의 루트 노드 (python은 접근 불가)

import org.apache.spark.ml.tree.{InternalNode, ContinuousSplit}
// 어떤 특징 변수로 루트 노드를 분할했는지 알 수 있음
dtmodel.rootNode.asInstanceOf[InternalNode].split.featureIndex 
// 예를 들어 위 결과가 15라고 나왔을 때 15번 특징 변수 값을 분할하는 데 사용한 임계치 확인
// 범주형 변수일 경우 ContinuoutSplit이 아니라 CategoricalSplit(leftCategories, rightCategories 필드 가짐) 사용
dtmodel.rootNode.asInstanceOf[InternalNode].split.asInstanceOf[ContinuousSplit].threshold
// 왼쪽 자식과 오른쪽 자식
dtmodel.rootNode.asInstanceOf[InternalNode].leftChild
dtmodel.rootNode.asInstanceOf[InternalNode].rightChild
```

#### 8.3.1.5 모델 평가
```scala
val dtpredicts = dtmodel.transform(pendtvalid)
val dtresrdd = dtpredicts.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))

import org.apache.spark.mllib.evaluation.MulticlassMetrics
val dtmm = new MulticlassMetrics(dtresrdd)
dtmm.precision
dtmm.confusionMatrix
```

이 예제의 경우 의사 결정 트리가 로지스틱 회귀보다 나은 결과를 보임

### 8.3.2 랜덤 포레스트
의사 결정 트리를 여러 개 학습하고, 각 트리의 예측 결과를 모아서 가장 좋은 결과를 선택하는 앙상블 기법

또한, 의사 결정 트리의 각 노드별로 특징 변수의 *부분 집합을 무작위 선정*하고, 이 집합 내에서만 다음 분기를 결정하는 특징 변수 배깅 기법을 사용한다. 이를 통해 의사 결정 트리들의 상관성이 높을수록 오차율이 커지는 문제를 해결할 수 있다.

성능이 좋고 과적합이 덜 발생하며 학습과 사용도 쉽지만 시각화하기가 어렵고 따라서 직관적으로 해석하기도 어렵다.

#### 8.3.2.1 스파크의 랜덤 포레스트 사용
`RandomForestClassifier`에 기존 `DecisionTreeClassifier` 매개 변수 외에 추가로 사용할 수 있는 매개변수
* `numTrees`: 학습할 트리 개수(기본: 20)
* `featureSubsetStrategy`: 특징 변수 배깅을 수행할 방식.
  * `all`: 모든 특징 변수 사용
  * `onethird`: 특징 변수의 1/3을 무작위로 골라서 사용
  * `sqrt`: 특징 변수 개수의 제곱근만큼 무작위로 골라서 사용
  * `log2`: 특징 변수 개수에 `log2`를 취한 값만큼 무작위로 골라서 사용
  * `auto`(기본): 분류 문제에는 `sqrt`, 회귀 문제에는 `onethird` 사용

```scala
import org.apache.spark.ml.classification.RandomForestClassifier
val rf = new RandomForestClassifier()
rf.setMaxDepth(20)
val rfmodel = rf.fit(pendttrain)
rfmodel.trees // 학습한 의사 결정 트리들 확인

val rfpredicts = rfmodel.transform(pendtvalid)
val rfresrdd = rfpredicts.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))

import org.apache.spark.mllib.evaluation.MulticlassMetrics
val rfmm = new MulticlassMetrics(rfresrdd)
rfmm.precision
rfmm.confusionMatrix
```

이 예제에서 랜덤 포레스트 모델의 오차율은 고작 1%이다. 이 예제 뿐 아니라 랜덤 포레스트는 성능이 탁월하고 사용성이 좋아 가장 널리 사용되는 머신 러닝 알고리즘 중 하나이며, 고차원 데이터셋에서도 좋은 성능을 보인다.

## 8.4 군집화
*특정 유사도 지표에 따라 데이터셋 예제들을 다수의 그룹으로 묶는 것이 목표이며 비지도 학습.* 모든 데이터에 레이블을 부여하기 힘들거나 군집을 사전에 알 수 없는 경우 등 다양한 경우에 사용해서 군집을 찾아내고, 데이터를 더욱 잘 이해할 수 있다.

군집화의 활용 예
* 데이터를 여러 그룹으로 분할
* 이미지 세분화(image segmentation)
* 이상 탐지(anomaly detection)
* 텍스트 분류(text categorization) 또는 주제 인식(topic recognition)
* 검색 결과 그루핑

스파크에 구현된 군집화 알고리즘
* k-평균 군집화: 간단하고 널리 사용됨. 군집이 구 형태가 아니거나 크기(밀도 또는 반경)가 동일하지 않은 경우 잘 작동하지 않음. 각 데이터 예제가 속한 군집을 모델링(하드 군집화)
* 가우스 혼합 모델: 각 군집이 가우스 분포라고 가정하고, 이 분포들의 혼합으로 군집 모형을 만듦. 각 데이터 예제가 각 군집에 속할 확률을 모델링(소프트 군집화)
* 거듭제곱 반복 군집화: 스펙트럼 군집화의 일종으로 스파크에서는 GraphX 라이브러리를 기반으로 구현되어 있음

### 8.4.1 k-평균 군집화
1. 데이터 포인트 k개를 *무작위*로 선택해 *각 군집의 최초 중심점*으로 설정
2. 각 중심점과 *모든 포인트 간 거리를 계산*하고, 각 포인트를 *가장 가까운 군집에 포함*
3. 각 군집의 평균점을 계산해 군집의 새로운 중심점으로 사용
4. 2번과 3번 단계를 반복하다가 새로운 중심점이 이전 중심점과 크게 다르지 않으면 종료

이렇게 알고리즘 학습을 완료하면, 각 군집의 중심점을 알 수 있으므로 새로운 데이터가 들어와도 가장 가까운 중심점의 군집에 포함시킨다.

#### 8.4.1.1 스파크의 k-평균 군집화 사용
손글씨 데이터를 각 군집으로 묶어보자. 손글씨 데이터는 이미 표준화되어 있으므로 군집화에 바로 사용할 수 있다. 또한 군집화에서는 데이터셋을 훈련/검증 데이터셋으로 나눌 필요가 없다.

`KMeans` 매개변수
* `k`: 군집 개수(기본: 2)
* `maxIter`: 최대 반복 횟수(반드시 지정)
* `predictionCol`: 예측 결과 칼럼 이름(기본: prediction)
* `featuresCol`: 특징 변수 칼럼 이름(기본: features)
* `tol`: 수렴 허용치 (군집의 중심점이 움직인 거리가 이 값보다 작으면 반복 종료)
* `seed`: 군집 초기화에 사용할 난수 seed

```scala
import org.apache.spark.ml.clustering.KMeans
val kmeans = new KMeans()
kmeans.setK(10)
kmeans.setMaxIter(500)

val kmmodel = kmeans.fit(penlpoints)
```

#### 8.4.1.2 모델 평가
군집화는 레이블이 없고, 군집 자체를 좋거나 나쁜 군집으로 분리할 수 없기 때문에 모델 평가가 매우 어렵다. 하지만 아래 몇 가지 평가 지표가 있다.
* 군집 비용(군집 왜곡): k-평균 군집화 모델을 평가하는 기본 지표. 각 군집의 중심점과 해당 군집에 포함된 각 데이터 포인트 간 거리를 제곱해 합산한 값. 데이터셋마다 다르므로, 동일 데이터셋으로 학습한 여러 k-평균 군집화 모델을 비교할 때 사용.
* 군집 중심점과 평균 거리: 군집 비용을 해당 데이터셋의 예제 개수로 나눈 값의 제곱근 
* 분할표: 각 군집에 가장 많이 포함된 레이블을 해당 군집의 레이블로 가정한 후, 군집 번호를 각 열에 나열하고 실제 레이블을 각 행에 나열해 비교한 표

```scala
kmmodel.computeCost(penlpoints) // 군집 비용 게산
math.sqrt(kmmodel.computeCost(penlpoints)/penlpoints.count()) // 군집 중심점과 평균 거리

val kmpredicts = kmmodel.transform(penlpoints)
printContingency(kmpredicts, 0 to 9) // 실제 레이블과 예측된 레이블이 튜플로 구성된 RDD, 실제 레이블 값의 배열 전달
// 군집 결과의 분할표와 순도 출력
```

#### 8.4.1.4 군집 개수 결정
숫자 데이터는 군집 개수를 정확하게 결정할 수 있었지만 실제로 군집화 작업을 수행할 때는 군집 개수를 몇 개로 설정할지 고민해야 한다. 이럴 때 엘보 기법(elbow method)을 활용할 수 있다.

엘보 기법은 군집 개수를 조금씩 늘려가며 모델을 훈련시키고, 각 모델의 군집 비용을 게산한다. 군집 개수와 군집 비용을 그래프로 그려보고 기울기가 급격하게 변하는 엘보 지점들을 군집 개수의 후보로 선정한다.