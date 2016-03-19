/*
----------------------------------------------------------------------
			Parsing the 3 datasets
----------------------------------------------------------------------
*/

val rawCouponListTrain = sc.textFile("/Users/kevin/Desktop/Coupon_Purchase_Prediction/coupon_list_train.csv")

//val categories:List[String] = List("Food", "Spa", "Hair salon", "Nail and eye salon", "Beauty", "Relaxation", "Delivery service", "Gift card", "Other coupon", "Leisure", "Hotel and Japanese hotel", "Lesson")
case class MatchDataCoupon(couponID: Double, categName: Double, discountRate: Double, listPrice: Double, discountPrice: Double, dispPeriod: Double, validPeriod: Double, usableDate: Array[Double])
//case class Coupon_ID_Index(realCouponID: String, couponID: Double)

def parseCategory(category: String): Double = {
			category match {
					case "Delivery service" => return 1
					case "Food" => return 2
					case "Hotel and Japanese hotel" => return 3
					case "Hair salon" => return 4
					case "Relaxation" => return 5
					case "Other coupon" => return 6
					case "Spa" => return 7
					case "Lesson" => return 8
					case "Leisure" => return 9
					case "Nail and eye salon" => return 10
					case "Gift card" => return 11
					case "Health and medical" => return 12
					case "Beauty" => return 13
					case _     =>  return -1
				}
		}

def isHeader_1(line: String) = line.contains("CAPSULE_TEXT")
val noheader1 = rawCouponListTrain.filter(x => !isHeader_1(x)).zipWithIndex

val data1 = noheader1.map { line =>
	val values = line._1.split(',')
	//var categName : Array[Double] = new Array[Double](categories.size)
	val couponID = line._2.toDouble
	val discountRate = if (values(2) != "") values(2).toDouble else -1.0
	val listPrice = if (values(3) != "") values(3).toDouble else -1.0
	val discountPrice = if (values(4) != "") values(4).toDouble else -1.0
	val dispPeriod = if (values(7) != "") values(7).toDouble else -1.0
	val validPeriod = if (values(10) != "") values(10).toDouble else -1.0
	val usableDate = values.slice(11,20).map(s => if (s == "") -1.0 else s.toDouble)
	val categName = parseCategory(values(1))
	//var i = -1
	//categories.foreach{y => i+=1; categName(i) = if(y == values(1)) 1.0 else 0.0}
	MatchDataCoupon(couponID, categName, discountRate, listPrice, discountPrice, dispPeriod, validPeriod, usableDate)
}

data1.cache()

val referenceTable1 = noheader1.map{ line =>
	val values = line._1.split(',')
	val couponID = line._2.toDouble
	val realCouponID = values(23)
	(realCouponID, couponID)
	//Coupon_ID_Index(realCouponID, couponID)
}

referenceTable1.cache()

val rawUserData = sc.textFile("/Users/kevin/Desktop/Coupon_Purchase_Prediction/user_list.csv")

def isHeader_2(line: String) = line.contains("REG_DATE")

val noheader2 = rawUserData.filter(x => !isHeader_2(x)).zipWithIndex
case class MatchDataUser(userID: Double, age: Double, sex: Double)
//case class User_ID_Index(realUserID: String, userID: Double)

def parse_1(line: (String, Long)) = {
	val pieces = line._1.split(',')
	val sex = if(pieces(1) == "f") 0.0 else 1.0
	val age = pieces(2).toDouble
	val userID = line._2.toDouble
	MatchDataUser(userID, age, sex)
}

def parse_1_1(line: (String, Long)) = {
	val pieces = line._1.split(',')
	val userID = line._2.toDouble
	val realUserID = pieces(5)
	//User_ID_Index(realUserID, userID)
	(realUserID, userID)
}

val data2 = noheader2.map(line => parse_1(line))
val referenceTable2 = noheader2.map(line => parse_1_1(line))

data2.cache()
referenceTable2.cache()


val rawCouponDetailTrain = sc.textFile("/Users/kevin/Desktop/Coupon_Purchase_Prediction/coupon_detail_train.csv")
def isHeader_3(line: String) = line.contains("ITEM_COUNT")
val noheader3 = rawCouponDetailTrain.filter(x => !isHeader_3(x))
case class MatchDataUserCoupon(userID: String, couponID: String, purchaseCount: Double)

def parse_2(line: String) = {
	val bits = line.split(',')
	val userID = bits(4)
	val couponID = bits(5)
	val purchaseCount = bits(0).toDouble
	MatchDataUserCoupon(userID, couponID, purchaseCount)
}

val data3 = noheader3.map(line => parse_2(line))
data3.cache()

/*
----------------------------------------------------------------------
			Joining the 3 datasets together
----------------------------------------------------------------------
*/

val rdd2 = data2.map {
	x => (x.userID, (x.age, x.sex))
}	
// rdd2 = RDD[(userID, (age, sex))]
//		  RDD[(Double, (Double, Double))]

val INT_rdd3 = data3.map {
	x => (x.userID, (x.couponID, x.purchaseCount))
}
// INT_rdd3 = RDD[(userID, (couponID, purchaseCount))]
//			  RDD[(String, (String, Double))]

val rdd3 = INT_rdd3.join(referenceTable2).map {
	x => (x._2._1._1, (x._2._1._2, x._1, x._2._2))}.join(referenceTable1).map {
	y => (y._2._1._3, (y._2._2, y._2._1._1))
}
// rdd3 = RDD[(userID, couponID, purchaseCount)] 
//		  RDD[(Double, Double, Double)]

val joinedrdd23 = rdd3.join(rdd2).map {
    x => (x._2._1._1, (x._1, x._2._1._2, x._2._2._1, x._2._2._2))
}
// joinedrdd23 = RDD[(couponID, (userID, purchaseCount, age, sex))]
//				 RDD[(Double, (Double, Double, Double, Double))]

val rdd1 = data1.map {
  x => (x.couponID, (x.categName, x.discountRate, x.listPrice, x.discountPrice, x.dispPeriod, x.validPeriod, x.usableDate))
}

// rdd1 = RDD[(couponID, (categName, discountRate, listPrice, discountPrice, dispPeriod, validPeriod, usableDate))]
//		  RDD[(Double, (Double, Double, Double, Double, Double, Double, Array[Double]))]

val joinedrdd = joinedrdd23.join(rdd1).map {
	x => (x._2._1._1, x._2._1._3, x._2._1._4, x._1, x._2._1._2, x._2._2._1, x._2._2._2, x._2._2._3, x._2._2._4, x._2._2._5, x._2._2._6, x._2._2._7)
}
// joinedrdd = RDD[(userID, age, sex, couponID, purchaseCount, categName, discountRate, listPrice, discountPrice, dispPeriod, validPeriod, usableDate)]
// 			   RDD[(Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Array[Double])]

val sortedRDD = joinedrdd.sortBy(_._1)

val resortedRDD = sortedRDD.map {
	x => (x._1, (x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, -1.0))
}.groupByKey().map{case (x, iter) => (x, iter.toArray)}

def couponAdder(line: (Double, Array[(Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Array[Double], Double)])): (Double, Array[(Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Array[Double], Double)]) = {
	val size = line._2.size
	for(i <- 0 until (size - 1)){
		if((i+1) < size){
			line._2(i) = (line._2(i)._1, line._2(i)._2, line._2(i)._3, line._2(i)._4, line._2(i)._5, line._2(i)._6, line._2(i)._7, line._2(i)._8, line._2(i)._9, line._2(i)._10, line._2(i)._11, line._2(i+1)._3)
		}
	}
	line
}

val trainingData = resortedRDD.map(line => couponAdder(line)).flatMapValues(x => x).map(x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4, x._2._5,
 				x._2._6, x._2._8, x._2._9, x._2._10, x._2._12))
// RDD[(userID, age, sex, couponID, purchaseCount, categName, discountRate, discountPrice, dispPeriod, validPeriod, nextCouponID)]
// RDD[(Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)]

trainingData.cache()

/*
----------------------------------------------------------------------
		Creating FeatureVector and LabeledPoint
----------------------------------------------------------------------
*/

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._

val data = trainingData.map { line =>
	val featureVector = Vectors.dense(Array(line._1, line._2, line._3, line._4, line._5, line._6, line._7, line._8, line._9, line._10))
	val label = line._11
	LabeledPoint(label, featureVector)
}

/*
----------------------------------------------------------------------
			Classification
----------------------------------------------------------------------
*/

import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Gini

// Split the data into training and test sets (30% held out for cross-validation)

val splits = data.randomSplit(Array(0.7, 0.3))
val (trainData, testData) = (splits(0), splits(1))
trainData.cache()

// Train a DecisionTree Model

val numClasses = 19413
val categoricalFeaturesInfo = Map[Int, Int](5 -> 14)
val impurity = Gini.instance
val maxDepth = 5
val maxBins = 23000
val maxMemoryInMB = 10000
val quantileCalculationStrategy: QuantileStrategy = org.apache.spark.mllib.tree.configuration.QuantileStrategy.Sort

val strategy = new Strategy(Algo.Classification, impurity, maxDepth, numClasses, maxBins, quantileCalculationStrategy, categoricalFeaturesInfo, 1, 0.0, maxMemoryInMB, 1, false, 10);

val model = DecisionTree.train(trainData, strategy)
