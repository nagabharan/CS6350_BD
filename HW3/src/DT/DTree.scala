import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini

object DTree {

  def main(args: Array[String]): Unit = {

  	val conf = new SparkConf().setAppName("BD HW3 Decision Tree").setMaster("local")
    val sc = new SparkContext(conf)
	
	// Load and Parse the data
	val data = sc.textFile("/home/nagabharan/Desktop/BDHW3/DT/dataset/glass.data")
	val parsedData = data.map { line =>
		val parts = line.split(',').map(_.toDouble)
		LabeledPoint(parts(10).toDouble, Vectors.dense(parts(0).toDouble,parts(1).toDouble,parts(2).toDouble,parts(3).toDouble,parts(4).toDouble,parts(5).toDouble,parts(6).toDouble,parts(7).toDouble,parts(8).toDouble,parts(9).toDouble))
	}

	// Split Train and Test as 60/40
	val splits = parsedData.randomSplit(Array(0.6, 0.4))
	val (trainingData, testData) = (splits(0), splits(1))

	// Train DT model
	val numClasses = 8
	val categoricalFeaturesInfo = Map[Int, Int]()
	val impurity = "gini"
	val maxDepth = 5
	val maxBins = 32

	val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
	  impurity, maxDepth, maxBins)

	// Evaluate model on test instances and compute test error
	val predictionAndLabel = testData.map { point =>
	  val prediction = model.predict(point.features)
	  (point.label, prediction)
	}

	// Compute Accuracy
	val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testData.count()
	println("Accuracy of the Decision Tree is " + accuracy)

	// Save and load model
	model.save(sc, "/home/nagabharan/Desktop/HW3/output/DT/")
	val sameModel = DecisionTreeModel.load(sc, "/home/nagabharan/Desktop/HW3/output/DT/")
	}
}