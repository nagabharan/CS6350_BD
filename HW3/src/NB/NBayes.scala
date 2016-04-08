import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object NBayes {

  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("BD HW 3 NaiveBayes").setMaster("local")
    val sc = new SparkContext(conf)
    
    // Load and Parse data
    val data = sc.textFile("/home/nagabharan/Desktop/HW3/dataset/glass.data")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(10).toDouble, Vectors.dense(parts(0).toDouble,parts(1).toDouble,parts(2).toDouble,parts(3).toDouble,
        parts(4).toDouble,parts(5).toDouble,parts(6).toDouble,parts(7).toDouble,parts(8).toDouble,parts(9).toDouble))
    }

    // Split data into Train and Test 60/40
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    // Train Naive Bayes model
    val model = NaiveBayes.train(training, lambda = 1.0)

    // Evaluate model on test instances and compute test error
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))

    // Compute Accuracy
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("Accuracy of the Naive Bayes classifier is " + accuracy)

    // Save and load model
    model.save(sc, "/home/nagabharan/Desktop/HW3/output/NB/")
    val sameModel = NaiveBayesModel.load(sc, "/home/nagabharan/Desktop/HW3/output/NB/")
  }
}
