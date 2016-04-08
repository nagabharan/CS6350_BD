import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object KMeansMovie {
    // Set number of clusters and iterations
    val k = 10
    val maxIterations = 20
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("kMeans").setMaster("local")
        val sc = new SparkContext(conf)

        // Load data
        val itemUserMatrix = sc.textFile("/home/nagabharan/Desktop/HW3/dataset/itemusermat")
        val moviesData = sc.textFile("/home/nagabharan/Desktop/HW3/dataset/movies.dat")

        // Parse and map item-user matrix
        val data = itemUserMatrix.map(line => Vectors.dense(line.split(" ").drop(1).map(_.toDouble))).cache()

        // Train a kmeans model
        val kMeansModel = KMeans.train(data, k, maxIterations)

        // Map movie ratings
        val movieRatings = itemUserMatrix.map(line => getMovieRatings(line)).map(item => (item._1, Vectors.dense(item._2.map(_.toDouble))))

        // Map and test prediction
        val clusters = movieRatings.map(item => (item._1, kMeansModel.predict(item._2)))
      
        // Parse movies.dat for movie and genre
        val movies = moviesData.map(line => line.split("::")).map(item => (item(0).toLong, item.mkString(",")))

        // Generate the movie rating clusters
        val moviesCluster = movies.join(clusters).map(item => item.swap).map(item => (item._1._2, item._1._1)).reduceByKey(_+"%"+_)
        val output = moviesCluster.map(item => (item._1, item._2.split("%").take(5).mkString("\n\t"))).sortBy(_._1, true).map(item => ("Cluster" + item._1 + "\n\t" + item._2))

        output.saveAsTextFile("/home/nagabharan/Desktop/HW3/output/KMeans/")
    }

    // Generate <movieID, ratings> pairs for mapping and training
    def getMovieRatings(line: String): (Long, Array[String]) = {
        val movieRatings = line.split(" ")
        val movieID = movieRatings(0).toLong
        val ratings = movieRatings.drop(1)

        return (movieID, ratings)
    }

}
