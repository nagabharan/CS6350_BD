import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MutualFriendZip {
  val INPUT_DIR = "/home/nagabharan/Desktop/HW2/dataset/soc.txt"
  val OUTPUT_DIR = "/home/nagabharan/Desktop/HW2/output/q3"
  val INPUT_INFO_DIR = "/home/nagabharan/Desktop/HW2/dataset/userdata.txt"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Scala HW2").setMaster("local")
    val sc = new SparkContext(conf)

    val userA = readLine("Enter UserA : ")
    val userB = readLine("Enter UserB : ")

    val data = sc.textFile(INPUT_DIR)
    val userData = sc.textFile(INPUT_INFO_DIR)
    
    val flist1 = data.map(line => line.split("\\t")).filter(l1 => (l1.size == 2)).filter(line => (userB == line(0))).flatMap(line => line(1).split(","))
    val flist2 = data.map(line => line.split("\\t")).filter(l1 => (l1.size == 2)).filter(line => (userA == line(0))).flatMap(line => line(1).split(","))

    val mutuals = flist2.intersection(flist1).collect()

    val mutualzips = userData.map(line => line.split(",")).filter(line => mutuals.contains(line(0))).map(line => (line(0), line(1))).coalesce(1,true).saveAsTextFile(OUTPUT_DIR)

  }
}