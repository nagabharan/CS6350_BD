import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object TopAverageAge {

  val INPUT_DIR = "/home/nagabharan/Desktop/HW2/dataset/soc.txt"
  val OUTPUT_DIR = "/home/nagabharan/Desktop/HW2/output/q4"
  val INPUT_INFO_DIR = "/home/nagabharan/Desktop/HW2/dataset/userdata.txt"

  val CURRENT_YEAR = "2016"
  val CURRENT_MONTH = "3"

  // Join two tables by key of each friend in list
  def JoinFriendMapper(value: String): Array[(String, String)] = {
    var tempList = new ArrayBuffer[(String, String)]()
    val dataline = value.split("\\t")
    if (dataline.length < 2) {
      return tempList.toArray
    }

    val CurrentUID = dataline(0)
    val Friend = dataline(1).split(",")
    Friend.foreach(friend =>
      {
        tempList += ((friend, CurrentUID))
      })

    tempList.toArray
  }

  def JoinAgeMapper(value: String): (String, Double) = {

    val dataline = value.split(",")

    val CurrentUID = dataline(0)
    val DateOfBirth = dataline(9)
    val Age = Integer.valueOf(CURRENT_YEAR) - Integer.valueOf(DateOfBirth.split("/")(2))
    +(Integer.valueOf(CURRENT_MONTH) - Integer.valueOf(DateOfBirth.split("/")(0))) * 1.0 / 12

    (CurrentUID, Age)
  }

  def JoinFriendAgeReducer(args: (String, (Iterable[String], Iterable[Double]))): Array[(String, Double)] = {
    var tempList = new ArrayBuffer[(String, Double)]()

    val Age = args._2._2.head

    val FriendItr = args._2._1
    FriendItr.foreach(value =>
      {
        tempList += ((value, Age))
      })
    tempList.toArray
  }

  // Calculate average age of friends of each user
  def CalculateAveAgeReducer(args: (String, Iterable[Double])): (Double, String) = {
    var AgeSum = 0.0
    var Count = 0
    val values = args._2
    values.foreach(Content => //todo no friend
      {
        AgeSum += Content
        Count += 1
      })

    if (Count == 0) {
      return (0, args._1)
    }
    val AveAge = AgeSum * 1.0 / Count
    (AveAge, args._1)
  }

  // Join Address
  def JoinAddressMapper(value: String, targetMap: Map[String, Double]): String = {

    val dataline = value.split(",")

    val CurrentUID = dataline(0)
    val Address = dataline(3) + ", " + dataline(4) + ", " + dataline(5)

    val AveAgeOfFriend = "%.2f".format(targetMap.get(CurrentUID).get)

    CurrentUID + ", " + Address + ", " + AveAgeOfFriend
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Scala HW2").setMaster("local")
    
    val sc = new SparkContext(conf)

    val AdjData = sc.textFile(INPUT_DIR)
    val InfoData = sc.textFile(INPUT_INFO_DIR)

    val FriendMapperRes = AdjData.flatMap(JoinFriendMapper)
    val AgeMapperRes = InfoData.map(JoinAgeMapper)
    val JoinFriendAgeRes = FriendMapperRes.cogroup(AgeMapperRes).flatMap(JoinFriendAgeReducer)

    val CalculateAveRes = JoinFriendAgeRes.groupByKey().map(CalculateAveAgeReducer).sortByKey(false).map(tuple => (tuple._2, tuple._1))
    
    val Top20AveRes = CalculateAveRes.take(20)
    var TargetMap = Map[String, Double]()
    Top20AveRes.foreach(tuple => TargetMap += (tuple._1 -> tuple._2))

    val targetBc = sc.broadcast(TargetMap)
    
    val AddressMapperRes = InfoData.filter(line => targetBc.value.contains(line.split(",")(0))).
      map(line => JoinAddressMapper(line, targetBc.value)).saveAsTextFile(OUTPUT_DIR)
  }
}