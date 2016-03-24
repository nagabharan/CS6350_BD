import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer

object RecommendFriend {

  val INPUT_DIR = "/home/nagabharan/Desktop/HW2/dataset/soc.txt"
  val OUTPUT_DIR = "/home/nagabharan/Desktop/HW2/output/q1"
  val INPUT_INFO_DIR = "/home/nagabharan/Desktop/HW2/dataset/userdata.txt"

  val IS_FRIEND = -1

  def mapProcess(args: String, checkUIDs: String): Array[(String, (String, Int))] =
    {
      var MutualFList = new ArrayBuffer[(String, (String, Int))]()

      val UIDFriends = args.split("\\t")
      // No friend
      if (UIDFriends.length < 2) {
        return MutualFList.toArray // should not return null
      }

      val CurrentUID = UIDFriends(0)
      val FList = UIDFriends(1).split(",")

      var UIDChecklist = Set[String]()
      checkUIDs.split(",").foreach(uid => UIDChecklist += uid)

      // Check UIDCheck user = Current user
      if (UIDChecklist.contains(CurrentUID)) {
        for (i <- 0 until FList.length) {
          MutualFList += ((CurrentUID, (FList(i), IS_FRIEND)))
        }
      }

      // Check UIDCheck user in the friend list?
      var CurrentcheckUIDlist = Set[String]()
      for (i <- 0 until FList.length) {
        if (UIDChecklist.contains(FList(i))) {
          CurrentcheckUIDlist += FList(i)
        }
      }
      // Flist doesn't have UIDCheck user
      if (CurrentcheckUIDlist.isEmpty) {
        return MutualFList.toArray
      }

      // UIDCheck user is in friend list so emit
      CurrentcheckUIDlist.foreach(TargetUID =>
        {
          for (j <- 0 until FList.length) {
            if (!TargetUID.equals(FList(j))) {
              val item = (TargetUID, (FList(j), 1))
              MutualFList += item
            }
          }
        })

      MutualFList.toArray
    }

  def reduceProcess(args: (String, Iterable[(String, Int)])): String = {
    val Key = args._1
    var tempCandidates = Map[String, Int]()

    args._2.foreach(Candidate =>
      {
        val testCandidate = Candidate._1
        val MutualFs = Candidate._2

        if (tempCandidates.contains(testCandidate)) {
          val i = tempCandidates(testCandidate)
          if (i == -1) {
            // ignore direct friend
          } else if (MutualFs.equals(IS_FRIEND)) {
            // reset direct friend
            tempCandidates += (testCandidate -> -1)
          } else {
            tempCandidates += (testCandidate -> (i + 1))
          }
        } else {
          if (MutualFs.equals(IS_FRIEND)) {
            tempCandidates += (testCandidate -> -1)
          } else {
            tempCandidates += (testCandidate -> 1)
          }
        }
      })

    // Top 10 recommended friends (no need)

    val buffer = new StringBuilder()
    tempCandidates.foreach(entry =>
      {
        // not direct friend
        if (entry._2 != -1) {
          buffer.append(",")
          buffer.append(entry._1)
        }
      })

    if (buffer.nonEmpty) {
      buffer.deleteCharAt(0)
    }

    buffer.insert(0, Key + "\t")
    buffer.toString()
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Scala HW2").setMaster("local")
    
    val sc = new SparkContext(conf)

    val checkUIDs = sc.broadcast("924,8941,8942,9019,9020,9021,9022,9990,9992,9993")

    val data = sc.textFile(INPUT_DIR)

    val mapRes = data.flatMap(line => mapProcess(line, checkUIDs.value))
    val reduceRes = mapRes.groupByKey().map(reduceProcess).saveAsTextFile(OUTPUT_DIR)
  }
}