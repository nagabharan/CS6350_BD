import breeze.linalg._
import org.apache.spark.HashPartitioner
//spark-shell -i als.scala to run this code
//SPARK_SUBMIT_OPTS="-XX:MaxPermSize=4g" spark-shell -i als.scala

//Implementation of sec 14.3 Distributed Alternating least squares from stanford Distributed Algorithms and Optimization tutorial. 

//loads ratings from file
val ratings = sc.textFile("/home/nagabharan/Desktop/HW3/dataset/ratings.dat").map(l => (l.split("::")(0),l.split("::")(1),l.split("::")(2))) 

// counts unique movies
val itemCount = ratings.map(x=>x._2).distinct.count 

// counts unique user
val userCount = ratings.map(x=>x._1).distinct.count 

// get distinct movies
val items = ratings.map(x=>x._2).distinct   

// get distinct user
val users = ratings.map(x=>x._1).distinct  

// latent factor
val k = 5   

//create item latent vectors
val itemMatrix = items.map(x=> (x,DenseVector.zeros[Double](k)))   

//Initialize the values to 0.5
// generated a latent vector for each item using movie id as key Array((movie_id,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
var myitemMatrix = itemMatrix.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist  

//create user latent vectors
val userMatrix = users.map(x=> (x,DenseVector.zeros[Double](k)))

//Initialize the values to 0.5
// generate latent vector for each user using user id as key Array((userid,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
var myuserMatrix = userMatrix.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist 

// group rating by items. Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (itemid,(userid,rating)) e.g  (1,(2,3))
val ratingByItem = sc.broadcast(ratings.map(x => (x._2,(x._1,x._3)))) 

// group rating by user.  Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (userid,(item,rating)) e.g  (1,(3,5)) 
val ratingByUser = sc.broadcast(ratings.map(x => (x._1,(x._2,x._3)))) 

var i = 0
for( i <- 1 to 10){

  val ratItemVec = myitemMatrix.join(ratingByItem.value)

  // regularization factor which is lambda.
  val regfactor = 1.0 
  
  //generate an diagonal matrix with dimension k by k
  val regMatrix = DenseMatrix.zeros[Double](k,k)  

  //filling in the diagonal values for the reqularization matrix.  val regMatrix = DenseMatrix.zeros[Double](k,k)  
  regMatrix(0,::) := DenseVector(regfactor,0,0,0,0).t 
  regMatrix(1,::) := DenseVector(0,regfactor,0,0,0).t 
  regMatrix(2,::) := DenseVector(0,0,regfactor,0,0).t 
  regMatrix(3,::) := DenseVector(0,0,0,regfactor,0).t 
  regMatrix(4,::) := DenseVector(0,0,0,0,regfactor).t

//===========================================Homework 4. Implement code to calculate equation 2 and 3 .===================================================
//=================You will be required to write code to update the myuserMatrix which contains the latent vectors for each user and myitemMatrix which is the matrix that contains the latent vector for the items
//Please Fill in your code here.

  //Equation 2 - Update Latent Vector for each User
  val userbyItemMat = ratItemVec.map(x => (x._2._2._1,x._2._1*x._2._1.t )).reduceByKey(_+_).map(x=> (x._1,breeze.linalg.pinv(x._2 + regMatrix))) 
  
  val sumruiyi = ratItemVec.map(x => (x._2._2._1,x._2._1 * x._2._2._2.toDouble )).reduceByKey(_+_) 
  
  val joinres = userbyItemMat.join(sumruiyi) 
  
  myuserMatrix = joinres.map(x=> (x._1,x._2._1 * x._2._2)).partitionBy(new HashPartitioner(10)) 

  //Equation 3 - Update Latent Vector for each item
  val ratUserVec = myuserMatrix.join(ratingByUser.value)
  
  val itembyUserMat = ratUserVec.map(x => (x._2._2._1,x._2._1*x._2._1.t )).reduceByKey(_+_).map(x=> (x._1,breeze.linalg.pinv(x._2 + regMatrix))) 
  
  val sumruixu = ratUserVec.map(x => (x._2._2._1,x._2._1 * x._2._2._2.toDouble )).reduceByKey(_+_) 
  
  val joinres1 = itembyUserMat.join(sumruixu) 
  
  myitemMatrix = joinres1.map(x=> (x._1,x._2._1 * x._2._2)).partitionBy(new HashPartitioner(10)) 

//==========================================End of update latent factors=================================================================  
}
//======================================================Implement code to recalculate the ratings a user will give an item.====================

def predictRating(userId:String, movieId:String): Unit ={
  val user = myuserMatrix.filter(x => x._1.equals(userId))
  val item = myitemMatrix.filter(x => x._1.equals(movieId))
  val userLatent = new DenseVector(user.values.toArray)
  val itemLatent = new DenseVector(item.values.toArray)
  val finalrating = userLatent dot itemLatent
  
  println("\nUser Latent "+user+": "+userLatent)
  println("\nItem Latent "+item+": "+itemLatent)
  println("\nFinal Rating: "+finalrating)
}

//Hint: This requires multiplying the latent vector of the user with the latent vector of the  item. Please take the input from the command line. and
// Provide the predicted rating for user 1 and item 914, user 1757 and item 1777, user 1759 and item 231.

val user1 = myuserMatrix.filter(x=>(x._1.equals("1")))
val item1 = myitemMatrix.filter(x=>(x._1.equals("914")))
val userLatent1 = new DenseVector(user1.values.toArray)
val itemLatent1 = new DenseVector(item1.values.toArray)
val finalrating1 = userLatent1 dot itemLatent1

val user2 = myuserMatrix.filter(x=>(x._1.equals("1757")))
val item2 = myitemMatrix.filter(x=>(x._1.equals("1777")))
val userLatent2 = new DenseVector(user2.values.toArray)
val itemLatent2 = new DenseVector(item2.values.toArray)
val finalrating2 = userLatent2 dot itemLatent2

val user3 = myuserMatrix.filter(x=>(x._1.equals("1759")))
val item3 = myitemMatrix.filter(x=>(x._1.equals("231")))
val userLatent3 = new DenseVector(user3.values.toArray)
val itemLatent3 = new DenseVector(item3.values.toArray)
val finalrating3 = userLatent3 dot itemLatent3

println("\nUser Latent 1:"+userLatent1)
println("\nItem Latent 914:"+itemLatent1)
println("\nFinal Rating:"+finalrating1)

println("\nUser Latent 1757:"+userLatent2)
println("\nItem Latent 1777:"+itemLatent2)
println("\nFinal Rating:"+finalrating2)

println("\nUser Latent 1759:"+userLatent3)
println("\nItem Latent 231:"+itemLatent3)
println("\nFinal Rating:"+finalrating3)