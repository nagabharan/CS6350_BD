name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"

#mainClass in (Compile,run) := Some("RecommendFriend")
#mainClass in (Compile,run) := Some("MutualFriend")
#mainClass in (Compile,run) := Some("MutualFriendZip")
mainClass in (Compile,run) := Some("TopAverageAge")
