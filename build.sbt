name := "nyc-taxi-tipping-analysis"

version := "1.0"

scalaVersion := "2.12.21"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.2.1" % "provided"
)