name := "888-solution"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
