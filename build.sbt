name := "timeseries"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"


lazy val root = (project in file(".")).
  settings(
    name := "timeseries",
    version := "1.0",
    scalaVersion := "2.12.8",
    mainClass in Compile := Some("timeseries")
  )


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % "2.4.0",
  "org.scala-lang" % "scala-library" % "2.11.12" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",

  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}