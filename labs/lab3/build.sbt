name := "Triest"

version := "0.1"

scalaVersion := "2.11.12"
scalaVersion in ThisBuild := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % "1.9.1",
  "org.apache.flink" %% "flink-clients" % "1.9.1",
  "org.apache.flink" %% "flink-streaming-scala" % "1.9.1"
)
