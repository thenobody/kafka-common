name := "kafka-common"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "0.8.2.2",
  "io.spray" %%  "spray-json" % "1.3.2",
  "redis.clients" % "jedis" % "2.7.3",
  "org.slf4j" % "slf4j-log4j12" % "1.7.12"
)
    