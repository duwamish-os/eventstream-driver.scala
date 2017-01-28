organization := "nihilos"

name := "streaming-driver"

version := "1.0"

scalaVersion := "2.11.8"

parallelExecution in Test := false

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" %% "akka-http-experimental" % "1.0",
    "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "1.0",
    "org.apache.kafka" % "kafka_2.11" % "0.10.1.1",
    "org.apache.kafka" % "kafka-clients" % "0.10.1.1",
    "com.typesafe" % "config" % "1.3.1",
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.5",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.6",
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.4",
    "org.json" % "json" % "20160810",

    "com.typesafe.akka" %%"akka-http-testkit-experimental" % "1.0",
    "org.scalatest" %% "scalatest" % "3.0.0",
    "org.scalatest" %% "scalatest" % "3.0.0" % "test",
    "org.mockito" % "mockito-all" % "1.10.19" % "test",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.3.0" % "test",
    "net.manub" %% "scalatest-embedded-kafka" % "0.11.0" % "test"
  )
}
