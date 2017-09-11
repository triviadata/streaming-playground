import Dependencies._

lazy val shared = List(
    organization := "eu.ideata",
    scalaVersion := "2.11.7",
    version      := "0.1.0-SNAPSHOT",
    name := "streaming-playground",
    libraryDependencies += scalaTest % Test,
    resolvers ++= Seq(
      "confluent" at "http://packages.confluent.io/maven/",
      Resolver.sonatypeRepo("public")
    )
  )

lazy val core = (project in file ("core"))
    .settings(shared: _*)
    .settings(
      name := "core",
      libraryDependencies ++= avro
    )

lazy val generator = (project in file("generator"))
  .dependsOn(core)
  .settings(shared: _*)
  .settings(
    libraryDependencies ++= akka ++ kafkaAvroSerde ++ kafkaClient ++ scopt ++ avro,
    name:= "generator",
    mainClass in (Compile, run) := Some("eu.ideata.streaming.main.Main")
  )

lazy val spark16 = (project in file("spark_1_6"))
  .dependsOn(core)
  .settings(shared: _*)
  .settings(
    name := "spark16",
    libraryDependencies ++= spark16Deps ++ kafkaAvroSerde ++ avro4s ++ kafkaClient ++ scopt,
    mainClass in (Compile, run) := Some("eu.ideata.streaming.spark16.EnrichStreams")
  )

lazy val flink = (project in file("flink"))
  .dependsOn(core)
  .settings(shared: _*)
  .settings(
    name := "flink",
    libraryDependencies ++= flinkDeps ++ kafkaAvroSerde ++ scopt ++ avro
  )

lazy val root = (project in file(".")).aggregate(generator, core, flink)
  .settings(shared: _*)
  .settings(
    name := "root"
)




