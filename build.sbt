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
    libraryDependencies ++= akka ++ kafkaAvroSerde ++ kafkaClient ++ scopt ++ avro ++ circle,
    name:= "generator",
    mainClass in (Compile, run) := Some("eu.ideata.streaming.main.Main")
  )

lazy val flink = (project in file("flink"))
  .dependsOn(core)
  .settings(shared: _*)
  .settings(
    name := "flink",
    libraryDependencies ++= flinkDeps ++ kafkaAvroSerde ++ scopt ++ avro
  )

lazy val kafkaStreams = (project in file("kafka_streams"))
  .dependsOn(core)
  .settings(shared: _*)
  .settings(
    name := "kafkaStreams",
    libraryDependencies ++= kafkaStreamsDeps ++ scopt
  )

lazy val root = (project in file(".")).aggregate(generator, core, flink, kafkaStreams)
  .settings(shared: _*)
  .settings(
    name := "streaming-playground"
)




