import Dependencies._

lazy val shared = List(
    organization := "eu.ideata",
    scalaVersion := "2.10.5",
    version      := "0.1.0-SNAPSHOT",
    name := "streaming-playground",
    libraryDependencies += scalaTest % Test,
    resolvers ++= Seq(
      "confluent" at "http://packages.confluent.io/maven/",
      Resolver.sonatypeRepo("public")
    )
  )

javacOptions ++= Seq("-source", "1.7")
scalacOptions += "-target:jvm-1.7"

lazy val core = (project in file ("core"))
  .settings(shared: _*)
  .settings(
    name := "core",
    libraryDependencies ++= avro
  )

lazy val spark16 = (project in file("spark_1_6"))
  .dependsOn(core)
  .settings(shared: _*)
  .settings(
    name := "spark16",
    libraryDependencies ++= spark16Deps ++ kafkaAvroSerde ++ kafkaClient ++ scopt
  )


lazy val root = (project in file(".")).aggregate(core, spark16)
  .settings(shared: _*)
  .settings(
    name := "streaming-playground"
)




