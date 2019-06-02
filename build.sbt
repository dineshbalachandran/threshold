ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "threshold"

version := "0.1-SNAPSHOT"

organization := "com.dineshkb"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )
val flinkVersion = "1.8.0"
val liftVersion = "3.3.0"
val scalatestVersion = "3.0.5"
val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-runtime" % flinkVersion % "test" classifier "tests",
  "org.apache.flink" %% "flink-test-utils" % flinkVersion % "test")
val lift_json = "net.liftweb" %% "lift-json" % liftVersion

libraryDependencies += lift_json
libraryDependencies += scalatest
val scalatest = "org.scalatest" %% "scalatest" % scalatestVersion % "test"

assembly / mainClass := Some("ThresholdBreachIdentification")

// make run command include the provided dependencies
Compile / run := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
