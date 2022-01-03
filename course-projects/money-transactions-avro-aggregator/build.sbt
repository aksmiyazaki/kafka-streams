import sbt.Compile

ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.github.aksmiyazaki.money.producer"
ThisBuild / organizationName := "github-aksmiyazaki"
ThisBuild / javacOptions     ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
ThisBuild / scalacOptions    ++= Seq("-language:postfixOps")

lazy val root = (project in file("."))
  .settings(
    name := "money-transactions-avro-aggregator",
    resolvers += "Confluent Repo" at "https://packages.confluent.io/maven",
    idePackagePrefix := Some("com.github.aksmiyazaki.moneytransactions.aggregator"),
    libraryDependencies ++= (Dependencies.buildDependencies ++ Dependencies.testDependencies),
    Compile / avroSourceDirectories += (Compile / resourceDirectory).value / "avro",
    Compile / avroSpecificSourceDirectories += (Compile / resourceDirectory).value / "avro",
    Compile / sourceGenerators += (Compile / avroScalaGenerateSpecific).taskValue
  )
