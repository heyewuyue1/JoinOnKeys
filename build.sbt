ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "JoinOnKeys",
    idePackagePrefix := Some("cn.edu.ruc")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.apache.spark" %% "spark-core" % "3.4.0"
)