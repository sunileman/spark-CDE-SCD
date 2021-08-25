
name := "spark-scd"

version := "0.1"

scalaVersion in ThisBuild := "2.11.12"



Global / onChangedBuildSource := ReloadOnSourceChanges


val sparkVersion = "2.4.5"
val phoenixVersion = "5.0.0.7.2.6.1-1"

lazy val commonSettings = Seq(
  organization := "com.cloudera",
  version := "0.1.0-SNAPSHOT"
)


resolvers in Global ++= Seq(
  "Sbt plugins"                   at "https://dl.bintray.com/sbt/sbt-plugin-releases",
  "Maven Central Server"          at "https://repo1.maven.org/maven2",
  "TypeSafe Repository Releases"  at "https://repo.typesafe.com/typesafe/releases/",
  "TypeSafe Repository Snapshots" at "https://repo.typesafe.com/typesafe/snapshots/",
  "cloudera.repo" at "https://cloudera-build-us-west-1.vpc.cloudera.com/s3/build/1377805/cdh/7.x/maven-repository/",
  "hortonworks.public.repo" at "https://repo.hortonworks.com/content/repositories/releases/",
  "hortonworks.repo" at "https://nexus-private.hortonworks.com/nexus/content/groups/public/"
)

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,

  //"org.apache.phoenix" % "phoenix-spark" % "5.0.0.7.2.6.0-71",




  //uncomment before packaging. comment during testing


  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",



  //uncomment during testing, comment out prior to packaging



  /*

  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion ,
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion


   */




)