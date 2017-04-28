organization := "com.nyu"

name := "data-engineering-task"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.7"

libraryDependencies ++= {
  val sparkVersion =  "2.1.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.joda" % "joda-convert" % "1.8.1",
    "com.databricks" %% "spark-csv" % "1.5.0",
    "mysql" % "mysql-connector-java" % "5.1.24",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3" % "test"
  )
}

// ToDo: This setting is not optimized, need more work to take out the delete META-INF files outside (some examples already here)
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "hadoop", "hadoop-yarn-common", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "hadoop", "hadoop-yarn-api", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "spark", "spark-core_2.11", xs @ _*) => MergeStrategy.first
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.first
  case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
  case PathList("org", "glassfish", xs @ _*) => MergeStrategy.first
  case "aopalliance" => MergeStrategy.first
  case "commons-beanutils" => MergeStrategy.first
  case "commons-beanutils-core" => MergeStrategy.first
  case  "commons-collections" => MergeStrategy.first
  case "commons-logging" => MergeStrategy.first
  case "about.html"  => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "*.RSA") => MergeStrategy.discard
  case PathList("META-INF", "*.DSA") => MergeStrategy.discard
  case PathList("META-INF", "*.SF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

mainClass in assembly := Some("com.nyu.usract.UserActivityPipeline")
