// properties
val APP_VERSION = "0.1.0-SNAPSHOT"
val SCALA_VERSION = "2.10.5"
val SPARK_VERSION = "1.5.1"
val DSA_SPARK_VERSION = "0.3.0"
val IGNITION_VERSION = "0.4.0-SNAPSHOT"

// settings
name := "dslink-scala-ignition"
organization := "org.iot-dsa"
version := APP_VERSION
scalaVersion := SCALA_VERSION

// building
resolvers += "sparkts.repo" at "https://repository.cloudera.com/artifactory/libs-release-local/"
resolvers += Resolver.sonatypeRepo("snapshots")
scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation", "-Xlint", 
	"-Ywarn-dead-code", "-language:_", "-target:jvm-1.7", "-encoding", "UTF-8")
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
mainClass in Compile := Some("org.dsa.iot.ignition.Main")

// packaging
enablePlugins(JavaAppPackaging)
mappings in Universal += file("dslink.json") -> "dslink.json"

// dependencies
val sparkLibs = Seq(
  "org.apache.spark"         %% "spark-core"                 % SPARK_VERSION,
  "org.apache.spark"         %% "spark-streaming"            % SPARK_VERSION,
  "org.apache.spark"         %% "spark-streaming-kafka"      % SPARK_VERSION,
  "org.apache.spark"         %% "spark-sql"                  % SPARK_VERSION,
  "org.apache.spark"         %% "spark-mllib"                % SPARK_VERSION,
  "org.apache.spark"         %% "spark-hive"                 % SPARK_VERSION
)

libraryDependencies ++= Seq(
  "io.netty"            % "netty-all"               % "4.0.33.Final",
  "com.uralian"        %% "ignition"                % IGNITION_VERSION
  		exclude("io.netty", "*"),
  "org.iot-dsa"        %% "sdk-dslink-scala-spark"  % DSA_SPARK_VERSION
  		exclude("com.fasterxml.jackson.core", "*"),
  "com.cloudera.sparkts" % "sparkts" % "0.3.0",
  "org.scalatest"      %% "scalatest"               % "2.2.1"         % "test",
  "org.scalacheck"     %% "scalacheck"              % "1.12.1"        % "test",
  "org.mockito"         % "mockito-core"            % "1.10.19"       % "test"
) ++ sparkLibs
