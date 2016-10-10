// properties
val APP_VERSION = "0.1.0-SNAPSHOT"
val SCALA_VERSION = "2.11.8"
val SPARK_VERSION = "1.6.1"
val SCALA_DSA_VERSION = "0.4.0"
val IGNITION_VERSION = "0.4.0"

// settings
name := "dslink-scala-ignition"
organization := "org.iot-dsa"
version := APP_VERSION
scalaVersion := SCALA_VERSION
//crossScalaVersions := Seq("2.10.5", SCALA_VERSION)

// building
resolvers += "sparkts.repo" at "https://repository.cloudera.com/artifactory/libs-release-local/"
resolvers += Resolver.sonatypeRepo("snapshots")
resolvers += "Bintray megamsys" at "https://dl.bintray.com/megamsys/scala/"
scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation", "-Xlint", "-language:_", "-target:jvm-1.7", "-encoding", "UTF-8")
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
mainClass in Compile := Some("org.dsa.iot.ignition.Main")

// packaging
enablePlugins(JavaAppPackaging)
mappings in Universal += file("dslink.json") -> "dslink.json"

// publishing docs to github
site.settings
site.includeScaladoc()
ghpages.settings
git.remoteRepo := "https://github.com/IOT-DSA/dslink-scala-ignition.git"

// publishing to maven repo
publishMavenStyle := true
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
pomIncludeRepository := { _ => false }
pomExtra := (
  <url>https://github.com/IOT-DSA/dslink-scala-ignition</url>
  <licenses>
    <license>
      <name>The Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>scm:git:https://github.com/IOT-DSA/dslink-scala-ignition.git</url>
    <connection>scm:git:git@github.com:IOT-DSA/dslink-scala-ignition.git</connection>
  </scm>
  <developers>
    <developer>
      <id>snark</id>
      <name>Vlad Orzhekhovskiy</name>
      <email>vlad@uralian.com</email>
      <url>http://uralian.com</url>
    </developer>
  </developers>)
  
pgpSecretRing := file("local.secring.gpg")

pgpPublicRing := file("local.pubring.gpg")

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
  "io.netty"                  % "netty-all"                  % "4.0.33.Final",
  "com.uralian"              %% "ignition"                   % IGNITION_VERSION
  		exclude("io.netty", "*"),
  "org.iot-dsa"              %% "sdk-dslink-scala"           % SCALA_DSA_VERSION
  		exclude("com.fasterxml.jackson.core", "*"),   		
  "org.scalatest"            %% "scalatest"                  % "2.2.1"                 % "test",
  "org.scalacheck"           %% "scalacheck"                 % "1.12.1"                % "test",
  "org.mockito"               % "mockito-core"               % "1.10.19"               % "test"
) ++ sparkLibs