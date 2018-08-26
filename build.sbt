import ReleaseTransformations._

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-encoding",
  "utf8",
  "-Ylog-classpath"
)

lazy val commonSettings = Seq(
  version := "0.3.7",
  organization := "com.eztier",
  name := "hl7-cassandra-adapter-scala",
  scalaVersion := "2.12.4",
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public"),
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % Test
val hapiV231 = "ca.uhn.hapi" % "hapi-structures-v231" % "2.3"
val cassandraUdtCodecHelper = "com.eztier" %% "cassandra-udt-codec-helper-scala" % "0.2.19"

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      scalaTest,
      hapiV231,
      cassandraUdtCodecHelper
    )
  )

// Publishing
sonatypeProfileName := "com.eztier"

licenses := Seq("The Apache Software License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

homepage := Some(url("https://github.com/once-ler/hl7-cassandra-adapter-scala"))

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := {_ => false}

releaseCrossBuild := false

releasePublishArtifactsAction := PgpKeys.publishSigned.value

// publishTo := Some(Resolver.file("file", new File("/home/htao/tmp")))

publishTo := sonatypePublishTo.value

/*
publishTo := Some(
  if (isSnapshot.value)
    "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  else
    "releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")
*/

scmInfo := Some(
  ScmInfo(
    browseUrl = url("https://github.com/once-ler/hl7-cassandra-adapter-scala"),
    connection = "scm:git@github.com:once-ler/hl7-cassandra-adapter-scala.git"
  )
)

developers := List(
  Developer(
    id = "once-ler",
    name = "Henry Tao",
    email = "htao@eztier.com",
    url = url("https://github.com/once-ler")
  )
)

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommand("publishSigned"),
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  // releaseStepCommand("sonatypeRelease"),
  pushChanges
)


