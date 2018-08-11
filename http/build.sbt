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
  version := "0.1.1",
  organization := "com.eztier",
  scalaVersion := "2.12.4",
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public"),
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

val akka = "com.typesafe.akka"
val akkaHttpV = "10.1.0"

val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"

val akkaStream = akka %% "akka-stream" % "2.5.9"
val akkaSlf4j = akka %% "akka-slf4j" % "2.5.9"
val akkaStreamTestkit = akka %% "akka-stream-testkit" % "2.5.9" % Test
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4" % Test

// HTTP server
val akkaHttp = akka %% "akka-http" % akkaHttpV
val akkaHttpCore = akka %% "akka-http-core" % akkaHttpV
val akkaHttpSprayJson = akka %% "akka-http-spray-json" % akkaHttpV
val akkaHttpTestkit = akka %% "akka-http-testkit" % akkaHttpV % Test

// Support of CORS requests, version depends on akka-http
val akkaHttpCors = "ch.megard" %% "akka-http-cors" % "0.3.0"

val hapiV231 = "ca.uhn.hapi" % "hapi-structures-v231" % "2.3"

lazy val http = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "test-scalaxb",
    libraryDependencies ++= Seq(
      scalaTest,
      logback,
      akkaStream,
      akkaSlf4j,
      akkaStreamTestkit,
      akkaHttp,
      akkaHttpCore,
      akkaHttpSprayJson,
      akkaHttpTestkit,
      hapiV231
    )
  )