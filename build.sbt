import Dependencies._

name := "resource-pool"

organization := "com.evolution"
organizationName := "Evolution"
organizationHomepage := Some(url("https://evolution.com"))
homepage := Some(url("https://github.com/evolution-gaming/resource-pool"))
startYear := Some(2023)

crossScalaVersions := Seq("2.13.16", "3.3.6")
scalaVersion := crossScalaVersions.value.head
scalacOptions := crossSettings(
  scalaVersion = scalaVersion.value,
  if2 = Seq(
    "-Xsource:3",
  ),
  // Good compiler options for Scala 2.13 are coming from com.evolution:sbt-scalac-opts-plugin:0.0.9,
  // but its support for Scala 3 is limited, especially what concerns linting options.
  //
  // If Scala 3 is made the primary target, good linting scalac options for it should be added first.
  if3 = Seq(
    "-Ykind-projector:underscores",

    // disable new brace-less syntax:
    // https://alexn.org/blog/2022/10/24/scala-3-optional-braces/
    "-no-indent",

    // improve error messages:
    "-explain",
    "-explain-types",
  ),
)
autoAPIMappings := true
versionScheme := Some("early-semver")
publishTo := Some(Resolver.evolutionReleases) // sbt-artifactory-plugin
versionPolicyIntention := Compatibility.BinaryCompatible // sbt-version-policy

libraryDependencies ++= Seq(
  CatsEffect,
  ScalaTest,
)

licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))

def crossSettings[T](scalaVersion: String, if3: T, if2: T): T = {
  scalaVersion match {
    case version if version.startsWith("3") => if3
    case _ => if2
  }
}

addCommandAlias("fmt", "all scalafmtAll scalafmtSbt")
addCommandAlias("check", "all versionPolicyCheck scalafmtCheckAll scalafmtSbtCheck")
addCommandAlias("build", "all compile test")
