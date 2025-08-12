import Dependencies._

name := "resource-pool"

organization := "com.evolution"
organizationName := "Evolution"
organizationHomepage := Some(url("https://evolution.com"))
homepage := Some(url("https://github.com/evolution-gaming/resource-pool"))
startYear := Some(2023)

crossScalaVersions := Seq("2.13.16", "3.3.6")
scalaVersion := crossScalaVersions.value.head
scalacOptions := Seq(
  "-release:17",
  "-Xsource:3",
  "-deprecation",
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

addCommandAlias("fmt", "all scalafmtAll scalafmtSbt")
addCommandAlias("check", "all versionPolicyCheck scalafmtCheckAll scalafmtSbtCheck")
addCommandAlias("build", "all compile test")
