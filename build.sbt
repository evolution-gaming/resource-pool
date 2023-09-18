import Dependencies._

name := "resource-pool"

organization := "com.evolution"

homepage := Some(new URL("http://github.com/evolution-gaming/resource-pool"))

startYear := Some(2023)

organizationName := "Evolution"

organizationHomepage := Some(url("http://evolution.com"))

scalaVersion := crossScalaVersions.value.head

crossScalaVersions := Seq("2.13.12")

publishTo := Some(Resolver.evolutionReleases)

libraryDependencies += compilerPlugin(`kind-projector` cross CrossVersion.full)

scalacOptsFailOnWarn := Some(false)

libraryDependencies ++= Seq(
  `cats-effect`,
  scalatest % Test,
)

licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))

releaseCrossBuild := true

versionScheme := Some("early-semver")
