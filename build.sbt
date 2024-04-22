import Dependencies._

name := "resource-pool"

organization := "com.evolution"
organizationName := "Evolution"
organizationHomepage := Some(url("https://evolution.com"))
homepage := Some(url("https://github.com/evolution-gaming/resource-pool"))
startYear := Some(2023)

scalaVersion := crossScalaVersions.value.head
crossScalaVersions := Seq("2.13.13")
scalacOptions := Seq(
  "-release:17",
  "-Xsource:3-cross",
)
releaseCrossBuild := true
autoAPIMappings := true
versionScheme := Some("early-semver")
publishTo := Some(Resolver.evolutionReleases)

libraryDependencies += compilerPlugin(`kind-projector` cross CrossVersion.full)
libraryDependencies ++= Seq(
  `cats-effect`,
  scalatest,
)

licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))
