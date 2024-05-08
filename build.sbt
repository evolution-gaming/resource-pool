import Dependencies._

name := "resource-pool"

organization := "com.evolution"
organizationName := "Evolution"
organizationHomepage := Some(url("https://evolution.com"))
homepage := Some(url("https://github.com/evolution-gaming/resource-pool"))
startYear := Some(2023)

inThisBuild(
  List(
    crossScalaVersions := Seq("2.13.14"),
    scalaVersion := crossScalaVersions.value.head,
    scalacOptions := Seq(
      "-release:17",
      "-Xsource:3",
      "-deprecation",
      "-Wunused:imports",
    ),
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
  ),
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

addCommandAlias("build", "all compile test")

// https://github.com/scalacenter/scalafix/issues/1488
addCommandAlias("check", "scalafixAll --check; all scalafmtCheckAll scalafmtSbtCheck")
addCommandAlias("fix", "scalafixAll; all scalafmtAll scalafmtSbt")
