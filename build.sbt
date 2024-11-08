import Dependencies._

name := "resource-pool"

organization := "com.evolution"
organizationName := "Evolution"
organizationHomepage := Some(url("https://evolution.com"))
homepage := Some(url("https://github.com/evolution-gaming/resource-pool"))
startYear := Some(2023)

crossScalaVersions := Seq("2.13.15", "3.3.4")
scalaVersion := crossScalaVersions.value.head
scalacOptions := Seq(
  "-release:17",
  "-Xsource:3",
  "-deprecation"
)
autoAPIMappings := true
versionScheme := Some("early-semver")
publishTo := Some(Resolver.evolutionReleases) // sbt-release
versionPolicyIntention := Compatibility.BinaryCompatible // sbt-version-policy

libraryDependencies ++= Seq(
  `cats-effect`,
  scalatest
)

licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))

//addCommandAlias("fmt", "scalafixAll; all scalafmtAll scalafmtSbt")
//addCommandAlias("check", "scalafixEnable; scalafixAll --check; all versionPolicyCheck scalafmtCheckAll scalafmtSbtCheck")
addCommandAlias("check", "versionPolicyCheck")
addCommandAlias("build", "all compile test")
