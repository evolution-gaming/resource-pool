import Dependencies._

name := "resource-pool"

organization         := "com.evolution"
organizationName     := "Evolution"
organizationHomepage := Some(url("https://evolution.com"))
homepage             := Some(url("https://github.com/evolution-gaming/resource-pool"))
startYear            := Some(2023)

crossScalaVersions := Seq("2.13.14")
scalaVersion       := crossScalaVersions.value.head
scalacOptions ++= Seq("-release:17", "-Xsource:3", "-deprecation", "-Wunused:imports")

releaseCrossBuild      := true
autoAPIMappings        := true
versionScheme          := Some("early-semver")
publishTo              := Some(Resolver.evolutionReleases)
versionPolicyIntention := Compatibility.BinaryCompatible // sbt-version-policy

libraryDependencies += compilerPlugin(`kind-projector` cross CrossVersion.full)
libraryDependencies ++= Seq(
  `cats-effect`,
  scalatest,
)

licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT")))

addCommandAlias("fmt", " all scalafmtAll scalafmtSbt; scalafixEnable; scalafixAll")
addCommandAlias(
  "check",
  "all versionPolicyCheck Compile/doc scalafmtCheckAll scalafmtSbtCheck; scalafixEnable; scalafixAll --check",
)
addCommandAlias("build", "all compile test")
