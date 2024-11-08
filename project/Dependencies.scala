import sbt._

object Dependencies {
  val scalatest        = "org.scalatest" %% "scalatest"      % "3.2.18" % Test
  val `kind-projector` = "org.typelevel"  % "kind-projector" % "0.13.3"
  val `cats-effect`    = "org.typelevel" %% "cats-effect"    % "3.5.5"
}
