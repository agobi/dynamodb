import sbt._

object Dependencies {
  val akkaV = "2.5.6"

  val AlpakkaDynamoDB   = "com.lightbend.akka"     %% "akka-stream-alpakka-dynamodb" % "0.14"
  val AkkaStream        = "com.typesafe.akka"      %% "akka-stream"                  % akkaV
  val Scanamo           = "com.gu"                 %% "scanamo"                      % "1.0.0-M2"
  val ScalaTest         = "org.scalatest"          %% "scalatest"                    % "3.0.4"  % "test"
  val AkkaStreamTestkit = "com.typesafe.akka"      %% "akka-stream-testkit"          % akkaV    % "test"
}
