organization in ThisBuild := "com.github.agobi"
scalacOptions in ThisBuild ++= Seq("-Xfatal-warnings","-Xlint","-deprecation","-feature","-unchecked")
scalaVersion in ThisBuild := "2.11.12"
crossScalaVersions in ThisBuild := Seq("2.12.10")

lazy val `alpakka-dynamodb` = project.in(file("alpakka"))
  .settings(
    startDynamoDBLocal := startDynamoDBLocal.dependsOn(compile in Test).value,
    dynamoDBLocalPort := 8384,
    test in Test := (test in Test).dependsOn(startDynamoDBLocal).value,
    testOnly in Test := (testOnly in Test).dependsOn(startDynamoDBLocal).evaluated,
    testOptions in Test += dynamoDBLocalTestCleanup.value,

    libraryDependencies ++= Seq(
      Dependencies.AlpakkaDynamoDB,
      Dependencies.AkkaStream,
      Dependencies.ScalaTest,
      Dependencies.AkkaStreamTestkit
    )
  )


lazy val `dynamodb-root` = project.in(file("."))
  .aggregate(`alpakka-dynamodb`)
  .settings(
    publishArtifact := false
  )
