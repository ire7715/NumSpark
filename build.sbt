lazy val root = (project in file(".")).settings(
  name := "NumSpark",
  version := "0.0.2",
  scalaVersion := "2.10.4",
  sparkVersion := "1.6.3",
  sparkComponents += "sql",
  libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0",
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test",

  libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.2_0.6.0" % "test",

  parallelExecution in Test := false,
  fork in Test := true
)
