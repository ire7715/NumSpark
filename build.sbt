lazy val root = (project in file(".")).settings(
  name := "NumSpark",
  version := "0.0.2",

  scalaVersion := "2.11.12",

  sparkVersion := "2.2.0",
  sparkComponents += "sql",

  libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0",
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % Test,
  libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % Test,

  parallelExecution in Test := false,
  fork in Test := true
)
