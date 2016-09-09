lazy val root = (project in file(".")).settings(
  name := "NumSpark",
  version := "0.0.0",
  scalaVersion := "2.10.4",
  sparkVersion := "1.6.1",
  libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0",
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  sparkComponents += "sql"
)
