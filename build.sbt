name := "Prueba"

version := "0.1"

scalaVersion := "2.11.12"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
"org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
)

// como tenemos varias clases con main(), pues tenemos que indicar cual es la que queremos utilizar para empaquetar en el jar
//mainClass in (Compile, run) := Some("Orange")
//Compile / run / mainClass := Some("Prueba:Orange")
//mainClass in (Compile, run) := Some("Prueba:Orange")
Compile/mainClass := Some("Prueba.Orange")

//empaquetar en un jar
exportJars := true
