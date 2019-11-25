name := "geo_app"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

resolvers ++= Seq(
  "jitpack.io" at "https://jitpack.io",
  "Artima Maven Repository" at "http://repo.artima.com/releases",
  "Sonatype Repository" at "https://oss.sonatype.org/content/repositories/releases/",
  "Central Repository" at "http://central.maven.org/maven2/",
  "Feature.fm Repository" at "https://dl.bintray.com/listnplay/maven/"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

libraryDependencies += "com.sanoma.cda" %% "maxmind-geoip2-scala" % "1.5.5"
