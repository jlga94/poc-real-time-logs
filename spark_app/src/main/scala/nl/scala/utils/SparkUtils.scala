package nl.scala.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {
   def getSparkSession(appName: String): SparkSession = {
      val spark = SparkSession
        .builder
        .master("local[*]") // Just for local test, comment this line before generating the FAT JAR
        .appName(appName)
        .enableHiveSupport
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      spark
   }
}
