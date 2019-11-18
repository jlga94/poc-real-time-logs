package nl.poc.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

object KafkaConsumer {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Geo-Kafka-Demo")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val ds = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.15.19.161:9092")
      .option("subscribe", "http_log")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val df = ds
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val hdfsDF = df.writeStream
      .option("checkpointLocation","/Users/joseaguilar/Documents/Repositories/checkpoint")
      .format("text")
      .start("/Users/joseaguilar/Documents/Repositories/Data")

    hdfsDF.awaitTermination()

  }
}
