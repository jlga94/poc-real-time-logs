package nl.poc.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

object KafkaConsumer {
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Please provide <kafka.bootstrap.servers> <checkpointLocation> and <OutputLocation>")
    }

    val spark = SparkSession
      .builder()
      .appName("Geo-Kafka-Demo")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val ds = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", args(0))
      .option("subscribe", "http_log")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val df = ds
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val messagesDF = df.writeStream
      .option("checkpointLocation",args(1))
      .format("text")
      .start(args(2))

    messagesDF.awaitTermination()

  }
}
