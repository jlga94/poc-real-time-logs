package nl.poc.streaming

import java.text.SimpleDateFormat
import java.util.Locale
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

case class ApacheLog(host: String,
                     hyphen: String,
                     user_id: String,
                     datetime: Timestamp,
                     req_method: String,
                     req_url: String,
                     req_protocol: String,
                     status: Int,
                     bytes: Int,
                     referrer: String,
                     user_agent: String
                    )

object KafkaConsumer {
  def main(args: Array[String]): Unit = {

    /*
    if (args.length != 3) {
      println("Please provide <kafka.bootstrap.servers> <checkpointLocation> and <OutputLocation>")
    }
     */

    val spark = SparkSession
      .builder()
      .appName("Geo-Kafka-Demo")
      .master("local[2]")
      .getOrCreate()

    val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+)\s?(\S+)?\s?(\S+)?" (\d{3}|-) (\d+|-)\s?"?([^"]*)"?\s?"?([^"]*)?"?$""".r

    import spark.implicits._

    val ds = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.15.19.59:9092")
      .option("subscribe", "http_log")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val df = ds
      .selectExpr("CAST(value AS STRING)")
      .as[String].map(stream => {
          val options = PATTERN.findFirstMatchIn(stream.trim())
          val matched = options.get
          ApacheLog(
            matched.group(1),
            matched.group(2),
            matched.group(3),
            new Timestamp(dateFormat.parse(matched.group(4)).getTime()),
            matched.group(5),
            matched.group(6),
            matched.group(7),
            matched.group(8).toInt,
            matched.group(9).toInt,
            matched.group(10),
            matched.group(11)
          )
      })

    val query = df.writeStream
      .format("console")
      .start()

    query.awaitTermination()

  }
}
