package nl.poc.streaming

import java.sql.Timestamp
import scala.util.matching.Regex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

case class SparkApacheLog(host: String,
                          hyphen: String,
                          user_id: String,
                          datetime: Timestamp,
                          req_method: String,
                          req_url: String,
                          req_protocol: String,
                          status: String,
                          bytes: Int,
                          referrer: String,
                          user_agent: String
                         )

case class ApacheLog(
                host: String,
                rfc931: String,
                username: String,
                data_time: String,
                req_method: String,
                req_url: String,
                req_protocol: String,
                statuscode: String,
                bytes: String,
                referrer: String,
                user_agent: String)

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

    val pattern = new Regex("^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)\\s?(\\S+)?\\s?(\\S+)?\" (\\d{3}|-) (\\d+|-)\\s?\"?([^\"]*)\"?\\s?\"?([^\"]*)?\"?$")

    val regex = """^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)\\s?(\\S+)?\\s?(\\S+)?\" (\\d{3}|-) (\\d+|-)\\s?\"?([^\"]*)\"?\\s?\"?([^\"]*)?\"?$""".r

    import spark.implicits._

    val ds = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.15.19.89:9092")
      .option("subscribe", "http_log")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val df = ds
      .selectExpr("CAST(value AS STRING)")
      .as[String].map(s => {
          println(s)
          val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+)\s?(\S+)?\s?(\S+)?" (\d{3}|-) (\d+|-)\s?"?([^"]*)"?\s?"?([^"]*)?"?$""".r
          val options = PATTERN.findFirstMatchIn(s)
          val matched = options.get
          println(matched)
        ApacheLog(matched.group(1),matched.group(2),matched.group(3),matched.group(4),matched.group(5),matched.group(6),matched.group(7),matched.group(8),matched.group(9),matched.group(10),matched.group(11))
      })

    val query = df.writeStream
      .format("console")
      .start()

    query.awaitTermination()

  }
}
