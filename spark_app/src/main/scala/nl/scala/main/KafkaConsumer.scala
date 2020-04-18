package nl.scala.main

import scala.util.matching.Regex
import java.text.SimpleDateFormat
import java.util.Locale
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

import com.sanoma.cda.geoip.{IpLocation, MaxMindIpGeo}

import nl.scala.main.classes.{ApacheLog, Geolocation, LogGeolocation}
import nl.scala.utils.{Conf,SparkUtils}


class KafkaConsumer(conf: Conf) {

  implicit val spark: SparkSession =
    SparkUtils.getSparkSession("Real-time-logs")

  def execute(): Unit = {
    try {
      ingestRealTimeLogs()
    } catch {
      case e: Exception =>
        println(e.getMessage)
        throw e
    } finally {
      spark.stop()
    }
  }

  def ingestRealTimeLogs(): Unit = {

    /**
     * The dateFormat need to be set up using the same format as the logs generated
     * The Regex Pattern is already set up, but it would be better to check the content over here
     * https://regex101.com/
     */
    val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZZZ", Locale.ENGLISH)
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+)\s?(\S+)?\s?(\S+)?" (\d{3}|-) (\d+|-)\s?"?([^"]*)"?\s?"?([^"]*)?"?$""".r

    /**
     * These variables need to be setup from the beginning, also could be set up as parameters:
     * geo_filename: You need to download from here:
     * https://dev.maxmind.com/geoip/geoip2/geolite2/
     * You need to setup the path for the file GeoLite2-City.mmdb
     *
     * input_topic: The topic from Kafka where we are going to listen, set it up with http_log
     * kafka_broker: The ip broker
     * output_topic: The topic from Kafka where we are going the send messages
     * spark_checkpoint: It is a folder that is required for Spark to send message to Kafka, it is used for fault-tolerance
     *
     */
    val geo_filename = conf.geo_filename()

    import spark.implicits._

    val ds = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.kafka_broker())
      .option("subscribe", conf.input_topic())
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    /**
     * Get the data from the stream, use the MaxMindIpGeo to get location based on the ip
     * Filter the streams that don't have location
     * Transform the data into Json, put the alias Value
     * Write the stream into the output_topic into Kafka
     */
    val df = ds
      .selectExpr("CAST(value AS STRING)")
      .as[String].map(stream => {
      val matched = PATTERN.findFirstMatchIn(stream.trim()).get
      val geoIp = MaxMindIpGeo(geo_filename, 1000, synchronized = true)
      val log = get_apache_log(matched, dateFormat)

      val ip_location = geoIp.getLocation(log.host).getOrElse(null)
      val data_location = get_data_location(ip_location)

      get_log_geolocation(log, data_location)
    })

    val dfFilter = df
      .filter(_.countryCode!=null)
      .select(
        to_json(
          struct(
            df.columns
              .map(col(_)):_*
          )
        ).alias("value")
      )

    val query = dfFilter.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.kafka_broker())
      .option("topic", conf.output_topic())
      .option("checkpointLocation",conf.checkpoint())
      .start()

    query.awaitTermination()
  }

  /**
   * Get all the data from the Regex, and create an ApacheLog with the correct data type
   *
   * @param matched Get all the data from the matched Regex using group
   * @param dateFormat It will be used to get the Timestamp
   * @return
   */
  def get_apache_log(matched: Regex.Match, dateFormat: SimpleDateFormat): ApacheLog = {
    classes.ApacheLog(
      matched.group(1),
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
  }

  /**
   * Get all the data from the iplocation, and create an Geolocation with the correct data type, if there is no
   * match with the data, fill it with null or 0 for integers
   *
   * @param iplocation Get all the data from the iplocation
   * @return
   */
  def get_data_location(iplocation: IpLocation): Geolocation = {
    var data_location: Geolocation = null
    iplocation match {
      case null => {
        data_location = new Geolocation(
          null,
          null,
          null,
          null,
          0.0,
          0.0,
          null,
          null,
          null,
          null,
          null
        )
      }
      case _ => {
        val geopoint = iplocation.geoPoint.getOrElse(null)
        var latitude = 0.0
        var longitude = 0.0
        geopoint match {
          case null => {
            latitude = 0.0
            longitude = 0.0
          }
          case _ => {
            latitude = geopoint.latitude
            longitude = geopoint.longitude
          }
        }

        data_location = new Geolocation(
          iplocation.countryCode.getOrElse("None"),
          iplocation.countryName.getOrElse("None"),
          iplocation.region.getOrElse("None"),
          iplocation.city.getOrElse("None"),
          latitude,
          longitude,
          iplocation.postalCode.getOrElse("None"),
          iplocation.continent.getOrElse("None"),
          iplocation.regionCode.getOrElse("None"),
          iplocation.continentCode.getOrElse("None"),
          iplocation.timezone.getOrElse("None")
        )
      }
    }
    data_location
  }

  /**
   * Get all the data from both classes and merge it into a new one
   *
   * @param log
   * @param data_location
   * @return
   */
  def get_log_geolocation(log: ApacheLog, data_location: Geolocation): LogGeolocation = {
    classes.LogGeolocation(
      log.host,
      log.user_id,
      log.datetime,
      log.req_method,
      log.req_url,
      log.req_protocol,
      log.status,
      log.bytes,
      log.referrer,
      log.user_agent,
      data_location.countryCode,
      data_location.countryName,
      data_location.region,
      data_location.city,
      data_location.latitude,
      data_location.longitude,
      data_location.postalCode,
      data_location.continent,
      data_location.regionCode,
      data_location.continentCode,
      data_location.timezone
    )
  }
}
