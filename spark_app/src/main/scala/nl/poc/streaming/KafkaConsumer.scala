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
import com.sanoma.cda.geoip.{IpLocation, MaxMindIpGeo}

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
                     user_agent: String,
                     countryCode: String,
                     countryName: String,
                     region: String,
                     city: String,
                     latitude: Double,
                     longitude: Double,
                     postalCode: String,
                     continent: String,
                     regionCode: String,
                     continentCode: String,
                     timezone: String
                    )

case class dataLocation(
                       countryCode: String,
                       countryName: String,
                       region: String,
                       city: String,
                       latitude: Double,
                       longitude: Double,
                       postalCode: String,
                       continent: String,
                       regionCode: String,
                       continentCode: String,
                       timezone: String
                       )



object KafkaConsumer {

  def get_data_location(iplocation:IpLocation): dataLocation = {
    var data_location: dataLocation = null
    iplocation match {
      case null => {
        data_location = new dataLocation(
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

        data_location = new dataLocation(
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

    val geo_filename = "/Users/joseaguilar/Documents/Repositories/GeoIP/GeoLite2-City_20191105/GeoLite2-City.mmdb"

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
          val geoIp = MaxMindIpGeo(geo_filename, 1000, synchronized = true)
          val iplocation = geoIp.getLocation(matched.group(1)).getOrElse(null)

          val data_location = get_data_location(iplocation)

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
                matched.group(11),
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
      })

    val query = df.writeStream
      .format("console")
      .start()

    query.awaitTermination()

  }
}
