package nl.scala.utils

import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val topic = opt[String](required = true)
  val kafka_broker = opt[String](required = true)
  val geo_filename = opt[String](required = true)
  verify()
}