package nl.scala.utils

import org.rogach.scallop.ScallopConf

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input_topic = opt[String](required = true)
  val output_topic = opt[String](required = true)
  val kafka_broker = opt[String](required = true)
  val geo_filename = opt[String](required = true)
  val checkpoint = opt[String](required = true)
  verify()
}