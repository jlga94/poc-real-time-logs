package nl.scala.main

import nl.scala.utils.Conf


object Main {
   def main(args: Array[String]): Unit = {
      val conf = new Conf(args)
      val sparkProject = new KafkaConsumer(conf)
      sparkProject.execute()
   }
}
