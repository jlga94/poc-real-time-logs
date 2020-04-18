package nl.scala.main.classes

import java.sql.Timestamp

case class ApacheLog(
                      host: String,
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
