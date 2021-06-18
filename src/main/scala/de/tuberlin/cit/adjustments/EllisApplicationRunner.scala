package de.tuberlin.cit.adjustments

import org.apache.log4j.{BasicConfigurator, Logger}
import py4j.GatewayServer


object EllisApplicationRunner {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()

    val app: EllisApplication = new EllisApplication()
    val server: GatewayServer = new GatewayServer(app)
    Logger.getLogger(classOf[EllisApplication]).info("Gateway Server started...")
    server.start()
  }
}
