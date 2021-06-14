package de.tuberlin.cit.adjustments
import org.apache.log4j.Logger
import org.apache.spark.scheduler._
import org.apache.spark.SparkConf

import scala.language.postfixOps

class TestListener(sparkConf: SparkConf) extends SparkListener {
  private val logger: Logger = Logger.getLogger(classOf[StageScaleOutPredictor])
  logger.info("Initializing Test listener")

}
