package de.tuberlin.cit.jobs

import de.tuberlin.cit.adjustments.{EllisScaleOutListener, EnelScaleOutListener}
import org.apache.spark.{SparkConf, SparkContext}

object JobUtils {
  def addCustomListeners(sparkContext: SparkContext, sparkConf: SparkConf): Unit ={

    print("About to add EllisScaleOutListener...")
    sparkContext.addSparkListener(new EllisScaleOutListener(sparkContext, sparkConf))
    print("About to add EnelScaleOutListener...")
    sparkContext.addSparkListener(new EnelScaleOutListener(sparkContext, sparkConf))
  }
}
