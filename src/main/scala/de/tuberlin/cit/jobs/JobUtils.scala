package de.tuberlin.cit.jobs

import de.tuberlin.cit.adjustments.{EllisScaleOutListener, EnelScaleOutListener}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.SparkListener

object JobUtils {
  def handleMethod(sparkContext: SparkContext, sparkConf: SparkConf): SparkListener ={
    var listener: SparkListener = null
    if(sparkConf.contains("spark.customExtraListener.method")){
      val method: String = sparkConf.get("spark.customExtraListener.method")
      if(method.equals("enel")){
        listener = new EnelScaleOutListener(sparkContext, sparkConf)
      }
      else if(method.equals("ellis")){
        listener = new EllisScaleOutListener(sparkContext, sparkConf)
      }
    }

    if(listener == null){
      throw new IllegalArgumentException("No listener initialized!")
    }

    listener
  }
}
