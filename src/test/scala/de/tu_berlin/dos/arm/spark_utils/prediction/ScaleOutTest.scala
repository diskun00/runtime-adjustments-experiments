package de.tu_berlin.dos.arm.spark_utils.prediction

import de.tu_berlin.dos.arm.spark_utils.adjustments.{Args, StageScaleOutPredictor}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.FlatSpec

class ScaleOutTest extends FlatSpec{

  def test_run(args: Array[String]): Unit = {

    val conf = new Args(args)

    val appSignature = "Spark Test"

    val sparkConf = new SparkConf()
      .setAppName(appSignature)
      .setMaster("local[1]")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.extraListeners", "de.tu_berlin.dos.spark.StageScaleOutPredictor") // this makes sure the 'app start' event is called

    val sparkContext = new SparkContext(sparkConf)
    val listener = new StageScaleOutPredictor(
      sparkContext,
      appSignature,
      conf.dbPath(),
      conf.minContainers(),
      conf.maxContainers(),
      conf.maxRuntime().toInt,
      conf.adaptive())
    sparkContext.addSparkListener(listener)

    val data = sparkContext.textFile("/Users/diskun/Work/Workspace/Java/runtime-adjustments-experiments/kmeans.txt", 2)

    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))

    val clusters = new org.apache.spark.mllib.clustering.KMeans()
      .setEpsilon(0)
      .setK(5)
      .setMaxIterations(10)
      .run(parsedData)
    //    val clusters = MLLibKMeans.train(parsedData, conf.k(), conf.iterations())

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    clusters.clusterCenters.foreach(v => {
      println(v)
    })

    sparkContext.stop()
  }

}
