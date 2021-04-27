package de.tuberlin.cit.adjustments

import breeze.numerics.pow
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.util.concurrent.ThreadLocalRandom

object SparkTest {


  def main(args: Array[String]): Unit = {

    val conf = new Args(args)

    val appSignature = "Spark Test"

    val sparkConf = new SparkConf()
      .setAppName(appSignature)
      .setMaster("local[1]")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.extraListeners", "de.tuberlin.cit.spark.StageScaleOutPredictor") // this makes sure the 'app start' event is called

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

  def getTrainingSet(sc: SparkContext, m: Int, n: Int): RDD[LabeledPoint] = {
    sc
      .range(1, m)
      .map(_ => {
        val x = ThreadLocalRandom.current().nextDouble()
        val noise = ThreadLocalRandom.current().nextGaussian()

        // generate the function value with added gaussian noise
        val label = function(x) + noise

        // generate a vandermonde matrix from x
        val vector = polyvander(x, n - 1)

        LabeledPoint(label, new DenseVector(vector))
      })
  }

  def polyvander(x: Double, order: Int): Array[Double] = {
    (0 to order).map(pow(x, _)).toArray
  }

  def function(x: Double): Double = {
    2 * x + 10
  }

}
