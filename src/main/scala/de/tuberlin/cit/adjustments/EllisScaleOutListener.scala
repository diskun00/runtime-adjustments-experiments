package de.tuberlin.cit.adjustments

import java.util.Date
import breeze.linalg._
import de.tuberlin.cit.prediction.{Bell, Ernest, UnivariatePredictor}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler._
import scalikejdbc._
import org.apache.log4j.Logger

import scala.language.postfixOps

class EllisScaleOutListener(sparkConf: SparkConf) extends SparkListener {
  private val logger: Logger = Logger.getLogger(classOf[StageScaleOutPredictor])
  logger.info("Initializing Ellis listener")
  private var sparkContext: SparkContext = _
  private val appSignature: String = sparkConf.get("spark.app.name")
  check_configurations()
  private val dbPath: String = sparkConf.get("spark.extraListeners.ellis.dbPath")
  private val minExecutors: Int = sparkConf.get("spark.extraListeners.ellis.minExecutors").toInt
  private val maxExecutors: Int = sparkConf.get("spark.extraListeners.ellis.maxExecutors").toInt
  private val targetRuntimeMs: Int = sparkConf.get("spark.extraListeners.ellis.targetRuntimems").toInt
  private val isAdaptive: Boolean = sparkConf.getBoolean("spark.extraListeners.ellis.isAdaptive",true)

  private var appEventId: Long = _
  private var appStartTime: Long = _
  private var jobStartTime: Long = _
  private var jobEndTime: Long = _

  private var scaleOut: Int = _
  private var nextScaleOut: Int = _


  Class.forName("org.h2.Driver")
  ConnectionPool.singleton(s"jdbc:h2:$dbPath", "sa", "")



  def init(): Unit ={
    scaleOut = computeInitialScaleOut()
    nextScaleOut = scaleOut
    logger.info(s"Using initial scale-out of $scaleOut.")
    sparkContext = SparkContext.getOrCreate(sparkConf)
    sparkContext.requestTotalExecutors(scaleOut, 0, Map[String,Int]())
  }

  def check_configurations(){
    /**
     * check parameters are set in environment
     */
    val parametersList = List("spark.extraListeners.ellis.dbPath", "spark.extraListeners.ellis.minExecutors",
      "spark.extraListeners.ellis.maxExecutors", "spark.extraListeners.ellis.targetRuntimems")
    logger.info("Current spark conf" + sparkConf.toDebugString)
    for (param <- parametersList) {
      if (!sparkConf.contains(param)) {
        throw new IllegalArgumentException("parameter " + param + " is not shown in the Environment!")
      }
    }
  }

  def computeInitialScaleOut(): Int = {
    val (scaleOuts, runtimes) = getNonAdaptiveRuns

    val halfExecutors = (minExecutors + maxExecutors) / 2

    scaleOuts.length match {
      case 0 => maxExecutors
      case 1 => halfExecutors
      case 2 =>

        if (runtimes.max < targetRuntimeMs) {
          (minExecutors + halfExecutors) / 2
        } else {
          (halfExecutors + maxExecutors) / 2
        }

      case _ =>

        val predictedScaleOuts = (minExecutors to maxExecutors).toArray
        val predictedRuntimes = computePredictionsFromStageRuntimes(predictedScaleOuts)

        val candidateScaleOuts = (predictedScaleOuts zip predictedRuntimes)
          .filter(_._2 < targetRuntimeMs)
          .map(_._1)

        if (candidateScaleOuts.isEmpty) {
          predictedScaleOuts(argmin(predictedRuntimes))
        } else {
          candidateScaleOuts.min
        }

    }

  }

  def computePredictionsFromStageRuntimes(predictedScaleOuts: Array[Int]): Array[Int] = {
    val result = DB readOnly { implicit session =>
      sql"""
      SELECT JOB_ID, SCALE_OUT, DURATION_MS
      FROM APP_EVENT JOIN JOB_EVENT ON APP_EVENT.ID = JOB_EVENT.APP_EVENT_ID
      WHERE APP_ID = ${appSignature}
      ORDER BY JOB_ID;
      """.map({ rs =>
        val jobId = rs.int("job_id")
        val scaleOut = rs.int("scale_out")
        val durationMs = rs.int("duration_ms")
        (jobId, scaleOut, durationMs)
      }).list().apply()
    }
    val jobRuntimeData: Map[Int, List[(Int, Int, Int)]] = result.groupBy(_._1)

    val predictedRuntimes: Array[DenseVector[Int]] = jobRuntimeData.keys
      .toArray
      .sorted
      .map(jobId => {
        val (x, y) = jobRuntimeData(jobId).map(t => (t._2, t._3)).toArray.unzip
        val predictedRuntimes: Array[Int] = computePredictions(x, y, predictedScaleOuts)
        DenseVector(predictedRuntimes)
      })

    predictedRuntimes.fold(DenseVector.zeros[Int](predictedScaleOuts.length))(_ + _).toArray
  }

  def getNonAdaptiveRuns: (Array[Int], Array[Int]) = {
    val result = DB readOnly { implicit session =>
      sql"""
      SELECT APP_EVENT.STARTED_AT, SCALE_OUT, DURATION_MS
      FROM APP_EVENT JOIN JOB_EVENT ON APP_EVENT.ID = JOB_EVENT.APP_EVENT_ID
      WHERE APP_ID = ${appSignature};
      """.map({ rs =>
        val startedAt = rs.timestamp("started_at")
        val scaleOut = rs.int("scale_out")
        val durationMs = rs.int("duration_ms")
        (startedAt, scaleOut, durationMs)
      }).list().apply()
    }

    val (scaleOuts, runtimes) = result
      .groupBy(_._1)
      .toArray
      .flatMap(t => {
        val jobStages = t._2
        val scaleOuts = jobStages.map(_._2)
        val scaleOut = scaleOuts.head
        val nonAdaptive = scaleOuts.forall(scaleOut == _)
        if (nonAdaptive) {
          val runtime = jobStages.map(_._3).sum
          List((scaleOut, runtime))
        } else {
          List()
        }
      })
      .unzip

    (scaleOuts, runtimes)
  }

  def computePredictions(scaleOuts: Array[Int], runtimes: Array[Int], predictedScaleOuts: Array[Int]): Array[Int] = {
    val x = convert(DenseVector(scaleOuts), Double)
    val y = convert(DenseVector(runtimes), Double)

    // calculate the range over which the runtimes must be predicted
    val xPredict = DenseVector(predictedScaleOuts)

    // subdivide the scaleout range into interpolation and extrapolation
    //    val interpolationMask: BitVector = (xPredict :>= min(scaleOuts)) :& (xPredict :<= max(scaleOuts))
    val interpolationMask: BitVector = (xPredict >:= min(scaleOuts)) &:& (xPredict <:= max(scaleOuts))
    val xPredictInterpolation = xPredict(interpolationMask).toDenseVector
    val xPredictExtrapolation = xPredict(!interpolationMask).toDenseVector

    // predict with respective model
    val yPredict = DenseVector.zeros[Double](xPredict.length)

    // fit ernest
    val ernest: UnivariatePredictor = new Ernest()
    ernest.fit(x, y)

    val uniqueScaleOuts = unique(x).length
    if (uniqueScaleOuts <= 2) {
      // for very few data, just take the mean
      yPredict := sum(y) / y.length
    } else if (uniqueScaleOuts <= 5) {
      // if too few data use ernest model
      yPredict := ernest.predict(convert(xPredict, Double))
    } else {
      // fit data using bell (for interpolation)
      val bell: UnivariatePredictor = new Bell()
      bell.fit(x, y)
      yPredict(interpolationMask) := bell.predict(convert(xPredictInterpolation, Double))
      yPredict(!interpolationMask) := ernest.predict(convert(xPredictExtrapolation, Double))
    }

    yPredict.map(_.toInt).toArray
  }


  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {

    DB localTx { implicit session =>
      sql"""
      UPDATE app_event
      SET finished_at = ${new Date(applicationEnd.time)}
      WHERE id = ${appEventId};
      """.update().apply()
    }

  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    logger.info(s"Job ${jobStart.jobId} started.")
    jobStartTime = jobStart.time
    if( jobStart.jobId == 0){
      logger.info("Initializing Ellis listener on first job.")
      init()
    }

    // https://stackoverflow.com/questions/29169981/why-is-sparklistenerapplicationstart-never-fired
    // onApplicationStart workaround
    if (appStartTime == 0) {
      appStartTime = jobStartTime

      DB localTx { implicit session =>
        appEventId =
          sql"""
        INSERT INTO app_event (
          app_id,
          started_at
        )
        VALUES (
          ${appSignature},
          ${new Date(appStartTime)}
        );
        """.updateAndReturnGeneratedKey("id").apply()
      }
    }

  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    jobEndTime = jobEnd.time
    val jobDuration = jobEndTime - jobStartTime
    logger.info(s"Job ${jobEnd.jobId} finished in $jobDuration ms with $scaleOut nodes.")



    DB localTx { implicit session =>
      sql"""
      INSERT INTO job_event (
        app_event_id,
        job_id,
        started_at,
        finished_at,
        duration_ms,
        scale_out
      )
      VALUES (
        ${appEventId},
        ${jobEnd.jobId},
        ${new Date(jobStartTime)},
        ${new Date(jobEndTime)},
        ${jobDuration},
        ${scaleOut}
      );
      """.update().apply()
    }

    if (nextScaleOut != scaleOut) {
      scaleOut = nextScaleOut
    }

    if (isAdaptive) {
      val (scaleOuts, runtimes) = getNonAdaptiveRuns
      if (scaleOuts.length > 3) { // do not scale adaptively for the bootstrap runs
        updateScaleOut(jobEnd.jobId)
      }
    }

  }

  def updateScaleOut(jobId: Int): Unit = {
    val result = DB readOnly { implicit session =>
      sql"""
      SELECT JOB_ID, SCALE_OUT, DURATION_MS
      FROM APP_EVENT JOIN JOB_EVENT ON APP_EVENT.ID = JOB_EVENT.APP_EVENT_ID
      WHERE APP_ID = ${appSignature}
      ORDER BY JOB_ID;
      """.map({ rs =>
        val jobId = rs.int("job_id")
        val scaleOut = rs.int("scale_out")
        val durationMs = rs.int("duration_ms")
        (jobId, scaleOut, durationMs)
      }).list().apply()
    }
    val jobRuntimeData: Map[Int, List[(Int, Int, Int)]] = result.groupBy(_._1)

    // calculate the prediction for the remaining runtime depending on scale-out
    val predictedScaleOuts = (minExecutors to maxExecutors).toArray
    val remainingRuntimes: Array[DenseVector[Int]] = jobRuntimeData.keys
      .filter(_ > jobId)
      .toArray
      .sorted
      .map(jobId => {
        val (x, y) = jobRuntimeData(jobId).map(t => (t._2, t._3)).toArray.unzip
        val predictedRuntimes: Array[Int] = computePredictions(x, y, predictedScaleOuts)
        DenseVector(predictedRuntimes)
      })

    if (remainingRuntimes.length <= 1) {
      return
    }

    val nextJobId = jobRuntimeData.keys.filter(_ > jobId).min

    // predicted runtimes of the next job
    val nextJobRuntimes = remainingRuntimes.head
    // predicted runtimes sum of the jobs *after* the next job
    val futureJobsRuntimes = remainingRuntimes.drop(1).fold(DenseVector.zeros[Int](predictedScaleOuts.length))(_ + _)

    val currentRuntime = jobEndTime - appStartTime
    logger.info(s"Current runtime: $currentRuntime")
    val nextJobRuntime = nextJobRuntimes(scaleOut - minExecutors)
    logger.info(s"Next job runtime prediction: $nextJobRuntime")
    val remainingTargetRuntime = targetRuntimeMs - currentRuntime - nextJobRuntime
    logger.info(s"Remaining runtime: $remainingTargetRuntime")
    val remainingRuntimePrediction = futureJobsRuntimes(scaleOut - minExecutors)
    logger.info(s"Remaining runtime prediction: $remainingRuntimePrediction")

    // check if current scale-out can fulfill the target runtime constraint
    val relativeSlackUp = 1.05
    val absoluteSlackUp = 0

    val relativeSlackDown = .85
    val absoluteSlackDown = 0

    if (remainingRuntimePrediction > remainingTargetRuntime * relativeSlackUp + absoluteSlackUp) {

      val nextScaleOutIndex = futureJobsRuntimes.findAll(_ < remainingTargetRuntime * .9)
        .sorted
        .headOption
        .getOrElse(argmin(futureJobsRuntimes))
      val nextScaleOut = predictedScaleOuts(nextScaleOutIndex)

      if (nextScaleOut != scaleOut) {
        logger.info(s"Adjusting scale-out to $nextScaleOut after job $nextJobId.")
        sparkContext.requestTotalExecutors(nextScaleOut, 0, Map[String,Int]())
        this.nextScaleOut = nextScaleOut
      }

    } else if (remainingRuntimePrediction < remainingTargetRuntime * relativeSlackDown - absoluteSlackDown) {

      val nextScaleOutIndex = futureJobsRuntimes.findAll(_ < remainingTargetRuntime * .9)
        .sorted
        .headOption
        .getOrElse(argmin(futureJobsRuntimes))
      val nextScaleOut = predictedScaleOuts(nextScaleOutIndex)

      if (nextScaleOut < scaleOut) {
        logger.info(s"Adjusting scale-out to $nextScaleOut after job $nextJobId.")
        sparkContext.requestTotalExecutors(nextScaleOut, 0, Map[String,Int]())
        this.nextScaleOut = nextScaleOut
      }

    } else {
      logger.debug(s"Scale-out is not changed.")
    }

  }
}
