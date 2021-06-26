package de.tuberlin.cit.adjustments

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler._
import org.json4s.DefaultFormats
import org.json4s.native.{Json, Serialization}
import sttp.client3._
import sttp.client3.json4s._
import org.apache.log4j.Logger
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.RDDInfo

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.util.Try


class EnelScaleOutListener(sparkContext: SparkContext, sparkConf: SparkConf) extends SparkListener {

  private val logger: Logger = Logger.getLogger(classOf[EnelScaleOutListener])
  logger.info("Initializing Enel listener")
  private val applicationId: String = sparkConf.getAppId
  private val applicationSignature: String = sparkConf.get("spark.app.name")
  checkConfigurations()
  private val restTimeout: Int = sparkConf.get("spark.customExtraListener.restTimeout").toInt
  private val service: String = sparkConf.get("spark.customExtraListener.service")
  private val port: Int = sparkConf.get("spark.customExtraListener.port").toInt
  private val onlineScaleOutPredictionEndpoint: String = sparkConf.get("spark.customExtraListener.onlineScaleOutPredictionEndpoint")
  private val updateInformationEndpoint: String = sparkConf.get("spark.customExtraListener.updateInformationEndpoint")
  private val applicationExecutionId: String = sparkConf.get("spark.customExtraListener.applicationExecutionId")
  private val isAdaptive: Boolean = sparkConf.getBoolean("spark.customExtraListener.isAdaptive", defaultValue = true)
  private val method: String = sparkConf.get("spark.customExtraListener.method")

  private var jobId: Int = _

  private var desiredScaleOut: Int = _
  private var currentScaleOut: Int = _

  private var reconfigurationRunning: Boolean = false

  private val infoMap: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, String]] =
    scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, String]]()

  // scale-out, time of measurement, total time
  private var scaleOutBuffer: ListBuffer[(Int, Long, Long)] = ListBuffer()

  private val initialExecutors: Int = sparkConf.get("spark.customExtraListener.initialExecutors").toInt
  logger.info(s"Using initial scale-out of $initialExecutors.")

  // for json4s
  implicit val serialization: Serialization.type = org.json4s.native.Serialization
  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def getExecutorCount: Int = {
    sparkContext.getExecutorMemoryStatus.toSeq.length - 1
  }

  def proceed(): Boolean = {
    !isAdaptive || (isAdaptive && method.equals("enel"))
  }

  def saveDivision(a: Int, b: Int): Double = {
    Try(a / b).getOrElse(0).toDouble
  }

  def saveDivision(a: Long, b: Long): Double = {
    Try(a / b).getOrElse(0L).toDouble
  }

  def saveDivision(a: Double, b: Double): Double = {
    Try(a / b).getOrElse(0.0)
  }

  def checkConfigurations(){
    /**
     * check parameters are set in environment
     */
    val parametersList = List("spark.customExtraListener.restTimeout", "spark.customExtraListener.service",
      "spark.customExtraListener.initialExecutors", "spark.customExtraListener.port", "spark.customExtraListener.onlineScaleOutPredictionEndpoint",
      "spark.customExtraListener.updateInformationEndpoint", "spark.customExtraListener.applicationExecutionId", "spark.customExtraListener.method")
    logger.info("Current spark conf" + sparkConf.toDebugString)
    for (param <- parametersList) {
      if (!sparkConf.contains(param)) {
        throw new IllegalArgumentException("parameter " + param + " is not shown in the Environment!")
      }
    }
  }

  def updateInformation(applicationId: Option[String], updateMap: Map[String, String], updateEvent: String): Unit = {
    case class RequestPayload(application_execution_id: String,
                              application_id: Option[String],
                              job_id: Option[Int],
                              update_event: String,
                              updates: String)

    val backend = HttpURLConnectionBackend()

    try {
      val payload: RequestPayload = RequestPayload(
        applicationExecutionId,
        applicationId,
        null,
        updateEvent,
        Json(DefaultFormats).write(updateMap))

      basicRequest
        .post(uri"http://$service:$port/$updateInformationEndpoint")
        .contentType("application/json")
        .body(payload)
        .response(ignore)
        .send(backend)
    } finally backend.close()
  }

  def computeRescalingTimeRatio(startTime: Long, endTime: Long, startScaleOut: Int, endScaleOut: Int): Double = {

    val dividend: Long = scaleOutBuffer
      .filter(e => e._1 != startScaleOut || e._1 != endScaleOut)
      .filter(e => e._2 + e._3 >= startTime && e._2 <= endTime)
      .map(e => {
        val startTimeScaleOut: Long = e._2
        var endTimeScaleOut: Long = e._2 + e._3
        if (e._3 == 0L)
          endTimeScaleOut = endTime

        val intervalStartTime: Long = Math.min(Math.max(startTime, startTimeScaleOut), endTime)
        val intervalEndTime: Long = Math.max(startTime, Math.min(endTime, endTimeScaleOut))

        intervalEndTime - intervalStartTime
      })
      .sum

    saveDivision(dividend, endTime - startTime)
  }

  def extractFromRDD(seq: Seq[RDDInfo]): (Int, Int, Long, Long) = {
    var numPartitions: Int = 0
    var numCachedPartitions: Int = 0
    var memSize: Long = 0L
    var diskSize: Long = 0L
    seq.foreach(rdd => {
      numPartitions += rdd.numPartitions
      numCachedPartitions += rdd.numCachedPartitions
      memSize += rdd.memSize
      diskSize += rdd.diskSize
    })
    Tuple4(numPartitions, numCachedPartitions, memSize, diskSize)
  }

  def extractFromTaskMetrics(taskMetrics: TaskMetrics): (Double, Double, Double, Double, Double) =  {
    // cpu time is nanoseconds, run time is milliseconds
    val cpuUtilization: Double = saveDivision(taskMetrics.executorCpuTime, (taskMetrics.executorRunTime * 1000000))
    val gcTimeRatio: Double = saveDivision(taskMetrics.jvmGCTime, taskMetrics.executorRunTime)
    val shuffleReadWriteRatio: Double = saveDivision(taskMetrics.shuffleReadMetrics.totalBytesRead,
      taskMetrics.shuffleWriteMetrics.bytesWritten)
    val inputOutputRatio: Double = saveDivision(taskMetrics.inputMetrics.bytesRead, taskMetrics.outputMetrics.bytesWritten)
    val memorySpillRatio: Double = saveDivision(taskMetrics.diskBytesSpilled, taskMetrics.peakExecutionMemory)
    Tuple5(cpuUtilization, gcTimeRatio, shuffleReadWriteRatio, inputOutputRatio, memorySpillRatio)
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {

    if(!proceed())
      ()

    handleScaleOutMonitoring(executorAdded.time)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {

    if(!proceed())
      ()

    handleScaleOutMonitoring(executorRemoved.time)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {

    if(!proceed())
      ()

    logger.info(s"Application ${applicationStart.appId} started.")

    val updateMap: Map[String, String] = Map(
      "application_id" -> applicationStart.appId.orNull,
      "application_signature" -> applicationSignature,
      "attempt_id" -> applicationStart.appAttemptId.orNull,
      "start_time" -> applicationStart.time.toString
    )
    updateInformation(applicationStart.appId, updateMap, "APPLICATION_START")

  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {

    if(!proceed())
      ()

    logger.info(s"Application ${applicationId} finished.")

    val updateMap: Map[String, String] = Map(
      "end_time" -> applicationEnd.time.toString,
      "end_scale_out" -> getExecutorCount.toString
    )
    updateInformation(Option(applicationId), updateMap, "APPLICATION_END")

  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

    if(!proceed())
      ()

    logger.info(s"Job ${jobStart.jobId} started.")
    jobId = jobStart.jobId

    val mapKey: String = f"${applicationId}-${jobId}"

    infoMap(mapKey) = scala.collection.mutable.Map[String, String](
      "job_id" -> jobStart.jobId.toString,
      "start_time" -> jobStart.time.toString,
      "start_scale_out" -> getExecutorCount.toString,
      "stages" -> jobStart.stageInfos.map(si => f"${si.stageId}").mkString(",")
    )
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {

    if(!proceed())
      ()

    logger.info(s"Job ${jobEnd.jobId} finished.")

    val mapKey: String = f"${applicationId}-${jobId}"

    val endScaleOut = getExecutorCount
    val rescalingTimeRatio: Double = computeRescalingTimeRatio(
      infoMap(mapKey)("start_time").toLong,
      jobEnd.time,
      infoMap(mapKey)("start_scale_out").toInt,
      endScaleOut
    )

    infoMap(mapKey) = infoMap(mapKey).++(scala.collection.mutable.Map[String, String](
      "end_time" -> jobEnd.time.toString,
      "end_scale_out" -> endScaleOut.toString,
      "rescaling_time_ratio" -> rescalingTimeRatio.toString,
      "stages" -> Json(DefaultFormats).write(
        infoMap(mapKey)("stages").split(",")
        .map(si => f"${si}" ->  infoMap(f"${applicationId}-${jobId}-${si}"))
      )
    ))

    // remove all already "finished" transitions
    scaleOutBuffer = scaleOutBuffer.filter(_._3 == 0L)

    handleUpdateScaleOut()
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {

    if(!proceed())
      ()

    logger.info(s"Stage ${stageSubmitted.stageInfo.stageId} submitted.")

    val stageInfo: StageInfo = stageSubmitted.stageInfo
    val rddInfo: (Int, Int, Long, Long) = extractFromRDD(stageInfo.rddInfos)

    val mapKey: String = f"${applicationId}-${jobId}-${stageInfo.stageId}"
    infoMap(mapKey) = scala.collection.mutable.Map[String, String](
      "start_time" -> stageInfo.submissionTime.toString,
      "start_scale_out" -> getExecutorCount.toString,
      "stage_id" -> f"${stageInfo.stageId}",
      "stage_name" -> stageInfo.name,
      "parent_stage_ids" -> stageInfo.parentIds.map(psi => f"${psi}").toString(),
      "num_tasks" -> stageInfo.numTasks.toString,
      "rdd_num_partitions" -> rddInfo._1.toString,
      "rdd_num_cached_partitions" -> rddInfo._2.toString,
      "rdd_mem_size" -> rddInfo._3.toString,
      "rdd_disk_size" -> rddInfo._4.toString
    )
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {

    if(!proceed())
      ()

    logger.info(s"Stage ${stageCompleted.stageInfo.stageId} completed.")

    val stageInfo: StageInfo = stageCompleted.stageInfo
    val metricsInfo: (Double, Double, Double, Double, Double) = extractFromTaskMetrics(stageInfo.taskMetrics)

    val mapKey: String = f"${applicationId}-${jobId}-${stageInfo.stageId}"

    val endScaleOut = getExecutorCount
    val rescalingTimeRatio: Double = computeRescalingTimeRatio(
      stageInfo.submissionTime.getOrElse(0L),
      stageInfo.completionTime.getOrElse(0L),
      infoMap(mapKey)("start_scale_out").toInt,
      endScaleOut
    )

    infoMap(mapKey) = infoMap(mapKey).++(scala.collection.mutable.Map[String, String](
      "attempt_id" -> stageInfo.attemptNumber().toString,
      "end_time" -> stageInfo.completionTime.toString,
      "end_scale_out" -> endScaleOut.toString,
      "rescaling_time_ratio" -> rescalingTimeRatio.toString,
      "failure_reason" -> stageInfo.failureReason.getOrElse(""),
      "metrics" -> Json(DefaultFormats).write(scala.collection.mutable.Map[String, Double](
        "cpu_utilization" -> metricsInfo._1,
        "gc_time_ratio" -> metricsInfo._2,
        "shuffle_rw_ratio" -> metricsInfo._3,
        "data_io_ratio" -> metricsInfo._4,
        "memory_spill_ratio" -> metricsInfo._5
      ))
    ))
  }

  def handleUpdateScaleOut(): Unit = {

    case class ResponsePayload(best_scale_out: Int,
                               best_predicted_runtime: Long,
                               do_rescale: Boolean)

    case class RequestPayload(application_execution_id: String,
                              application_id: String,
                              job_id: Int,
                              update_event: String,
                              updates: String,
                              predict: Boolean)

    val backend = HttpURLConnectionBackend()

    val mapKey: String = f"${applicationId}-${jobId}"
    try {
      val payload: RequestPayload = RequestPayload(
        applicationExecutionId,
        applicationId,
        jobId,
        "JOB_END",
        Json(DefaultFormats).write(infoMap(mapKey)),
        isAdaptive && method.equals("enel") && !reconfigurationRunning)

      val response = basicRequest
        .post(uri"http://$service:$port/$onlineScaleOutPredictionEndpoint")
        .contentType("application/json")
        .body(payload)
        .readTimeout(restTimeout.seconds)
        .response(asJson[ResponsePayload])
        .send(backend)

      val bestScaleOut: Int = response.body.right.get.best_scale_out
      val doRescale: Boolean = response.body.right.get.do_rescale

      if(doRescale && bestScaleOut != currentScaleOut){
        reconfigurationRunning = true

        logger.info(s"Adjusting scale-out from $currentScaleOut to $bestScaleOut.")
        desiredScaleOut = bestScaleOut
        sparkContext.requestTotalExecutors(desiredScaleOut, 0, Map[String,Int]())
      }
      else {
        logger.info(s"Scale-out is not changed.")
      }
    } finally backend.close()
  }

  def handleScaleOutMonitoring(executorActionTime: Long): Unit = {
    currentScaleOut = getExecutorCount
    logger.info(s"Current number of executors: $currentScaleOut.")

    if(scaleOutBuffer.nonEmpty){
      val lastElement: (Int, Long, Long) = scaleOutBuffer.last
      scaleOutBuffer(scaleOutBuffer.size - 1) = (lastElement._1, lastElement._2, executorActionTime - lastElement._2)
    }

    scaleOutBuffer.append((currentScaleOut, executorActionTime, 0L))

    if(reconfigurationRunning){
      if(currentScaleOut == desiredScaleOut){
        reconfigurationRunning = false
      }
    }
  }
}
