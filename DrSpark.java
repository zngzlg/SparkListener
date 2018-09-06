package org.apache.spark.deploy.history

import java.io._
import java.net.{HttpURLConnection, URL}
import java.text.SimpleDateFormat
import java.util.Properties

import com.codahale.metrics.MetricRegistry
import com.tencent.tdw.DrRunner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.env.EnvironmentListener
import org.apache.spark.ui.exec._

import scala.collection.mutable.ListBuffer

class DrSpark {
  val logger: Logger = Logger.getLogger("TRACE")
  val _conf = new SparkConf()
  val MetricRegistry = new MetricRegistry
  val CounterMetric: com.codahale.metrics.Counter = MetricRegistry.counter("SparkCounter")


  def counterIncrement(): Unit = {
    CounterMetric.inc()
  }

  _conf.set("spark.ui.retainedDeadExecutors", "10000")
  _conf.set("spark.ui.timeline.executors.maximum", "100000")

  var _sparkProperties = new Properties()
  lazy val storageListener = new StorageStatusListener(_conf)
  lazy val executorsListener = new ExecutorsListener(storageListener, _conf)
  lazy val environmentListener = new EnvironmentListener()
  lazy val tdwListener = new TDWListener(_conf)

  def load(in: InputStream, sourceName: String): Unit = {
    val replayBus = new ReplayListenerBus()
    replayBus.addListener(environmentListener)
    replayBus.addListener(storageListener)
    replayBus.addListener(executorsListener)
    replayBus.addListener(tdwListener)
    replayBus.replay(in, sourceName, maybeTruncated = false)
  }

  def getEnvironmentData() = {
    environmentListener.systemProperties.foreach { case (name, value) =>
      _sparkProperties.setProperty(name, value)
    }
    environmentListener.sparkProperties.foreach { case (name, value) =>
      _sparkProperties.setProperty(name, value)
    }
    environmentListener.jvmInformation.foreach { case (name, value) =>
      _sparkProperties.setProperty(name, value)
    }
  }

  def getEventLog(appid: String) = {
    val conf = new Configuration()
    val _begin = System.currentTimeMillis
    val entity = DrRunner.loadJobContext(appid)
    val _fetchEnd = System.currentTimeMillis
    logger.info(s"fetch ${entity.HdfsSchema}${entity.JobId}, cost ${_fetchEnd - _begin}ms.")
    val logDir = entity.HdfsSchema + appid
    val _fs = FileSystem.get(conf)
    load(new BufferedInputStream(_fs.open(new Path(logDir))), appid)
    logger.info(s"load ${entity.JobId} cost ${System.currentTimeMillis - _fetchEnd}ms.")
  }

  def getExecutorSummary(appid: String): ListBuffer[TDWExecutorTaskSummary] = {
    getEventLog(appid)
    getEnvironmentData()
    val collector = new ListBuffer[TDWExecutorTaskSummary]
    for (i <- tdwListener.executorToTaskSummary) {
      collector += i._2
    }
    collector
  }

  def getExecutorStorage(appid: String) = {
    getEventLog(appid)
    getEnvironmentData()

    var collector: Map[String, Seq[TDWExecutorStorageStatus]] = Map()
    for (i <- tdwListener.executorToTaskSummary) {
      collector += (i._1 -> i._2.storageMetrics.storageStatus)
    }
    collector
  }

  def getExecutorShuffle(appid: String): Map[String, Seq[TDWExecutorTaskShuffleMetrics]] = {
    getEventLog(appid)
    getEnvironmentData()

    var collector: Map[String, Seq[TDWExecutorTaskShuffleMetrics]] = Map()
    for (i <- tdwListener.executorToTaskSummary) {
      collector += (i._1 -> i._2.taskList.map(l => l.shuffleMetrics))
    }
    collector
  }

  def getTaskListByExecutorId(appId: String, executorId: String): Option[ListBuffer[TDWTaskMetrics]] = {
    getEventLog(appId)
    getEnvironmentData()
    tdwListener.executorToTaskSummary.get(executorId).map {
      l => l.taskList
    }
  }

  def getStorageListByExecutorId(appId: String, executorId: String): Option[ListBuffer[TDWExecutorStorageStatus]] = {
    getEventLog(appId)
    getEnvironmentData()
    tdwListener.executorToTaskSummary.get(executorId).map {
      l => l.storageMetrics.storageStatus
    }
  }

  def getByExecutorId(appId: String, executorId: String): Option[TDWExecutorTaskSummary] = {
    getEventLog(appId)
    getEnvironmentData()
    tdwListener.executorToTaskSummary.get(executorId).map {
      l => l
    }
  }

  def getExecutorIO(appid: String): Map[String, Seq[TDWExecutorTaskIOMetrics]] = {
    getEventLog(appid)
    getEnvironmentData()

    var collector: Map[String, Seq[TDWExecutorTaskIOMetrics]] = Map()
    for (i <- tdwListener.executorToTaskSummary) {
      collector += (i._1 -> i._2.taskList.map(l => l.ioMetrics))
    }
    collector
  }

  def getExecutorTasks(appid: String) = {
    getEventLog(appid)
    getEnvironmentData()
    var collector: Map[String, Seq[TDWTaskMetrics]] = Map()
    for (i <- tdwListener.executorToTaskSummary) {
      collector += (i._1 -> i._2.taskList)
    }
    collector
  }

  private def getChart[T,P](title:String,url: URL,xAxis:Seq[T], data:Map[String,Seq[P]], multiYAxis:Boolean = true):String = {
    val con: HttpURLConnection = url.openConnection().asInstanceOf[HttpURLConnection]
    con.setRequestMethod("POST")
    con.setRequestProperty("User-Agent", "Mozilla/5.0")
    con.setRequestProperty("Accept-Language", "en-US,en;q=0.5")
    con.setRequestProperty("Content-Type", "application/json")
    con.setDoOutput(true)
    val df:SimpleDateFormat = new SimpleDateFormat("MM/dd HH:mm")
    val series = data.toIndexedSeq.map(l=>s"""{
                          "name": "${l._1}",
                          "type": "line",
                          "areaStyle": {
                              "normal":{}
                          },
                          "yAxisIndex" : "${if (multiYAxis) data.toIndexedSeq.indexOf(l) else 0}",
                          "data": [${l._2.mkString(",")}]
                      }""".stripMargin).mkString(",")

    val yAxis = if (multiYAxis) data.toIndexedSeq.map(l=>s"""{
                          "name":"${l._1}",
                          "type":"value"
                      }""".stripMargin).mkString(",") else s"""{"type":"value"}"""

    val param =
      s""" {
           "chartConfig":{
                      "title": {
                          "text": "${title}",
                          "x": "center"
                      },
                      "legend": {
                          "data": ["${data.keys.mkString(s"""","""")}"],
                          "x": "left"
                      },
                      "xAxis": {
                          "type": "category",
                          "data": ["${xAxis.map(l=>df.format(l)).mkString(s"""","""")}"]
                      },
                      "yAxis": [${yAxis}],
                      "series": [${series}]
                  },
                  "width": 800,
                  "height": 400,
                  "clipRect": { "top": 0, "left": 0, "width": 800, "height": 400 },
                  "responseType": "url"
                    }
      """.stripMargin
    val wr: DataOutputStream = new DataOutputStream(con.getOutputStream)
    wr.writeBytes(param)
    wr.flush()
    wr.close()

    val responseCode = con.getResponseCode
    logger.info("Sending 'POST' request to URL : " + url)
    logger.info("Response Code : " + responseCode)
    val in = new BufferedReader(new InputStreamReader(con.getInputStream))
    val inputLine = in.readLine()
    in.close()
    logger.info(inputLine)
    inputLine
  }

  def getExecutorChartService(appid: String, executorid: String): Map[String,String] = {
    getEventLog(appid)
    getEnvironmentData()
    val serviceStart = System.currentTimeMillis()
    val appTimeline = tdwListener.executorToTaskSummary.get(executorid).map {
      executor => executor.taskList.map(l => l.endTime)
    }.getOrElse(ListBuffer())
    val inputBytes: Seq[Long] = tdwListener.executorToTaskSummary.get(executorid).map {
      executor => executor.taskList.map(l => l.ioMetrics.inputBytes/1024)
    }.getOrElse(Seq())
    val outputBytes = tdwListener.executorToTaskSummary.get(executorid).map {
      executor => executor.taskList.map(l => l.ioMetrics.outputBytes/1024)
    }.getOrElse(Seq())
    val shuffleReads = tdwListener.executorToTaskSummary.get(executorid).map {
      executor => executor.taskList.map(l => l.shuffleMetrics.remoteBytesRead/1024)
    }.getOrElse(Seq())
    val shuffleWrites = tdwListener.executorToTaskSummary.get(executorid).map {
      executor => executor.taskList.map(l => l.shuffleMetrics.shuffleBytesWrite/1024)
    }.getOrElse(Seq())
    val cpuTime = tdwListener.executorToTaskSummary.get(executorid).map {
      executor => executor.taskList.map(l => l.executorCpuTime/1000000)
    }.getOrElse(Seq())
    val executorTime = tdwListener.executorToTaskSummary.get(executorid).map {
      executor => executor.taskList.map(l => l.executorRuntime)
    }.getOrElse(Seq())
    val storageTimeline = tdwListener.executorToTaskSummary.get(executorid).map {
      executor => executor.storageMetrics.storageStatus.map(l => l.time)
    }.getOrElse(Seq())
    val storageMem = tdwListener.executorToTaskSummary.get(executorid).map {
      executor => executor.storageMetrics.storageStatus.map(l => l.memUsed/1024)
    }.getOrElse(Seq())
    val storageDisk = tdwListener.executorToTaskSummary.get(executorid).map {
      executor => executor.storageMetrics.storageStatus.map(l => l.diskUsed/1024)
    }.getOrElse(Seq())
    val shuffleWait = tdwListener.executorToTaskSummary.get(executorid).map{
      executor => executor.taskList.map(l=>l.shuffleMetrics.shuffleWait)
    }.getOrElse(Seq())
    val resultSerTime = tdwListener.executorToTaskSummary.get(executorid).map{
      executor => executor.taskList.map(l=>l.ioMetrics.resultSerializationTime)
    }.getOrElse(Seq())
    val taskDeserTime = tdwListener.executorToTaskSummary.get(executorid).map{
      executor => executor.taskList.map(l=>l.ioMetrics.executorDeserializeTime)
    }.getOrElse(Seq())
    val gcTime = tdwListener.executorToTaskSummary.get(executorid).map{
      executor => executor.taskList.map(l=>l.jvmGCTime)
    }.getOrElse(Seq())
    val resultSize = tdwListener.executorToTaskSummary.get(executorid).map{
      executor => executor.taskList.map(l=>l.resultSize/1024)
    }.getOrElse(Seq())
    var charts = Map[String,String]()
    val url = new URL("http://render.oa.com/echarts")
    charts += ("ioChart" -> getChart[Long,Long]("ioChart",url,appTimeline,Map("InputBytes(kb)"->inputBytes,"OutputBytes(kb)"->outputBytes)))
    charts += ("shuffleChart" -> getChart[Long,Long]("shuffleChart",url,appTimeline,Map("shuffleReads(kb)"->shuffleReads,"shuffleWrites(kb)"->shuffleWrites)))
    charts += ("cpuChart" -> getChart[Long,Long]("cpuChart",url,appTimeline,Map("cpuTime(ms)"->cpuTime,"executionTime(ms)"->executorTime),false))
    charts += ("storageChart" -> getChart[Long,Long]("storageChart",url,storageTimeline,Map("storageMem(kb)"->storageMem,"storageDisk(kb)"->storageDisk),false))
    charts += ("serializationChart"-> getChart[Long,Long]("serializationChart",url,appTimeline,Map("resultSerialization(ms)"->resultSerTime,"taskDeserization(ms)"->taskDeserTime),false))
    charts += ("otherChart" -> getChart[Long,Long]("otherChart",url,appTimeline,Map("shuffleWait(ms)"->shuffleWait,"gc(ms)"->gcTime),false))
    charts += ("driverBurden" -> getChart[Long,Long]("driverBurden",url,appTimeline,Map("resultSize(kb)"->resultSize),false))
    logger.info(s"chart service cost ${System.currentTimeMillis() - serviceStart} ms")
    charts
  }

  def getApplicationSeriesService(appid:String):Map[String,Seq[Long]] = {
    getEventLog(appid)
    getEnvironmentData()
    val serviceStart = System.currentTimeMillis()
    val sortedRepo = tdwListener.executorToTaskSummary.toIndexedSeq.sortBy(l=>l._2.removedTime)
    val appTimeline = sortedRepo.map {
      executor => executor._2.removedTime
    }
    val inputBytes: Seq[Long] = sortedRepo.map {
      executor => executor._2.ioMetrics.inputBytes/1024
    }
    val outputBytes = sortedRepo.map {
      executor => executor._2.ioMetrics.outputBytes/1024
    }
    val shuffleReads = sortedRepo.map {
      executor => executor._2.shuffleMetrics.remoteBytesRead/1024
    }
    val shuffleWrites = sortedRepo.map {
      executor => executor._2.shuffleMetrics.shuffleBytesWrite/1024
    }
    val cpuTime = sortedRepo.map {
      executor => executor._2.executorCpuTime/1000000
    }
    val executorTime = sortedRepo.map {
      executor => executor._2.executorRuntime
    }
    val storageTimeline = sortedRepo.map {
      executor => executor._2.storageMetrics.removeTime
    }
    val storageMem = sortedRepo.map {
      executor => executor._2.storageMetrics.memUsed/1024
    }
    val storageDisk = sortedRepo.map {
      executor => executor._2.storageMetrics.diskUsed/1024
    }
    val shuffleWait = sortedRepo.map{
      executor => executor._2.shuffleMetrics.shuffleWait
    }
    val resultSerTime = sortedRepo.map{
      executor => executor._2.ioMetrics.resultSerializationTime
    }
    val taskDeserTime = sortedRepo.map{
      executor => executor._2.ioMetrics.executorDeserializeTime
    }
    val gcTime = sortedRepo.map{
      executor => executor._2.jvmGCTime
    }
    val memSpill = sortedRepo.map{
      executor => executor._2.ioMetrics.memoryBytesSpilled / 1024
    }
    val diskSpill = sortedRepo.map{
      executor => executor._2.ioMetrics.diskBytesSpilled / 1024
    }
    val resultSize = sortedRepo.map{
      executor => executor._2.resultSize / 1024
    }

    var charts = Map[String,Seq[Long]]()
    charts += "appTimeline" -> appTimeline
    charts += "storageTimeline" -> storageTimeline
    charts += "inputBytes" -> inputBytes
    charts += "outputBytes" -> outputBytes
    charts += "shuffleReads" -> shuffleReads
    charts += "shuffleWrites" -> shuffleWrites
    charts += "cpuTime" -> cpuTime
    charts += "executorTime" -> executorTime
    charts += "storageMem" ->storageMem
    charts += "storageDisk" -> storageDisk
    charts += "resultSerTime" -> resultSerTime
    charts += "taskDeserTime" -> taskDeserTime
    charts += "memorySpill" -> memSpill
    charts += "diskSpill" -> diskSpill
    charts += "resultSize"-> resultSize
    charts += "shuffleWait" -> shuffleWait
    charts += "gcTime" ->gcTime
    logger.info(s"chart service cost ${System.currentTimeMillis() - serviceStart} ms")
    charts
  }

  def getApplicationChartService(appid: String): Map[String,String] = {
    //getEventLog(appid)
    //getEnvironmentData()
    val serviceStart = System.currentTimeMillis()
    val sortedRepo = tdwListener.executorToTaskSummary.toIndexedSeq.sortBy(l=>l._2.removedTime)
    val appTimeline = sortedRepo.map {
      executor => executor._2.removedTime
    }
    val inputBytes: Seq[Long] = sortedRepo.map {
      executor => executor._2.ioMetrics.inputBytes/1024
    }
    val outputBytes = sortedRepo.map {
      executor => executor._2.ioMetrics.outputBytes/1024
    }
    val shuffleReads = sortedRepo.map {
      executor => executor._2.shuffleMetrics.remoteBytesRead/1024
    }
    val shuffleWrites = sortedRepo.map {
      executor => executor._2.shuffleMetrics.shuffleBytesWrite/1024
    }
    val cpuTime = sortedRepo.map {
      executor => executor._2.executorCpuTime/1000000
    }
    val executorTime = sortedRepo.map {
      executor => executor._2.executorRuntime
    }
    val storageTimeline = sortedRepo.map {
      executor => executor._2.storageMetrics.removeTime
    }
    val storageMem = sortedRepo.map {
      executor => executor._2.storageMetrics.memUsed/1024
    }
    val storageDisk = sortedRepo.map {
      executor => executor._2.storageMetrics.diskUsed/1024
    }
    val shuffleWait = sortedRepo.map{
      executor => executor._2.shuffleMetrics.shuffleWait
    }
    val resultSerTime = sortedRepo.map{
      executor => executor._2.ioMetrics.resultSerializationTime
    }
    val taskDeserTime = sortedRepo.map{
      executor => executor._2.ioMetrics.executorDeserializeTime
    }
    val gcTime = sortedRepo.map{
      executor => executor._2.jvmGCTime
    }
    val memSpill = sortedRepo.map{
      executor => executor._2.ioMetrics.memoryBytesSpilled / 1024
    }
    val diskSpill = sortedRepo.map{
      executor => executor._2.ioMetrics.diskBytesSpilled / 1024
    }
    val resultSize = sortedRepo.map{
      executor => executor._2.resultSize / 1024
    }
    var charts = Map[String,String]()
    val url = new URL("http://render.oa.com/echarts")
    charts += ("ioChart" -> getChart[Long,Long]("ioChart",url,appTimeline,Map("InputBytes(kb)"->inputBytes,"OutputBytes(kb)"->outputBytes)))
    charts += ("shuffleChart" -> getChart[Long,Long]("shuffleChart",url,appTimeline,Map("shuffleReads(kb)"->shuffleReads,"shuffleWrites(kb)"->shuffleWrites)))
    charts += ("cpuChart" -> getChart[Long,Long]("cpuChart",url,appTimeline,Map("cpuTime(ms)"->cpuTime,"executionTime(ms)"->executorTime),false))
    charts += ("storageChart" -> getChart[Long,Long]("storageChart",url,storageTimeline,Map("storageMem(kb)"->storageMem,"storageDisk(kb)"->storageDisk),false))
    charts += ("serializationChart"-> getChart[Long,Long]("serializationChart",url,appTimeline,Map("resultSerialization(ms)"->resultSerTime,"taskDeserization(ms)"->taskDeserTime),false))
    charts += ("spillChart" -> getChart[Long,Long]("spillChart",url,appTimeline,Map("memorySpill(kb)"->memSpill,"diskSpill(kb)"->diskSpill),false))
    charts += ("driverBurden" -> getChart[Long,Long]("driverBurden",url,appTimeline,Map("resultSize(kb)"->resultSize),false))
    charts += ("otherChart" -> getChart[Long,Long]("otherChart",url,appTimeline,Map("shuffleWait(ms)"->shuffleWait,"gc(ms)"->gcTime),false))
    logger.info(s"chart service cost ${System.currentTimeMillis() - serviceStart} ms")
    charts
  }
}

object DrSpark{

  def main(args: Array[String]): Unit = {
    //getEnvironmentData()
    val spakr = new DrSpark()
    val appid = "C:\\application_1534970549996_12041826.inprogress"
    //spakr.getEventLog(appid)
    val in = new FileInputStream(appid)
    spakr.load(in,"application_1531993477909_49619800")
    //val chart_url = spakr.getExecutorChartService(appid,"29")
    val chart_url = spakr.getApplicationChartService(appid)
    println(chart_url)
  }
}
