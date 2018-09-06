package org.apache.spark.ui.exec

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.spark.scheduler._
import org.apache.spark.storage._
import org.apache.spark._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** 序列化时不包含relation信息 **/
@JsonIgnoreProperties(Array("taskList"))
case class TDWExecutorTaskSummary(
  var executorId: String,
  var stageId: Int = 0,
  var totalCores: Int = 0,
  var tasksActive: Int = 0,
  var tasksFailed: Int = 0,
  var tasksComplete: Int = 0,
  var duration: Long = 0L,
  var jvmGCTime: Long = 0L,
  var resultSize: Long = 0L,
  var isAlive: Boolean = true,
  var executorLogs: Map[String, String] = Map.empty,
  var addedTime: Long = 0L, // Executor 加入时间
  var removedTime: Long = 0L, // Executor 移除时间
  var executorCpuTime: Long = 0L,
  var executorRuntime: Long = 0L,
  var ioMetrics: TDWExecutorTaskIOMetrics = TDWExecutorTaskIOMetrics(),
  var shuffleMetrics: TDWExecutorTaskShuffleMetrics = TDWExecutorTaskShuffleMetrics(),
  var storageMetrics: TDWExecutorBlockMetrics = TDWExecutorBlockMetrics(),
  @JsonProperty("taskList") var taskList: ListBuffer[TDWTaskMetrics] = ListBuffer[TDWTaskMetrics]()
)

/** 序列化时不包含relation信息 **/
@JsonIgnoreProperties(Array("executorInfo"))
case class TDWTaskMetrics(
  var taskId: Long,
  @JsonProperty("executorInfo") var executorInfo:TDWExecutorTaskSummary,
  var isAlive: Boolean = true,
  var startTime: Long = 0,
  var endTime: Long = 0,
  var duration: Long = 0,
  var executorCpuTime: Long = 0L,
  var executorRuntime: Long = 0L,
  var jvmGCTime: Long = 0L,
  var resultSize: Long = 0L,
  var failed: Boolean = false,
  var killed: Boolean = false,
  var failedReason: String = null,
  var traceStack: String = null,
  var ioMetrics: TDWExecutorTaskIOMetrics = TDWExecutorTaskIOMetrics(),
  var shuffleMetrics: TDWExecutorTaskShuffleMetrics = TDWExecutorTaskShuffleMetrics()
)
case class TDWExecutorTaskIOMetrics(
  var diskBytesSpilled: Long = 0L,
  var memoryBytesSpilled: Long = 0L,
  var resultSerializationTime: Long = 0L,
  var executorDeserializeTime: Long = 0L, // Executor 在做task反序列化时的耗时，也就是task的依赖准备阶段
  var inputBytes: Long = 0L,
  var inputRecords: Long = 0L,
  var outputBytes: Long = 0L,
  var outputRecords: Long = 0L
)
case class TDWExecutorTaskShuffleMetrics(
  var shuffleWait:Long = 0L,
  var remoteBlockFetched:Long =0L,
  var localBlockFetched: Long = 0L,
  var totalBlockFetched: Long = 0L,
  var remoteBytesRead: Long = 0L,
  var localBytesRead: Long = 0L,
  var totalBytesRead: Long = 0L,
  var recordRead: Long = 0L,
  var shuffleBytesWrite: Long = 0L,
  var shuffleRecordWrite: Long = 0L,
  var writeTime:Long = 0L
)
@JsonIgnoreProperties(Array("storageStatus"))
case class TDWExecutorBlockMetrics(
  var addTime:Long = 0,
  var removeTime:Long = 0,
  var maxMem: Long =0L,
  var isAlive: Boolean = true,
  var memUsed: Long = 0L,
  var diskUsed: Long = 0L,
  @JsonProperty("storageStatus") var storageStatus: ListBuffer[TDWExecutorStorageStatus] = ListBuffer[TDWExecutorStorageStatus]()
)
case class TDWExecutorStorageStatus(
  var time:Long = 0L,
  var memUsed: Long = 0L,
  var diskUsed: Long = 0L
)

/**序列化时不包含relation信息**/
@JsonIgnoreProperties(Array("blockManagerId","storageStatus"))
case class TDWExecutorStorageSummary(
  var executorId: String,
  var addTime: Long = 0,
  @JsonProperty("blockManagerId") var blockManagerId: BlockManagerId = null,
  @JsonProperty("storageStatus") var storageStatus: StorageStatus= null
)

class TDWListener(conf: SparkConf) extends SparkListener {

  val executorToTaskSummary = mutable.LinkedHashMap[String, TDWExecutorTaskSummary]()
  val executorToStorageSummary = mutable.LinkedHashMap[String, TDWExecutorStorageSummary]()
  var taskMetrics = mutable.LinkedHashMap[Long, TDWTaskMetrics]()
  var timeline:Long = -1L // app timeline

  /** 记录executor添加的时间, 用于计算executor资源使用量 **/
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    val eid = executorAdded.executorId
    val taskSummary = executorToTaskSummary.getOrElseUpdate(eid, TDWExecutorTaskSummary(eid))
    taskSummary.executorLogs = executorAdded.executorInfo.logUrlMap
    taskSummary.addedTime = executorAdded.time
    taskSummary.storageMetrics.addTime = executorAdded.time
    taskSummary.totalCores = executorAdded.executorInfo.totalCores
    updateTimeline(executorAdded.time)
  }

  /** 记录executor删除时间，用于计算executor资源使用量 **/
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = synchronized {
    executorToTaskSummary.get(executorRemoved.executorId).foreach(e => {
      e.isAlive = false
      e.removedTime = executorRemoved.time
      e.storageMetrics.removeTime = executorRemoved.time
    })
    updateTimeline(executorRemoved.time)
  }

  /** 记录task的生成时间，关联至executor汇总 **/
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val eid = taskStart.taskInfo.executorId
    val info = taskStart.taskInfo
    val executor = executorToTaskSummary.getOrElseUpdate(eid, TDWExecutorTaskSummary(eid))
    executor.tasksActive += 1
    val tm = taskMetrics.getOrElseUpdate(info.taskId,TDWTaskMetrics(info.taskId,executor))
    tm.startTime = info.launchTime
    executor.taskList += tm
    executor.stageId = taskStart.stageId
    updateTimeline(taskStart.taskInfo.launchTime)
  }

  /** 分别记录task产生的IO信息、Shuffle信息、Storage信息，并汇总至executor **/
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo

    if (info != null) {
      val eid = info.executorId

      val executor = executorToTaskSummary.getOrElseUpdate(eid, TDWExecutorTaskSummary(eid))
      val taskMetric = taskMetrics.getOrElseUpdate(info.taskId,TDWTaskMetrics(info.taskId,executor))

      taskEnd.reason match {
        case Resubmitted =>
          // Note: For resubmitted tasks, we continue to use the metrics that belong to the
          // first attempt of this task. This may not be 100% accurate because the first attempt
          // could have failed half-way through. The correct fix would be to keep track of the
          // metrics added by each attempt, but this is much more complicated.
          executor.tasksFailed += 1
          taskMetric.failed = true
          taskMetric.failedReason = Resubmitted.toErrorString


        case e: ExceptionFailure =>
          executor.tasksFailed += 1
          taskMetric.failed = true
          taskMetric.failedReason = e.description
          taskMetric.traceStack = e.toErrorString

        case e:ExecutorLostFailure =>
          executor.tasksFailed += 1
          taskMetric.failed = true
          taskMetric.failedReason = e.toErrorString

        case e:FetchFailed =>
          executor.tasksFailed += 1
          taskMetric.failed = true
          taskMetric.failedReason = e.toErrorString

        case e:TaskCommitDenied =>
          executor.tasksFailed += 1
          taskMetric.failed = true
          taskMetric.failedReason = e.toErrorString

        case TaskKilled =>
          taskMetric.killed =true
        case _ =>
          executor.tasksComplete += 1
      }
      if (executor.tasksActive >= 1) {
        executor.tasksActive -= 1
      }

      taskMetric.isAlive = false
      executor.duration += info.duration
      taskMetric.endTime = info.finishTime
      taskMetric.duration = info.duration

      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        taskMetric.executorCpuTime += metrics.executorCpuTime
        taskMetric.executorRuntime += metrics.executorRunTime
        taskMetric.jvmGCTime += metrics.jvmGCTime
        taskMetric.resultSize += metrics.resultSize
        taskMetric.ioMetrics.resultSerializationTime += metrics.resultSerializationTime
        taskMetric.ioMetrics.diskBytesSpilled += metrics.diskBytesSpilled
        taskMetric.ioMetrics.memoryBytesSpilled += metrics.memoryBytesSpilled
        taskMetric.ioMetrics.executorDeserializeTime += metrics.executorDeserializeTime
        taskMetric.ioMetrics.inputRecords += metrics.inputMetrics.recordsRead
        taskMetric.ioMetrics.inputBytes += metrics.inputMetrics.bytesRead
        taskMetric.ioMetrics.outputBytes += metrics.outputMetrics.bytesWritten
        taskMetric.ioMetrics.outputRecords += metrics.outputMetrics.recordsWritten
        taskMetric.shuffleMetrics.remoteBytesRead += metrics.shuffleReadMetrics.remoteBytesRead
        taskMetric.shuffleMetrics.localBytesRead += metrics.shuffleReadMetrics.localBytesRead
        taskMetric.shuffleMetrics.totalBytesRead += metrics.shuffleReadMetrics.totalBytesRead
        taskMetric.shuffleMetrics.totalBlockFetched += metrics.shuffleReadMetrics.totalBlocksFetched
        taskMetric.shuffleMetrics.remoteBlockFetched += metrics.shuffleReadMetrics.remoteBlocksFetched
        taskMetric.shuffleMetrics.localBlockFetched += metrics.shuffleReadMetrics.localBlocksFetched
        taskMetric.shuffleMetrics.shuffleBytesWrite += metrics.shuffleWriteMetrics.bytesWritten
        taskMetric.shuffleMetrics.shuffleWait += metrics.shuffleReadMetrics.fetchWaitTime
        taskMetric.shuffleMetrics.recordRead += metrics.shuffleReadMetrics.recordsRead
        taskMetric.shuffleMetrics.writeTime += metrics.shuffleWriteMetrics.writeTime

        executor.executorCpuTime += metrics.executorCpuTime
        executor.executorRuntime += metrics.executorRunTime
        executor.jvmGCTime += metrics.jvmGCTime
        executor.resultSize += metrics.resultSize
        executor.ioMetrics.resultSerializationTime += metrics.resultSerializationTime
        executor.ioMetrics.diskBytesSpilled += metrics.diskBytesSpilled
        executor.ioMetrics.memoryBytesSpilled += metrics.memoryBytesSpilled
        executor.ioMetrics.executorDeserializeTime += metrics.executorDeserializeTime
        executor.ioMetrics.inputRecords += metrics.inputMetrics.recordsRead
        executor.ioMetrics.inputBytes += metrics.inputMetrics.bytesRead
        executor.ioMetrics.outputBytes += metrics.outputMetrics.bytesWritten
        executor.ioMetrics.outputRecords += metrics.outputMetrics.recordsWritten
        executor.shuffleMetrics.remoteBytesRead += metrics.shuffleReadMetrics.remoteBytesRead
        executor.shuffleMetrics.localBytesRead += metrics.shuffleReadMetrics.localBytesRead
        executor.shuffleMetrics.totalBytesRead += metrics.shuffleReadMetrics.totalBytesRead
        executor.shuffleMetrics.totalBlockFetched += metrics.shuffleReadMetrics.totalBlocksFetched
        executor.shuffleMetrics.remoteBlockFetched += metrics.shuffleReadMetrics.remoteBlocksFetched
        executor.shuffleMetrics.localBlockFetched += metrics.shuffleReadMetrics.localBlocksFetched
        executor.shuffleMetrics.shuffleBytesWrite += metrics.shuffleWriteMetrics.bytesWritten
        executor.shuffleMetrics.shuffleWait += metrics.shuffleReadMetrics.fetchWaitTime
        executor.shuffleMetrics.recordRead += metrics.shuffleReadMetrics.recordsRead
        executor.shuffleMetrics.writeTime += metrics.shuffleWriteMetrics.writeTime

        if (metrics.updatedBlockStatuses.nonEmpty) {
          updateStorageStatus(taskEnd.taskInfo.finishTime, executor,eid, metrics.updatedBlockStatuses)
        }
      }
      updateTimeline(taskEnd.taskInfo.finishTime)
    }
  }
  private def updateTimeline(time:Long): Unit ={
    if (time > timeline) {
      timeline = time
    }
  }
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    applicationStart.driverLogs.foreach { logs =>
      val storageStatus = executorToStorageSummary.find { s =>
        s._2.blockManagerId.executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER ||
          s._2.blockManagerId.executorId == SparkContext.DRIVER_IDENTIFIER
      }
      storageStatus.foreach { s =>
        val eid = s._2.blockManagerId.executorId
        val taskSummary = executorToTaskSummary.getOrElseUpdate(eid, TDWExecutorTaskSummary(eid))
        taskSummary.executorLogs = logs.toMap
        taskSummary.addedTime = applicationStart.time
      }
    }

    updateTimeline(applicationStart.time)
  }
  /** 防止application结束时，丢失event log **/
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = synchronized {
    executorToTaskSummary.filter(m => m._2.removedTime == 0).foreach{ l =>
      l._2.removedTime = applicationEnd.time
      l._2.isAlive = false
      l._2.storageMetrics.isAlive = false
      l._2.storageMetrics.removeTime = applicationEnd.time
    }
    updateTimeline(applicationEnd.time)
  }

  /** executor产生时会一并产生blockManager，driver也会产生blockManager，用于关联executor **/
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded):Unit = synchronized {
    val blockManagerId = blockManagerAdded.blockManagerId
    val executorId = blockManagerId.executorId
    val maxMem = blockManagerAdded.maxMem
    val storageStatus = new StorageStatus(blockManagerId, maxMem)

    val storage = executorToStorageSummary.getOrElseUpdate(executorId,TDWExecutorStorageSummary(executorId))
    val executor = executorToTaskSummary.getOrElseUpdate(executorId,TDWExecutorTaskSummary(executorId))

    storage.storageStatus = storageStatus
    storage.addTime = blockManagerAdded.time
    storage.blockManagerId = blockManagerId

    if (executor.addedTime == 0){
      executor.addedTime = blockManagerAdded.time
    }
    executor.storageMetrics.maxMem = storage.storageStatus.maxMem
    executor.storageMetrics.addTime = storage.addTime
    executor.storageMetrics.isAlive = true
    updateTimeline(blockManagerAdded.time)
  }

  /** blockManager结束时 **/
  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = synchronized {
    val blockManagerId = blockManagerRemoved.blockManagerId
    val executorId = blockManagerId.executorId
    executorToTaskSummary.get(executorId).foreach { l=>
        l.storageMetrics.removeTime = blockManagerRemoved.time
        l.storageMetrics.isAlive = false
    }
    updateTimeline(blockManagerRemoved.time)
  }

  /** block更新时，汇总至executor **/
  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
    val eid = blockUpdated.blockUpdatedInfo.blockManagerId.executorId
    executorToTaskSummary.get(eid).foreach { l =>
      val blockId = blockUpdated.blockUpdatedInfo.blockId
      val blockStatus = BlockStatus(
        blockUpdated.blockUpdatedInfo.storageLevel,
        blockUpdated.blockUpdatedInfo.memSize,
        blockUpdated.blockUpdatedInfo.diskSize
      )
      updateStorageStatus(timeline, l, eid, Seq((blockId, blockStatus)))
    }
  }

  /** 显式释放缓存的rdd 时，移除所有block，更新status，并汇总至executor **/
  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = synchronized {
    val unpersistedRDDId = unpersistRDD.rddId
    for (storage <- executorToStorageSummary) {
      storage._2.storageStatus.rddBlocksById(unpersistedRDDId).foreach {
        case (blockId, blockStatus) => {
          executorToTaskSummary.get(storage._2.executorId).foreach {
            executor => updateStorageStatus(timeline, executor,storage._2,Seq((blockId, blockStatus)))
          }
        }
      }
    }
  }

  /** Update storage status list to reflect updated block statuses */
  private def updateStorageStatus(timeline:Long, taskSummary:TDWExecutorTaskSummary, executorId:String, updatedBlocks: Seq[(BlockId, BlockStatus)]) {
    executorToStorageSummary.get(executorId).foreach { storageSummary =>
      updatedBlocks.foreach { case (blockId, updatedStatus) =>
        if (updatedStatus.storageLevel == StorageLevel.NONE) {
          storageSummary.storageStatus.removeBlock(blockId)
        } else {
          storageSummary.storageStatus.updateBlock(blockId, updatedStatus)
        }
      }
      taskSummary.storageMetrics.memUsed = storageSummary.storageStatus.memUsed
      taskSummary.storageMetrics.diskUsed = storageSummary.storageStatus.diskUsed
      taskSummary.storageMetrics.storageStatus += TDWExecutorStorageStatus(
        timeline,
        storageSummary.storageStatus.memUsed,
        storageSummary.storageStatus.diskUsed
      )
    }
  }

  /** Update storage status list to reflect updated block statuses */
  private def updateStorageStatus(timeline:Long, taskSummary:TDWExecutorTaskSummary, storageSummary:TDWExecutorStorageSummary, updatedBlocks: Seq[(BlockId, BlockStatus)]) {
    updatedBlocks.foreach { case (blockId, updatedStatus) =>
      if (updatedStatus.storageLevel == StorageLevel.NONE) {
        storageSummary.storageStatus.removeBlock(blockId)
      } else {
        storageSummary.storageStatus.updateBlock(blockId, updatedStatus)
      }
    }
    taskSummary.storageMetrics.memUsed = storageSummary.storageStatus.memUsed
    taskSummary.storageMetrics.diskUsed = storageSummary.storageStatus.diskUsed
    taskSummary.storageMetrics.storageStatus += TDWExecutorStorageStatus(
      timeline,
      storageSummary.storageStatus.memUsed,
      storageSummary.storageStatus.diskUsed
    )
  }
}
