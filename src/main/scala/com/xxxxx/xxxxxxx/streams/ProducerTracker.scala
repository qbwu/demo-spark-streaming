/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @author qb.wu@outlook.com
 * @date 2017/4/27
 */

package com.xxxxx.xxxxxxx.streams

import java.text.SimpleDateFormat
import java.util.{ Date, Calendar }
import scala.collection.JavaConversions._
import org.slf4j.{ Logger, LoggerFactory }
import org.apache.spark.streaming.scheduler._

final case class ProducerTrackerInitException(msg : String = "",
      cause : Throwable = null) extends Exception(msg, cause)

// Potential contention in the LiveListenerBus's thread and the main thread
final class ProducerTracker(processorDS : String,
                            producerDS : String,
                            notifier : Notifier)
                            extends StreamingListener {

    val processorPath = MetaClient.rawPath(processorDS)
    private[this] val logger = LoggerFactory.getLogger(this.getClass)
    private[this] val dateFormater = new SimpleDateFormat("yyyyMMdd")
    private[this] var batchNum = 0
    private[this] var commitTime = 0l
    // In fact we don't need the volatile anotations if main threads and onBatchCompleted do not
    // read them concurrently
    @volatile private[this] var hadoopCli : HadoopClient = null
    // trace is the current processing date
    @volatile private[this] var trace : String = null
    @volatile private[this] var hasError = false

    if (processorPath == null || processorPath.isEmpty) {
        throw new ProducerTrackerInitException(s"None of ${processorDS} data path")
    }

    (upstreamsSyncPoint(producerDS), getMeta("commit")) match {
        case (prod, proc) =>
            logger.info(s"Streams meta: ${prod}(up) <-- (down)${proc}")
            if (proc.isEmpty) {
                trace = nextDay(prod)
            } else {
                val commitR = "([0-9]{8})/([0-9]{2})00/([0-9]*)".r
                proc match {
                    case commitR(date, _, "0") =>
                        trace = date
                        // batchNum and commitTime reserve 0 to start from batch-00000 of some date
                    case commitR(date, i, ms) =>
                        trace = date
                        batchNum = i.toInt + 1
                        commitTime = ms.toLong
                    case _ =>
                        throw new ProducerTrackerInitException(
                            "Invalid commit point of ${processorDS}@meta")
                }
            }
    }

    if (trace == null || trace.isEmpty) {
        throw new ProducerTrackerInitException(s"None of ${producerDS} version to trace")
    }

    // should be call before starting the streamingListener
    def setHadoopClient(generator : () => HadoopClient) {
        if (hadoopCli != null) {
            return
        }
        synchronized {
            if (hadoopCli == null) {
                hadoopCli = generator()
            }
        }
    }

    def detectFailure = { hasError }

    def current = { trace }

    private def mvReadyFlag = hadoopCli.rename(processorPath) _
    private def updateMeta = MetaClient.update(processorDS) _
    private def getMeta = MetaClient.get(processorDS) _
    private def setMeta = MetaClient.set(processorDS) _

    // NotThreadSafe
    private def nextDay(d : String) : String = {
        val dt = dateFormater.parse(d)
        val c = Calendar.getInstance()
        c.setTime(dt)
        c.add(Calendar.DATE, 1)
        dateFormater.format(c.getTime())
    }

    private def upstreamsSyncPoint(upstreamsDS : String) : String = {
        // return "" if any producerDS's latest is empty
        upstreamsDS.split(",")
                   .filter{ ! _.isEmpty }
                   .map{ MetaClient.latest(_) }
                   .min
    }

    private def hasNotified = notifier.state
    private def doNotify { notifier.awake }

    private def switchDate(batchCompleted : StreamingListenerBatchCompleted) {
        val batchTime = batchCompleted.batchInfo.batchTime.milliseconds
        // try not to fail the batch due to the StreamingListener,
        // the date switching will delay in the result of excetption.
        try {
            logger.warn(s"Batch[${batchTime}]: Finish streaming@${trace}")
            trace = nextDay(trace)
            batchNum = 0
            doNotify
        } catch {
            case e : Exception =>
                logger.error(
                    s"Batch[${batchTime}]: Caught exceptions in the switchDate")
                e.printStackTrace
        }
    }

    private def commitOrAbort(batchCompleted : StreamingListenerBatchCompleted) {
        val batchTime = batchCompleted.batchInfo.batchTime.milliseconds
        // fail at once when batch failures detected.
        try {
            val numFailedOutputOp =
                batchCompleted.batchInfo.outputOperationInfos.values.count{ _.failureReason.nonEmpty }
            if (numFailedOutputOp == 0) {
                logger.info(s"Batch[${batchTime}]: Finish batch#${batchNum}")
                val latest = f"${trace}/${batchNum}%0,2d00"

                if (mvReadyFlag(s"${latest}/data/_SUCCESS", s"${latest}/@ready")) {
                    setMeta("commit", s"${latest}/${batchTime}")
                    updateMeta(latest)

                    commitTime = batchTime
                    batchNum += 1
                    // succeed in commiting
                    return
                }
            } else {
                logger.error(s"Batch[${batchTime}]: Oh, Batch#${batchNum} failed! :(")
            }
        } catch {
            case e : Exception =>
                logger.error(
                  s"Batch[${batchTime}]: Caught exceptions in the StreamingListener::onBatchCompleted")
                e.printStackTrace
        }
        // fail to commit
        hasError = true
        doNotify
    }

    // only be called from the listenerThread in LiveListenerBus
    override def onBatchCompleted(batchCompleted : StreamingListenerBatchCompleted) {
        val batchTime = batchCompleted.batchInfo.batchTime.milliseconds
        logger.info(s"batchTime=${batchTime}, onBatchCompletedTime=${System.currentTimeMillis}")

        if (hasNotified) {
            logger.info(s"Batch[${batchTime}]: hasNotified before")
            return
        }

        if (batchTime <= commitTime) {
            logger.warn(
                s"Batch[${batchTime}]: redo finished batches in recovery, inconsistent state is possible")
            return
        }

        def fallBehind = upstreamsSyncPoint(producerDS) >= trace
        val batchNumRecs = batchCompleted.batchInfo.numRecords

        // TODO: there may be some bugs in the DirectKafkaInputDStream's restore(), the jobs
        // seem not to report the input info to the inputInfoTracker in JobScheduler, making the
        // numberRecords of pending jobs and rescheduling jobs keep to be 0.
        if (batchNumRecs == 0 && fallBehind) {
            switchDate(batchCompleted)
        } else {
            commitOrAbort(batchCompleted)
            if (batchNumRecs == 0) {
                logger.warn(s"Batch[${batchTime}]: batch probably delays")
            }
        }
    }
}

// vim: set ts=4 sw=4 et:
