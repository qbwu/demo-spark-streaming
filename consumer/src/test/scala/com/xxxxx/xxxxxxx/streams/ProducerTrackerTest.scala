/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @author qb.wu@outlook.com
 * @date 2017/7/3
 */

package com.xxxxx.xxxxxxx.streams

import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

final class ProducerTrackerTest extends StreamsAlgo {
    private val logger = LoggerFactory.getLogger(this.getClass)
    val output = System.getProperty("output.hdfs.path")
    override def name = "producer-tracker-test"
    override def resourceDS = "streams/empty"
    override def resultDS = "ap_event_stream"
    override def registerKryoClasses = Array()
    override def transformation(date : String, dstr : DStream[(Array[Byte], String)] ) {
        var batchNo = 0
        var bv : Broadcast[Array[Long]] = null
        var acc : LongAccumulator  = null
        dstr.foreachRDD { rdd => {
                logger.info(s"Sleeps for 1 minutes in the batch ${batchNo}")
                Thread.sleep(1 * 1000 * 60)
                bv = ProducerTrackerTest.bv
                acc = ProducerTrackerTest.acc
                if (batchNo == 3) {
                    logger.info(s"Throws exception in the batch ${batchNo}")
                    throw new java.io.IOException
                }
                rdd.mapPartitions{
                    _.map{ _._2 } ++ (bv.value.asInstanceOf[Array[Long]].map{
                        _.toString
                    }.toIterator)
                }.map { x =>
                    acc.add(batchNo)
                    x
                }.saveAsTextFile(f"${output}/${date}/${batchNo}%0,2d00/data")
                batchNo += 1
            }
        }
        logger.info("after transformation")
        ProducerTrackerTest.delta += 10
    }
}

object ProducerTrackerTest {
    import Contexts._
    CtxVarManager.registBroadcastVar("bv", () => {
                (1l to 10l).map{ _ + delta }.toArray
                bv
            }
        )
    def bv =
        CtxVarManager.getBroadcastVar("bv").get.asInstanceOf[Broadcast[Array[Long]]]
    def acc =
        CtxVarManager.getLongAccumulator("acc")
    var delta = 0
}

// vim: set ts=4 sw=4 et:
