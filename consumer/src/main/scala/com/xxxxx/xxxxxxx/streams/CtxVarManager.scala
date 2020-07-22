/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @author qb.wu@outlook.com
 * @date 2017/7/23
 */

package com.xxxxx.xxxxxxx.streams

import java.util.concurrent.ConcurrentHashMap
import org.slf4j.{ Logger, LoggerFactory }
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{ SparkListener, SparkListenerApplicationEnd }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.util.{ LongAccumulator, AccumulatorV2 }
import org.apache.spark.broadcast.Broadcast

// Potential contention in LiveListenerBus's thread and all the batch jobs' threads
// which get broadcast variables or accumulators from CtxVarManager.
// However we think there is no contention at present, because:
//      1) only the onApplicationEnd was implemented
//      2) "spark.streaming.concurrentJobs"="1" was set
object CtxVarManager extends SparkListener {
    private val logger = LoggerFactory.getLogger(this.getClass)
    type BvGenerator = (SparkContext) => Broadcast[_]
    private case class LazyBroadcastVar(val generator : BvGenerator) {
        @volatile var data : Option[Broadcast[_]] = None
    }

    private val bvStore = new ConcurrentHashMap[String, LazyBroadcastVar]
    private val accumStore = new ConcurrentHashMap[String, AccumulatorV2[_, _]]

    def broadcastVarNames = bvStore.keySet.toArray.map{ _.toString }
    def accumulatorNames = accumStore.keySet.toArray.map{ _.toString }
    // never call the getBroadcastVar in the gernerator
    def registBroadcastVar(name : String, generator : () => _) : Boolean = {
        logger.info(s"regist ${name}")
        bvStore.putIfAbsent(name, new LazyBroadcastVar(sc => sc.broadcast(generator()))) == null
    }

    def getBroadcastVar(name : String)(implicit sc : SparkContext) : Option[Broadcast[_]] = {
        val bv = bvStore.get(name)
        if (bv == null) {
            None
        } else {
            var ret = bv.data
            if (ret == None) {
                bv.synchronized {
                    ret = bv.data
                    if (ret == None) {
                        logger.info(s"before the broadcast of Broadcast:${name}")
                        ret = Some(bv.generator(sc))
                        bv.data = ret
                    }
                }
            }
            ret
        }
    }

    def resetBroadcastVar(name : String) : Option[Broadcast[_]] = {
        val bv = bvStore.get(name)
        if (bv == null) {
            None
        } else {
            var ret = bv.data
            if (ret != None) {
                bv.synchronized {
                    ret = bv.data
                    if (ret != None) {
                        logger.info(s"before the reset of Broadcast:${name}")
                        bv.data = None
                    }
                }
            }
            ret
        }
    }

    def getLongAccumulator(name : String)(implicit sc : SparkContext) : LongAccumulator = {
        var accum = accumStore.get(name)
        if (accum == null) {
            // coarse grained synchronization cos the much cheaper cost for creating accumulator
            // than broadcast variables
            accumStore.synchronized {
                accum = accumStore.get(name)
                if (accum == null) {
                    logger.info(s"before the creation of Accumulator:${name}")
                    accum = sc.longAccumulator(name)
                    accumStore.put(name, accum)
                }
            }
        }
        accum.asInstanceOf[LongAccumulator]
    }

    def resetLongAccumulator(name : String) : LongAccumulator = {
        accumStore.remove(name).asInstanceOf[LongAccumulator]
    }

    // only be called from the listenerThread in LiveListenerBus
    override def onApplicationEnd(applicationEnd : SparkListenerApplicationEnd) {
        for (key <- broadcastVarNames) {
            resetBroadcastVar(key).foreach{ _.destroy }
        }
        for (key <- accumulatorNames) {
            resetLongAccumulator(key)
        }
    }
}

object Contexts {
    @volatile implicit var ssc : StreamingContext = null
    @volatile implicit var sc : SparkContext = null
}

// vim: set ts=4 sw=4 et:
