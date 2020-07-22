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

import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}

object Main {
    var algorithm : StreamsAlgo = null
    def appName = algorithm.name
    def producerDS = algorithm.resourceDS
    def processorDS = algorithm.resultDS

    var date : String = null

    def main(args : Array[String]) {
        val logger = LoggerFactory.getLogger(this.getClass)
        // Get arguments -------------------------------------
        val options = FlagsParser.parse(args)
        val algoClass = options('algoClass).asInstanceOf[String]
        val brokerList = options('brokerList).asInstanceOf[String]
        val latencyMin = options('latencyMin).asInstanceOf[Int]
        val checkpointDir = options('checkpointDir).asInstanceOf[String]
        val maxPartitionRate = options('maxPartitionRate).asInstanceOf[Int]
        val contextLifeD = options('contextLifeD).asInstanceOf[Int]
        val zkList = options.getOrElse('zookeeper, null).asInstanceOf[String]
        //-----------------------------------------------------

        algorithm = Class.forName(algoClass).newInstance.asInstanceOf[StreamsAlgo]
        if (algorithm == null) {
            logger.error(s"Failed to load the algorithm class: ${algoClass}")
            System.exit(-1)
        }

        def checkpointPath = s"${checkpointDir}/${date}/checkpoint"

        MetaClient.zkList = zkList
        // one data source can have more than one topics
        val topicSet = producerDS.split(",").flatMap{ MetaClient.topics(_).split(",") }.toSet
        logger.info(s"""Consuming topics: ${topicSet.mkString(",")}""")

        // consume from beginning when start without checkpoint
        val kafkaParams = Map("bootstrap.servers" -> brokerList,
                              "auto.offset.reset" -> "smallest")

        val notifier = new Notifier
        val producerTracker = new ProducerTracker(processorDS, producerDS, notifier)
        /*by default, spark.streaming.backpressure.enabled=false*/
        val conf = (new SparkConf).setAppName(s"${appName}")
            .set("spark.streaming.kafka.maxRatePerPartition", maxPartitionRate.toString)
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
            .set("spark.hadoop.mapred.output.compress", "true")
            .set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
            .set("spark.streaming.concurrentJobs", "1")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(algorithm.registerKryoClasses)

        var day = 0
        while ( { day += 1; true } ) {
            date = producerTracker.current
            logger.info(s"check the streams of ${date}")

            val checkpoint = checkpointPath

            import Contexts._
            ssc = StreamingContext.getOrCreate(checkpoint,
                () => {
                    // Only create the stream when there is none of checkpoint
                    logger.info(s"Create a new StreamingContext")
                    val ret = new StreamingContext(SparkContext.getOrCreate(conf), Minutes(latencyMin))
                    val dstr =
                        KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](
                            ret, kafkaParams, topicSet.map { t => s"${t}-${date}" })
                    algorithm.transformation(date, dstr)
                    ret.checkpoint(checkpoint)
                    ret
                })
            sc = ssc.sparkContext

            producerTracker.setHadoopClient(() => new HadoopClient(ssc.sparkContext.hadoopConfiguration))
            ssc.addStreamingListener(producerTracker)
            sc.addSparkListener(CtxVarManager)

            notifier.reset

            ssc.start
            // StreamingListener thread running and there can be some notices now {

            notifier.await

            if (producerTracker.detectFailure) {
                logger.warn(s"Exit for some failure detected ${date}")
                ssc.stop(true)
                System.exit(-1);
            }

            // for truncating the eventLog
            if (day >= contextLifeD) {
                logger.warn(s"Restart the sc @ ${date}")
                ssc.stop(true)
                day = 0
            } else {
                ssc.stop(false)
            }
            // } Listener thread stopped
        }
    }
}

// vim: set ts=4 sw=4 et:
