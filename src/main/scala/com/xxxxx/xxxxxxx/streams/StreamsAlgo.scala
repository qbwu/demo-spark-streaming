/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @author qb.wu@outlook.com
 * @date 2017/6/19
 */

package com.xxxxx.xxxxxxx.streams
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait StreamsAlgo extends Serializable {
    def name : String
    def resourceDS : String
    def resultDS : String
    def registerKryoClasses : Array[Class[_]]
    // DStream graph for the streaming data of specific date
    def transformation(date : String, dstr : DStream[(Array[Byte], String)]) : Unit
}

// vim: set ts=4 sw=4 et:
