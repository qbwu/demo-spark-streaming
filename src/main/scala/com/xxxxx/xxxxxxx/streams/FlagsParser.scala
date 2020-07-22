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

final case class ParseException(msg : String = "", cause : Throwable = null) extends
    Exception(msg, cause)

object FlagsParser {
    def usage = """
    Usage : launch_command --algo_class full_class_name] --broker_list ip:port[,...]
            --latency_min number  --checkpoint hdfs://addr/path
            --max_partition_rate number --context_life_d number [--zookeeper ip:port[,...]]
    """
    type OptionMap = Map[Symbol, Any]

    def parse(args : Array[String]) : OptionMap = {

        def nextOption(map : OptionMap, list : List[String]) : OptionMap = {
            def isSwitch(s : String) = { s(0) == '-' }

            list match {
                case Nil => map
                case "--algo_class" :: value :: tail =>
                    nextOption(map + ('algoClass -> value.toString), tail)
                case "--broker_list" :: value :: tail =>
                    nextOption(map + ('brokerList -> value.toString), tail)
                case "--latency_min" :: value :: tail =>
                    nextOption(map + ('latencyMin -> value.toInt), tail)
                case "--checkpoint" :: value :: tail =>
                    nextOption(map + ('checkpointDir -> value.toString), tail)
                case "--max_partition_rate" :: value :: tail =>
                    nextOption(map + ('maxPartitionRate -> value.toInt), tail)
                case "--context_life_d" :: value :: tail =>
                    nextOption(map + ('contextLifeD -> value.toInt), tail)
                case "--zookeeper" :: value :: tail =>
                    nextOption(map + ('zookeeper -> value.toString), tail)
                case _ =>
                    throw new ParseException(s"Unknown option ${list.head}")
            }
        }

        val options = nextOption(Map('contextLifeD -> 3), args.toList)

        if (!options.contains('algoClass)
            || !options.contains('brokerList)
            || !options.contains('latencyMin)
            || !options.contains('checkpointDir)
            || !options.contains('maxPartitionRate)) {
            throw new ParseException(s"Lack of flags\n${usage}")
        }

        options
    }
}
