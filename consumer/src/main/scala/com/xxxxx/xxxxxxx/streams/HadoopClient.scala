/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @author qb.wu@outlook.com
 * @date 2017/7/18
 */

package com.xxxxx.xxxxxxx.streams

import org.slf4j.{ Logger, LoggerFactory }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

final class HadoopClient(config : Configuration) {
    private val hdfs = FileSystem.get(config)
    def touch(p : String) : Boolean = {
        val path = new Path(p)
        try {
            hdfs.create(path, false).close
            true
        } catch {
            // path can be existed
            case e : java.io.IOException =>
                e.printStackTrace
                try {
                    hdfs.open(path).close
                    true
                } catch {
                    case e : java.io.IOException =>
                    e.printStackTrace
                    false
                }
        }
    }

    def rename(abs : String)(srcRel : String, dstRel : String) : Boolean = {
        try {
            hdfs.rename(new Path(s"${abs}/${srcRel}"), new Path(s"${abs}/${dstRel}"))
        } catch {
            case e : java.io.IOException =>
                e.printStackTrace
                return false
        }
    }
}
// vim: set ts=4 sw=4 et:
