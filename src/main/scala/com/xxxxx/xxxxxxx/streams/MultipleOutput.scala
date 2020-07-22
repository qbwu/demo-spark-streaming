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

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

final class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any) : Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String)
  : String = s"${key.asInstanceOf[String]}"
}
