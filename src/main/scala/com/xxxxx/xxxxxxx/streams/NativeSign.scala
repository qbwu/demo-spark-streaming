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

import java.lang.System

object NativeSign {
    // there is no unsigned long in scala & java
    @native def signMm64(key : String) : String
    System.loadLibrary("signjni")
}

// vim: set ts=4 sw=4 et:
