/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @file native_sign_jni.cpp
 * @author qb.wu@outlook.com
 * @date 2017/06/14 00:39:10
 * @brief
 *
 **/

#include "native_sign_jni.h"
#include <iostream>
#include <string.h>
#include <sign/sign.h>

JNIEXPORT jstring
JNICALL Java_com_xxxxx_xxxxxxx_streams_NativeSign_00024_signMm64
(JNIEnv *env, jobject /*jo*/, jstring jstr) {
    const auto *cstr = env->GetStringUTFChars(jstr, NULL);
    if (!cstr) {
        return NULL;
    }

    auto sign = std::to_string(xxxxxxx::sign::create_sign_mm64(cstr, strlen(cstr)));
    env->ReleaseStringUTFChars(jstr, cstr);
    return env->NewStringUTF(sign.c_str());
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
