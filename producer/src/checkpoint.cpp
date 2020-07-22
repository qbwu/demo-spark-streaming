/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @file checkpoint.cpp
 * @author qb.wu@outlook.com
 * @date 2017/05/27 18:42:52
 * @brief
 *
 **/

#include "checkpoint.h"
#include <fcntl.h>
#include <arpa/inet.h>
#include <errno.h>
#include <memory>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <base/logging.h>
#include <hadoop-v2/libdfs/hdfs.h>
#include <hadoop-v2/libhce/StringUtils.hh>

namespace {

const uint32_t BUFFER_SIZE = sizeof(uint32_t);

}

namespace offline {
namespace hadoop_kafka {

void Checkpoint::create() {
    auto ugi = HadoopUtils::splitString(_hdfs_profile.ugi, ",");
    auto *user = ugi.size() > 0 ? ugi[0].c_str() : nullptr;
    auto *passwd = ugi.size() > 1 ? ugi[1].c_str() : nullptr;

    const auto &host = _hdfs_profile.host;
    const auto &port = _hdfs_profile.port;
    _fs = hdfsConnectAsUserNewInstance(host.c_str(), port, user, passwd);
    if (_fs == nullptr) {
        LOG(ERROR) << "Failed to connect hdfs --> " << host  << ":" << port;
        throw HdfsError(hdfsGetErrMessage(), errno);
    }

    // C style, returning 0 for true case.
    if (hdfsExists(_fs, _path.c_str()) == 0) {
        LOG(WARNING) << "Recovering from the existed checkpoint: " << _path;
        return;
    }

    if (hdfsExists(_fs, _tmp_path.c_str()) == 0) {
        LOG(WARNING) << "Recovering from the existed tmp checkpoint: " << _path;
        if (hdfsRename(_fs, _tmp_path.c_str(), _path.c_str()) != 0) {
            LOG(ERROR) << "Failed to rename tmp checkpoint";
            throw HdfsError(hdfsGetErrMessage(), errno);
        } else {
            return;
        }
    }
    // when we get here, the file doesn't exist, so create it.
    auto p_file = hdfsOpenFile(_fs, _path.c_str(), O_WRONLY, BUFFER_SIZE, 0, 0);
    if (p_file == nullptr) {
        LOG(ERROR) << "Failed to open the initial checkpoint: " << _path;
        throw HdfsError(hdfsGetErrMessage(), errno);
    }

    hdfsCloseFile(_fs, p_file);
}

uint32_t Checkpoint::load() {
    if (_fs == nullptr) {
        LOG(ERROR) << "Failed to load checkpoint, lost the hdfs connection";
        throw NullPtrError();
    }

    auto file_p = hdfsOpenFile(_fs, _path.c_str(), O_RDONLY, BUFFER_SIZE, 0, 0);
    if (file_p == nullptr) {
        LOG(ERROR) << "Failed to open the checkpoint: " << _path;
        throw HdfsError(hdfsGetErrMessage(), errno);
    }

    static auto fdel = std::bind(hdfsCloseFile, _fs, std::placeholders::_1);
    auto file = std::unique_ptr<hdfsFile_internal, decltype(fdel)>(file_p, fdel);

    auto file_info_p = hdfsGetPathInfo(_fs, _path.c_str());
    if (file_info_p == nullptr) {
        LOG(ERROR) << "Failed to get the info of checkpoint: " << _path;
        throw HdfsError(hdfsGetErrMessage(), errno);
    }

    static auto idel = std::bind(hdfsFreeFileInfo, std::placeholders::_1, 1);
    auto file_info = std::unique_ptr<hdfsFileInfo, decltype(idel)>(file_info_p, idel);

    if (file_info -> mSize == 0) {
        return 0;
    }

    uint32_t net_uint32(0);
    auto size = hdfsRead(_fs, file.get(), reinterpret_cast<void*>(&net_uint32), BUFFER_SIZE);
    if (size != BUFFER_SIZE) {
        LOG(ERROR) << "Failed to read the checkpoint: " << _path << ", read size: "
                   << size;
        throw HdfsError(hdfsGetErrMessage(), errno);
    }

    // eliminate the difference between big-endian/little-endian machines where the same checkpoint
    // may be loaded or updated
    return ntohl(net_uint32);
}

void Checkpoint::update(uint32_t value) {
    if (_fs == nullptr) {
        LOG(ERROR) << "Failed to update checkpoint, lost the hdfs connection";
        throw NullPtrError();
    }

    auto file_p = hdfsOpenFile(_fs, _tmp_path.c_str(), O_WRONLY, BUFFER_SIZE, 0, 0);
    if (file_p == nullptr) {
        LOG(ERROR) << "Failed to open the checkpoint: " << _tmp_path;
        throw HdfsError(hdfsGetErrMessage(), errno);
    }

    static auto fdel = std::bind(hdfsCloseFile, _fs, std::placeholders::_1);
    auto file = std::unique_ptr<hdfsFile_internal, decltype(fdel)>(file_p, fdel);

    uint32_t net_uint32 = htonl(value);
    auto size = hdfsWrite(_fs, file.get(), reinterpret_cast<const void*>(&net_uint32), BUFFER_SIZE);
    if (size != BUFFER_SIZE) {
        LOG(ERROR) << "Failed to write the checkpoint: " << _tmp_path << ", written size: "
                   << size;
        throw HdfsError(hdfsGetErrMessage(), errno);
    }

    if (hdfsFlush(_fs, file.get()) != 0) {
        LOG(ERROR) << "Failed to flush the checkpoint: " << _tmp_path;
        throw HdfsError(hdfsGetErrMessage(), errno);
    }

    // C style, returning 0 for normal case.
    if (hdfsDelete(_fs, _path.c_str()) != 0
            || hdfsRename(_fs, _tmp_path.c_str(), _path.c_str()) != 0) {
        LOG(ERROR) << "Failed to rename the checkpoint: " << _tmp_path << " --> " << _path;
        throw HdfsError(hdfsGetErrMessage(), errno);
    }
}

} // hadoop_kafka
} // offline

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
