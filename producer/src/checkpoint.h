/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @file checkpoint.cpp
 * @author qb.wu@outlook.com
 * @date 2017/05/24 12:46:51
 * @brief HDFS based checkpoint module.
 *
 **/

#ifndef  OFFLINE_SERVICE_EVENT_STREAMS_CHECKPOINT_H
#define  OFFLINE_SERVICE_EVENT_STREAMS_CHECKPOINT_H

#include <iostream>
#include <hadoop/libdfs/hdfs.h>
#include "error_type.h"

namespace offline {
namespace hadoop_kafka {

struct HdfsProfile {
    std::string host;
    uint16_t port = 0;
    std::string ugi;

    HdfsProfile() = default;
    HdfsProfile(const std::string &h, uint16_t p, const std::string &u)
        : host(h), port(p), ugi(u) {}
};

class Checkpoint {
public:
    Checkpoint(const HdfsProfile &hdfs_profile,
               const std::string &path)
    : _hdfs_profile(hdfs_profile), _path(path), _tmp_path(path + ".tmp") {}

    ~Checkpoint() {
        if (_fs != nullptr) {
            hdfsDisconnect(_fs);
        }
    }

    // @throws
    //      HdfsError   :   1) can not make connection to HDFS; 2) can not rename an existed
    //                      temporary checkpoint; 3) can not open the checkpoint
    void create();

    // @throws
    //      NullPtrError    :   called before succesfully init
    //      HdfsError       :   can not open/stat/read the checkpoint
    uint32_t load();

    // @throws
    //      NullPtrError    :   called before succesfully init
    //      HdfsError       :   1) can not open/write the tmp checkpoint; 2) can not move the
    //                          temporary checkpoint to the real checkpoint
    void update(uint32_t value);

    std::string path() const {
        return _path;
    }

private:
    HdfsProfile _hdfs_profile;
    std::string _path;
    std::string _tmp_path;
    hdfsFS _fs;
};

class HdfsError : public CError {
public:
    HdfsError(const std::string &err_msg, int error_code)
        : CError(err_msg, error_code) {}
};

} // hadoop_kafka
} // offline

#endif // OFFLINE_SERVICE_EVENT_STREAMS_CHECKPOINT_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
