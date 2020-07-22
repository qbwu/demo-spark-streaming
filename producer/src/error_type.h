/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @file error_type.h
 * @author qb.wu@outlook.com
 * @date 2017/06/02 00:36:00
 * @brief Application exception types.
 *
 **/

#ifndef  OFFLINE_SERVICE_EVENT_STREAMS_ERROR_TYPE_H
#define  OFFLINE_SERVICE_EVENT_STREAMS_ERROR_TYPE_H

#include <iostream>
#include <stdexcept>

namespace offline {
namespace hadoop_kafka {

class CError : public std::runtime_error {
public:
    CError(const std::string &err_msg, int error_code)
        : std::runtime_error(err_msg), _error_code(error_code) {}

    int error_code() const { return _error_code; }

private:
    int _error_code = 0;
};

class NullPtrError : public std::logic_error {
public:
    NullPtrError() : std::logic_error("Dangling pointer") {}
};

} // hadoop_kafka
} // offline

#endif //  OFFLINE_SERVICE_EVENT_STREAMS_ERROR_TYPE_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
