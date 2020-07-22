/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @file kafka_producer.h
 * @author qb.wu@outlook.com
 * @date 2017/05/24 16:23:02
 * @brief Wrapper of rdkafka producer.
 *
 **/

#ifndef  OFFLINE_SERVICE_EVENT_STREAMS_KAFKA_PRODUCER_H
#define  OFFLINE_SERVICE_EVENT_STREAMS_KAFKA_PRODUCER_H

#include <iostream>
#include <memory>
#include <unordered_map>
#include <librdkafka/rdkafkacpp.h>
#include "error_type.h"

namespace offline {
namespace hadoop_kafka {

class KafkaProducer {
public:
    using KafkaProducerProperties = std::unordered_map<std::string, std::string>;

    KafkaProducer(const std::string &topic_str, const std::string &broker_list);

    ~KafkaProducer();

    // @throws
    //      NullPtrError            :   can not create the RdKafka::Conf or RdKafka::Producer
    //                                  or RdKafka::Topic
    //      std::domain_error       :   propertie values are outside of the domain
    //      std::invalid_argument   :   invalid properties
    //      std::runtime_error      :   internal errors
    void init(const KafkaProducerProperties &props, bool dump_conf);

    // @throws
    //      NullPtrError            :   called before succesfully init
    //      ProducerError           :   KafkaProducer stops or some errors happen when produce
    //                                  this message, possible error_codes:
    //                                      RdKafka::ERR__ALL_BROKERS_DOWN
    //                                      RdKafka::ERR__QUEUE_FULL
    //                                      RdKafka::ERR_MSG_SIZE_TOO_LARGE
    //                                      RdKafka::ERR__UNKNOWN_PARTITION
    //                                      RdKafka::ERR__UNKNOWN_TOPIC
    void send_msg(const void *key, std::size_t klen, void *payload, std::size_t len);

    // @throws
    //      NullPtrError            :   called before succesfully init
    void flush();

    bool is_stop() const;
    float fail_percent() const;

private:
    std::string _topic_str;
    std::string _broker_list;
    int32_t _partition = 0;

    // milliseconds to wait when the out queue is full
    int32_t _timeout = 0; // ms
    // milliseconds of every RdKafka::flush
    int32_t _flush_interval = 0; // ms

    class HashPartitionerCb;
    class EventCb;
    class DeliveryReportCb;

    std::unique_ptr<RdKafka::Producer> _producer;
    std::unique_ptr<RdKafka::Topic> _topic;
    std::unique_ptr<HashPartitionerCb> _hash_partitioner;
    std::unique_ptr<EventCb> _event_cb;
    std::unique_ptr<DeliveryReportCb> _dr_cb;
};

class ProducerError : public CError {
public:
    ProducerError(const std::string &err_msg, int error_code)
        : CError(err_msg, error_code) {}
};

} // hadoop_kafka
} // offline

#endif //  OFFLINE_SERVICE_EVENT_STREAMS_KAFKA_PRODUCER_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
