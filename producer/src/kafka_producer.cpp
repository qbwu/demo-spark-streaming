/***************************************************************************
 *
 * Copyright (c) 2017 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @file kafka_producer.cpp
 * @author qb.wu@outlook.com
 * @date 2017/05/27 18:38:52
 * @brief
 *
 **/

#include "kafka_producer.h"
#include <iostream>
#include <stdexcept>
#include <unordered_map>
#include <list>
#include <base/logging.h>
#include <sign/sign.h>
#include <utils/utils.h>

namespace {

const std::string TOPIC_PREFIX = "topic.";
const std::string GLOBAL_PREFIX = "global.";

const int32_t DEFAULT_TIMEOUT = 1000; // ms
const int32_t DEFAULT_FLUSH_INTERVAL = 1000; // ms

std::string
get_prop(const offline::hadoop_kafka::KafkaProducer::KafkaProducerProperties &props,
         const std::string &key);

std::string
get_prop(const offline::hadoop_kafka::KafkaProducer::KafkaProducerProperties &props,
         const std::string &key, const std::string &def);

void print_conf(RdKafka::Conf *conf);

}

namespace offline {
namespace hadoop_kafka {

class KafkaProducer::DeliveryReportCb : public RdKafka::DeliveryReportCb {
friend class KafkaProducer;
public:
    virtual void dr_cb(RdKafka::Message &message) override;
private:
    uint64_t _failed_cnt = 0;
    uint64_t _total_cnt = 0;
};

class KafkaProducer::EventCb : public RdKafka::EventCb {
friend class KafkaProducer;
public:
    virtual void event_cb(RdKafka::Event &event) override;
private:
    // r/w in one single thread, no need to be atomic
    bool _stop = false;
};

class KafkaProducer::HashPartitionerCb : public RdKafka::PartitionerCb {
friend class KafkaProducer;
public:
    virtual int32_t partitioner_cb(const RdKafka::Topic *topic, const std::string *key,
            int32_t partition_cnt, void *msg_opaque) override;
};

KafkaProducer::KafkaProducer(const std::string &topic_str, const std::string &broker_list)
        : _topic_str(topic_str), _broker_list(broker_list) {}

KafkaProducer::~KafkaProducer() {
    if (!_producer) {
        return;
    }
    // noexcept
    try {
        flush();
    } catch (...) {
        LOG(ERROR) << "caught excetions in destructor when flush all messages before exit.";
        exit(-1);
    }
}

void KafkaProducer::init(const KafkaProducerProperties &props, bool dump_conf) {
    auto conf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    auto topic_conf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
    if (!conf || !topic_conf) {
        throw NullPtrError();
    }

    const std::string key_timeout("extension.queue.full.waiting.ms");
    auto timeout_str = get_prop(props, key_timeout);
    _timeout = timeout_str.empty() ? DEFAULT_TIMEOUT : std::stoi(timeout_str);
    if (_timeout < 0) {
        throw std::domain_error("value out of domain([0, maxint]): " + key_timeout);
    }
    LOG(INFO) << key_timeout << "=" << _timeout;

    const std::string key_flush_interval("extension.flush.interval.ms");
    auto flush_interval_str = get_prop(props, key_flush_interval);
    _flush_interval =
            flush_interval_str.empty() ? DEFAULT_FLUSH_INTERVAL : std::stoi(flush_interval_str);
    if (_flush_interval < 0) {
        throw std::domain_error("value out of domain([0, maxint]): " + key_flush_interval);
    }
    LOG(INFO) << key_flush_interval << "=" << _flush_interval;

    _event_cb = utils::make_unique<EventCb>();
    _dr_cb = utils::make_unique<DeliveryReportCb>();

    std::string errstr;
    if (conf->set("metadata.broker.list", _broker_list, errstr) != RdKafka::Conf::CONF_OK
            || conf->set("event_cb", _event_cb.get(), errstr) != RdKafka::Conf::CONF_OK
            || conf->set("dr_cb", _dr_cb.get(), errstr) != RdKafka::Conf::CONF_OK) {
        LOG(ERROR) << "Failed to set the basic properties (metadata.broker.list, event_cb, dr_cb)"
                   << ", error: " << errstr;
        throw std::runtime_error("Internal error");
    }

    const std::string key_partitioner("extension.partitioner");
    auto partitioner = get_prop(props, key_partitioner, "hash");

    _partition = RdKafka::Topic::PARTITION_UA;
    if (partitioner == "random") {
        // Nothing to do.
    } else if (partitioner == "hash") {
        _hash_partitioner = utils::make_unique<HashPartitionerCb>();
        if (topic_conf->set("partitioner_cb", _hash_partitioner.get(), errstr)
                != RdKafka::Conf::CONF_OK) {
            LOG(ERROR) << "Failed to set the basic properties (partitioner_cb)"
                       << ", error: " << errstr;
            throw std::runtime_error("Internal error");
        }
    } else {
        auto part_id = std::stoi(partitioner);
        if (part_id >= 0) {
            _partition = part_id;
        }
    }
    LOG(INFO) << key_partitioner << "=" << partitioner << "&" << _partition;

    // General properties
    const auto topic_prefix_len = TOPIC_PREFIX.length();
    const auto global_prefix_len = GLOBAL_PREFIX.length();
    for (const auto &prop : props) {
        const auto &key = prop.first;
        const auto &value = prop.second;

        if (key.find(TOPIC_PREFIX) == 0) {
            if (topic_conf->set(key.substr(topic_prefix_len), value, errstr)
                    != RdKafka::Conf::CONF_OK) {
                LOG(ERROR) << "Failed to set the property: { " << key << "=" << value
                           << " }, error: " << errstr;
                throw std::invalid_argument("Invalid producer property");
            }
        } else if (key.find(GLOBAL_PREFIX) == 0) {
            if (conf->set(key.substr(global_prefix_len), value, errstr)
                    != RdKafka::Conf::CONF_OK) {
                LOG(ERROR) << "Failed to set the property { " << key << "=" << value
                           << " }, error: " << errstr;
                throw std::invalid_argument("Invalid topic property");
            }
        }
    }

    if (dump_conf) {
        LOG(INFO) << "# Global config #";
        print_conf(conf.get());
        LOG(INFO) << ">>>>>>>>>>>>>>>>>";
        LOG(INFO) << "# Topic config #";
        print_conf(topic_conf.get());
        LOG(INFO) << ">>>>>>>>>>>>>>>>>";
    }

    // Create producer using accumulated global configuration.
    _producer = std::unique_ptr<RdKafka::Producer>(
                                RdKafka::Producer::create(conf.get(), errstr));
    if (!_producer) {
        LOG(ERROR) << "Failed to create producer, error: " << errstr;
        throw NullPtrError();
    }

    // Create topic handle
    _topic = std::unique_ptr<RdKafka::Topic>(
            RdKafka::Topic::create(_producer.get(), _topic_str, topic_conf.get(), errstr));
    if (!_topic) {
        LOG(ERROR) << "Failed to create topic, error: " << errstr;
        throw NullPtrError();
    }
}

void KafkaProducer::send_msg(const void *key, std::size_t klen,
                             void *payload, std::size_t len) {
    if (!_producer || !_topic) {
        LOG(ERROR) << "None of producer or topic, Maybe KafkaProducer have not initialized";
        throw NullPtrError();
    }

    int32_t time(_timeout);
    int32_t time_wait(0);
    do {
        _producer->poll(time_wait);
        if (is_stop()) {
            throw ProducerError(RdKafka::err2str(RdKafka::ERR__ALL_BROKERS_DOWN),
                                RdKafka::ERR__ALL_BROKERS_DOWN);
        }
        auto rc = _producer->produce(_topic.get(), _partition, RdKafka::Producer::RK_MSG_COPY,
                                     payload, len, key, klen, nullptr);
        switch (rc) {
            case RdKafka::ERR_NO_ERROR:
                return;
            case RdKafka::ERR__QUEUE_FULL:
                time -= time_wait;
                if (time > 0) {
                    time_wait <<= 1;
                    if (time_wait <= 0) {
                        time_wait = std::max(1, std::min(time, 100));
                    }
                    LOG(INFO) << "Waiting queue free for another duration(ms):" << time_wait;
                    break;
                } // else fall in the default case
            default:
                LOG(ERROR) << "Fatal error caused by the produce() for the first time";
                throw ProducerError(RdKafka::err2str(rc), rc);
        }
    } while (true);
}

void KafkaProducer::flush() {
    if (!_producer) {
        LOG(ERROR) << "None of producer, maybe KafkaProducer have not been initialized";
        throw NullPtrError();
    }

    int n(0);
    const auto qlen = _producer->outq_len();
    while (_producer->outq_len() > 0) {
        ++n;
        LOG(INFO) << "Waiting(" << n << ") at queue length: " << _producer->outq_len();
        _producer->flush(_flush_interval);
    }

    LOG(INFO) << "Finished flushing, qlen=" << qlen << ", current fail-percent="
            << fail_percent();
}

int32_t KafkaProducer::HashPartitionerCb::partitioner_cb(
        const RdKafka::Topic * /*topic*/, const std::string *key,
        int32_t partition_cnt, void * /*msg_opaque*/) {
    return sign::create_sign_mm64(key->c_str(), key->length()) % partition_cnt;
}

void KafkaProducer::DeliveryReportCb::dr_cb(RdKafka::Message &message) {
    _total_cnt++;
    if (message.err()) {
        _failed_cnt++;
        LOG(WARNING) << "Message delivery for (" << message.len() << " bytes): "
                     << message.errstr();
        LOG_FIRST_N(WARNING, 100) << "Payload=[" <<
                             static_cast<const char*>(message.payload()) << "]";
    }
}

void KafkaProducer::EventCb::event_cb(RdKafka::Event &event) {
    switch (event.type()) {
        case RdKafka::Event::EVENT_ERROR:
            LOG(ERROR) << "ERROR (" << RdKafka::err2str(event.err()) << "): "
                       << event.str();
            if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) {
                _stop = true;
            }
            break;

        case RdKafka::Event::EVENT_STATS:
            LOG(INFO) << "STATS: " << event.str();
            break;

        case RdKafka::Event::EVENT_LOG:
            LOG(INFO) << "LOG-" << event.severity() << "-" << event.fac() << ": "
                      << event.str();
            break;

        case RdKafka::Event::EVENT_THROTTLE:
            LOG(INFO) << "THROTTLED: " << event.throttle_time() << " ms by " <<
                event.broker_name() << " id-" << static_cast<int>(event.broker_id());
            break;

        default:
            LOG(WARNING) << "EVENT " << event.type() << " (" << RdKafka::err2str(event.err())
                << "): " << event.str();
    }
}

bool KafkaProducer::is_stop() const {
    return _event_cb->_stop;
}

float KafkaProducer::fail_percent() const {
    return _dr_cb->_failed_cnt / (_dr_cb->_total_cnt + 0.01);
}

} // hadoop_kafka
} // offline

namespace {

std::string
get_prop(const offline::hadoop_kafka::KafkaProducer::KafkaProducerProperties &props,
        const std::string &key) {
    return get_prop(props, key, "");
}

std::string
get_prop(const offline::hadoop_kafka::KafkaProducer::KafkaProducerProperties &props,
        const std::string &key, const std::string &def) {
    auto it = props.find(key);
    if (it != props.end()) {
        return it->second;
    }
    return def;
}

void print_conf(RdKafka::Conf *conf) {
    std::unique_ptr<std::list<std::string>> list(conf->dump());
    for (auto it = list->begin(); it != list->end(); it = std::next(it, 2)) {
        LOG(INFO) << *it << "=" << *std::next(it, 1);
    }
}

}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
