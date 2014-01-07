#!/usr/bin/python

import time
import errno

from cffi import FFI

ffi = FFI()

""" Definitions for all rdkafka functionality we need """
ffi.cdef("""
const char *rd_kafka_version_str (void);
int rd_kafka_version (void);

void *rd_kafka_conf_new (void);
int rd_kafka_conf_set (void *conf,
		       const char *name,
		       const char *value,
		       char *errstr, size_t errstr_size);
void *rd_kafka_new (int type, void *conf,
                    char *errstr, size_t errstr_size);
void rd_kafka_destroy (void *rk);

void *rd_kafka_topic_conf_new (void);
int rd_kafka_topic_conf_set (void *conf,
				const char *name,
				const char *value,
				char *errstr, size_t errstr_size);
void *rd_kafka_topic_new (void *rk, const char *topic,
			  void *topic_conf);
void rd_kafka_topic_destroy (void *rkt);

int rd_kafka_produce (void *rkt, int32_t partitition,
		      int msgflags,
		      void *payload, size_t len,
		      const void *key, size_t keylen,
		      void *msg_opaque);

int  rd_kafka_outq_len (void *rk);

int rd_kafka_poll (void *rk, int timeout_ms);

enum rd_kafka_type_t { RD_KAFKA_PRODUCER, RD_KAFKA_CONSUMER, ... };

enum rd_kafka_conf_res_t { RD_KAFKA_CONF_UNKNOWN, RD_KAFKA_CONF_INVALID,
                           RD_KAFKA_CONF_OK, ... };


const int errstr_size;
char errstr[512];

#define RD_KAFKA_MSG_F_COPY ...
""");



rkc = ffi.verify("""
#include <librdkafka/rdkafka.h>

/* FIXME: This could be a ffi.new() */
const int errstr_size = 512;
char errstr[512];
""",
               libraries = ['rdkafka'])


class RdKafka:
    Producer = rkc.RD_KAFKA_PRODUCER
    Consumer = rkc.RD_KAFKA_CONSUMER
    
    # Kafka handle
    _rk = None


    """ Create Kafka handle """
    def __init__(self, client_type, config):
        conf = rkc.rd_kafka_conf_new()
        for k in config:
            if k == 'dr_cb':
                raise Exception('FIXME: set dr_cb')
                continue
            r = rkc.rd_kafka_conf_set(conf, str(k), str(config[k]),
                                      rkc.errstr, rkc.errstr_size)
            if r != rkc.RD_KAFKA_CONF_OK:
                raise Exception("%s" % ffi.string(rkc.errstr))

        self._rk = rkc.rd_kafka_new(client_type, conf, rkc.errstr, rkc.errstr_size)
        if self._rk == ffi.NULL:
            raise Exception("%s" % ffi.string(rkc.errstr))


    """ Destroy Kafka handle.
        Wait up to timeout_ms for all outstanding messages to be sent to broker
    """
    def destroy(self, timeout=5.0):
        end = time.time() + timeout

        while rkc.rd_kafka_outq_len(self._rk) > 0 and time.time() <= end:
            time.sleep(0.1)

        rkc.rd_kafka_destroy(self._rk)


    """ Create topic handle """
    def new_topic(self, topic, config):
        conf = rkc.rd_kafka_topic_conf_new()
        for k in config:
            r = rkc.rd_kafka_topic_conf_set(conf, str(k), str(config[k]),
                                            rkc.errstr, rkc.errstr_size)
            if r != rkc.RD_KAFKA_CONF_OK:
                raise Exception("%s" % ffi.string(rkc.errstr))

        rkt = rkc.rd_kafka_topic_new(self._rk, topic, conf)
        if rk == ffi.NULL:
            raise Exception('%s' % os.strerror(ffi.errno))

        return rkt


    """ Destroy topic handle """
    def destroy_topic(self,rkt):
        rkc.rd_kafka_topic_destroy(rkt)


    """ Produce message to topic 'rkt' partition 'partition' """
    def produce(self, rkt, partition, payload):
        r = rkc.rd_kafka_produce(rkt, int(partition), rkc.RD_KAFKA_MSG_F_COPY,
                                 payload, len(payload),
                                 ffi.NULL, 0, ffi.NULL)
        if r == -1:
            if ffi.errno == errno.ENOBUFS:
                raise Exception('queue.buffering.max.messages exceeded')
            elif ffi.errno == errno.EMSGSIZE:
                raise Exception('message size %i exceeds message.max.bytes' % \
                                    len(payload))
            elif ffi.errno == errno.ESRCH:
                raise Exception('unknown partition')
            else:
                raise Exception(os.strerror(ffi.errno))


    """ Poll Kafka handle for errors and delivery reports
        FIXME: As no callbacks can be set yet this serves no purpose.
    """
    def poll(self, timeout):
        return rkc.rd_kafka_poll(self._rk, int(round(timeout * 1000)))



if __name__ == '__main__':
    
    # Topic
    topic = 'onepart'
    # Partition
    partition = -1 # Random
    # Number of messages to send
    cnt = 100000

    verint = rkc.rd_kafka_version()
    ver = ffi.string(rkc.rd_kafka_version_str())

    print 'librdkafka version is 0x%x - %s' % (verint, ver)

    # Create producer
    rk = RdKafka(RdKafka.Producer,
                 {'metadata.broker.list':'localhost',
                  'queue.buffering.max.messages':cnt});

    # Create topic handle
    rkt = rk.new_topic(topic, {'request.required.acks':1})

    pre = time.time()

    # Produce to topic, partition 0
    for i in range(1, cnt):
        rk.produce(rkt, partition, 'Producing something important: msg #%i' % i)

    # Poll for delivery callback (FIXME: not implemented)
    #rk.poll(10)

    # Destroy topic handle
    rk.destroy_topic(rkt)

    # Destroy producer
    rk.destroy(10)

    post = time.time()

    print 'Sent %i messages in %.4f seconds: %i msgs/s' % \
        (cnt, post-pre, int(round(cnt / (post-pre))))
