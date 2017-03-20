/*
 * libmqtt.c -- libmqtt library implementation.
 *
 * Copyright (c) zhoukk <izhoukk@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "libmqtt.h"

#define MQTT_IMPLEMENTATION
#include "mqtt.h"

#include "lib/ae.h"
#include "lib/anet.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/socket.h>

#define LIBMQTT_READ_BUFF   4096
#define LIBMQTT_LOG_BUFF    4096

enum libmqtt_state {
    LIBMQTT_ST_SEND_PUBLUSH,
    LIBMQTT_ST_SEND_PUBACK,
    LIBMQTT_ST_SEND_PUBREC,
    LIBMQTT_ST_SEND_PUBREL,
    LIBMQTT_ST_SEND_PUBCOMP,
    LIBMQTT_ST_WAIT_PUBACK,
    LIBMQTT_ST_WAIT_PUBREC,
    LIBMQTT_ST_WAIT_PUBREL,
    LIBMQTT_ST_WAIT_PUBCOMP,
};

enum libmqtt_dir {
    LIBMQTT_DIR_IN,
    LIBMQTT_DIR_OUT,
};

struct libmqtt_pub {
    struct {
        uint16_t packet_id;
        char *topic;
        enum mqtt_qos qos;
        int retain;
        char *payload;
        int length;
    } p;
    enum libmqtt_state s;
    enum libmqtt_dir d;
    int t;

    struct libmqtt_pub *next;
};

struct libmqtt {
    struct mqtt_p_connect c;
    struct mqtt_parser p;
    uint16_t packet_id;

    struct {
        int now;
        int ping;
        int send;
    } t;

    struct {
        struct libmqtt_pub *head;
        struct libmqtt_pub *tail;
    } pub;

    void *ud;
    struct libmqtt_cb cb;

    void (* log)(void *ud, const char *str);
    char logbuf[LIBMQTT_LOG_BUFF];

    aeEventLoop *el;
    char *host;
    int port;
    int fd;
    long long id;
};

static int __connect(struct libmqtt *mqtt);

static int
__write(struct libmqtt *mqtt, const char *data, int size) {
    if (-1 == write(mqtt->fd, data, size)) {
        return -1;
    }
    mqtt->t.send = mqtt->t.now;
    return 0;
}

static void
__read(aeEventLoop *el, int fd, void *privdata, int mask) {
    struct libmqtt *mqtt;
    int nread;
    char buff[LIBMQTT_READ_BUFF];
    struct mqtt_b b;

    mqtt = (struct libmqtt *)privdata;
    nread = read(fd, buff, sizeof(buff));
    if (nread == -1 && errno == EAGAIN) {
        return;
    }
    b.s = buff;
    b.n = nread;
    if (nread <= 0 || mqtt__parse(&mqtt->p, mqtt, &b)) {
        aeDeleteFileEvent(el, fd, AE_READABLE);
        aeDeleteTimeEvent(el, mqtt->id);
        close(fd);
        mqtt->fd = 0;
        if (nread == 0 || __connect(mqtt)) {
            aeStop(el);
        }
    }
}

static void
__log(struct libmqtt *mqtt, const char *fmt, ...) {
    int n;
    va_list ap;

    if (!mqtt->log) return;
    n = snprintf(mqtt->logbuf, LIBMQTT_LOG_BUFF, "Client %.*s ", mqtt->c.client_id.n, mqtt->c.client_id.s);
    va_start(ap, fmt);
    n += vsnprintf(mqtt->logbuf+n, LIBMQTT_LOG_BUFF-n, fmt, ap);
    va_end(ap);
    mqtt->logbuf[n] = '\0';
    mqtt->log(mqtt->ud, mqtt->logbuf);
}

static void
__check_pub(struct libmqtt *mqtt) {
    struct libmqtt_pub **pp;

    pp = &mqtt->pub.head;
    while (*pp) {
        struct libmqtt_pub *pub;
        pub = *pp;
        if (mqtt->t.now - pub->t > mqtt->c.keep_alive) {
            switch (pub->s) {
            case LIBMQTT_ST_SEND_PUBLUSH:
            case LIBMQTT_ST_WAIT_PUBACK:
            case LIBMQTT_ST_WAIT_PUBREC:
                {
                    struct mqtt_packet p;
                    struct mqtt_b b;

                    memset(&p, 0, sizeof p);
                    p.h.type = PUBLISH;
                    p.h.dup = 1;
                    p.h.retain = pub->p.retain;
                    p.h.qos = pub->p.qos;
                    p.v.publish.packet_id = pub->p.packet_id;
                    p.v.publish.topic_name.s = pub->p.topic;
                    p.v.publish.topic_name.n = strlen(pub->p.topic);
                    p.payload.s = pub->p.payload;
                    p.payload.n = pub->p.length;

                    if (mqtt__serialize(&p, &b)) {
                        break;
                    }

                    if (0 == __write(mqtt, b.s, b.n)) {
                        __log(mqtt, "sending PUBLISH (d%d, q%d, r%d, m%d, \'%s\', ...(%d bytes))",
                              1, pub->p.qos, pub->p.retain, pub->p.packet_id, pub->p.topic, pub->p.length);
                        if (pub->p.qos == MQTT_QOS_0) {
                            *pp = (*pp)->next;
                            free(pub->p.topic);
                            if (pub->p.payload)
                                free(pub->p.payload);
                            free(pub);
                            pub = 0;
                            break;
                        } else if (pub->p.qos == MQTT_QOS_1) {
                            pub->s = LIBMQTT_ST_WAIT_PUBACK;
                        } else {
                            pub->s = LIBMQTT_ST_WAIT_PUBREC;
                        }
                    }
                    pub->t = mqtt->t.now;
                    mqtt_b_free(&b);
                }
                break;
            case LIBMQTT_ST_SEND_PUBACK:
                {
                    char puback[] = MQTT_PUBACK(pub->p.packet_id);
                    if (0 == __write(mqtt, puback, sizeof puback)) {
                        __log(mqtt, "sending PUBACK (id: %"PRIu16")", pub->p.packet_id);
                        *pp = (*pp)->next;
                        free(pub->p.topic);
                        if (pub->p.payload)
                            free(pub->p.payload);
                        free(pub);
                        pub = 0;
                    } else {
                        pub->t = mqtt->t.now;
                    }
                }
                break;
            case LIBMQTT_ST_SEND_PUBREC:
                {
                    char pubrec[] = MQTT_PUBREC(pub->p.packet_id);
                    if (0 == __write(mqtt, pubrec, sizeof pubrec)) {
                        __log(mqtt, "sending PUBREC (id: %"PRIu16")", pub->p.packet_id);
                        pub->s = LIBMQTT_ST_WAIT_PUBREL;
                    }
                    pub->t = mqtt->t.now;
                }
                break;
            case LIBMQTT_ST_SEND_PUBREL:
                {
                    char pubrel[] = MQTT_PUBREL(pub->p.packet_id);
                    if (0 == __write(mqtt, pubrel, sizeof pubrel)) {
                        __log(mqtt, "sending PUBREL (id: %"PRIu16")", pub->p.packet_id);
                        pub->s = LIBMQTT_ST_WAIT_PUBCOMP;
                    }
                    pub->t = mqtt->t.now;
                }
                break;
            case LIBMQTT_ST_SEND_PUBCOMP:
                {
                    char pubcomp[] = MQTT_PUBCOMP(pub->p.packet_id);
                    if (0 == __write(mqtt, pubcomp, sizeof pubcomp)) {
                        __log(mqtt, "sending PUBCOMP (id: %"PRIu16")", pub->p.packet_id);
                        *pp = (*pp)->next;
                        free(pub->p.topic);
                        if (pub->p.payload)
                            free(pub->p.payload);
                        free(pub);
                        pub = 0;
                    } else {
                        pub->t = mqtt->t.now;
                    }
                }
                break;
            case LIBMQTT_ST_WAIT_PUBREL:
                {
                    char pubrec[] = MQTT_PUBREC(pub->p.packet_id);
                    if (0 == __write(mqtt, pubrec, sizeof pubrec)) {
                        __log(mqtt, "sending PUBREC (id: %"PRIu16")", pub->p.packet_id);
                    }
                    pub->t = mqtt->t.now;
                }
                break;
            case LIBMQTT_ST_WAIT_PUBCOMP:
                {
                    char pubrel[] = MQTT_PUBREL(pub->p.packet_id);
                    if (0 == __write(mqtt, pubrel, sizeof pubrel)) {
                        __log(mqtt, "sending PUBREL (id: %"PRIu16")", pub->p.packet_id);
                    }
                    pub->t = mqtt->t.now;
                }
                break;
            }
        }
        if (pub)
            *pp = (*pp)->next;
    }
}

static int
__update(aeEventLoop *el, long long id, void *privdata) {
    struct libmqtt *mqtt;

    mqtt = (struct libmqtt *)privdata;
    mqtt->t.now += 1;

    if (mqtt->t.ping > 0 && (mqtt->t.now - mqtt->t.ping) > mqtt->c.keep_alive) {
        if (mqtt->fd > 0) {
            shutdown(mqtt->fd, SHUT_WR);
        }
        return 0;
    }

    if (mqtt->t.ping == 0 && (mqtt->t.now - mqtt->t.send) >= mqtt->c.keep_alive) {
        char b[] = MQTT_PINGREQ;
        if (0 == __write(mqtt, b, sizeof b)) {
            mqtt->t.ping = mqtt->t.now;
            __log(mqtt, "sending PINGREQ");
        }
    }
    __check_pub(mqtt);
    return 1000;
}

static int
__connect(struct libmqtt *mqtt) {
    int fd;
    long long id;

    if (ANET_ERR == (fd = anetTcpConnect(0, mqtt->host, mqtt->port))) {
        goto e1;
    }
    anetNonBlock(0, fd);
    anetEnableTcpNoDelay(0, fd);
    anetTcpKeepAlive(0, fd);
    if (AE_ERR == aeCreateFileEvent(mqtt->el, fd, AE_READABLE, __read, mqtt)) {
        goto e2;
    }
    if (mqtt->c.keep_alive > 0) {
        if (AE_ERR == (id = aeCreateTimeEvent(mqtt->el, 1000, __update, mqtt, 0))) {
            goto e3;
        }
        mqtt->id = id;
    }
    mqtt->fd = fd;
    return 0;

e3:
    aeDeleteFileEvent(mqtt->el, fd, AE_READABLE);
e2:
    close(fd);
e1:
    return -1;
}

static void
__generate_client_id(struct mqtt_b *b) {
    char id[1024] = {0};
    char hostname[256] = {0};

    gethostname(hostname, sizeof hostname);
    b->n = snprintf(id, sizeof id, "libmqtt/%d-%s", getpid(), hostname);
    b->s = strdup(id);
}

static uint16_t
__generate_packet_id(struct libmqtt *mqtt) {
    uint16_t id;

    id = ++mqtt->packet_id;
    if (id == 0)
        id = ++mqtt->packet_id;
    return id;
}

const char *libmqtt__strerror(int rc) {
    static const char *__libmqtt_error_strings[] = {
        "success",
        "null pointer access",
        "memory allocation error",
        "error mqtt qos",
        "error mqtt protocol version",
        "tcp connection error",
        "tcp write error",
        "max topic/qos per subscribe or unsubscribe",
    };

    if (-rc <= 0 || -rc > sizeof(__libmqtt_error_strings)/sizeof(char *))
        return 0;
    return __libmqtt_error_strings[-rc];
}

static int
__insert_pub(struct libmqtt *mqtt, struct mqtt_packet *p, enum libmqtt_dir d,
             enum libmqtt_state s) {
    struct libmqtt_pub *pub;

    pub = (struct libmqtt_pub *)malloc(sizeof *pub);
    if (!pub) goto e;
    memset(pub, 0, sizeof *pub);
    pub->p.packet_id = p->v.publish.packet_id;
    pub->p.qos = p->h.qos;
    pub->p.retain = p->h.retain;
    pub->p.topic = strndup(p->v.publish.topic_name.s, p->v.publish.topic_name.n);
    if (!pub->p.topic) goto e;
    if (p->payload.n > 0) {
        pub->p.payload = malloc(p->payload.n);
        if (!pub->p.payload) goto e;
        memcpy(pub->p.payload, p->payload.s, p->payload.n);
    }
    pub->p.length = p->payload.n;
    pub->d = d;
    pub->s = s;
    pub->t = mqtt->t.now;

    if (!mqtt->pub.head) {
        mqtt->pub.head = mqtt->pub.tail = pub;
    } else {
        mqtt->pub.tail->next = pub;
        mqtt->pub.tail = pub;
    }

    return 0;

e:
    if (pub) {
        if (pub->p.topic)
            free(pub->p.topic);
        if (pub->p.payload)
            free(pub->p.payload);
        free(pub);
    }
    return -1;
}

static void
__delete_pub(struct libmqtt *mqtt, struct libmqtt_pub *pub) {
    struct libmqtt_pub **pp;

    pp = &mqtt->pub.head;
    while (*pp) {
        if (*pp == pub) {
            *pp = (*pp)->next;
            free(pub->p.topic);
            if (pub->p.payload)
                free(pub->p.payload);
            free(pub);
        } else {
            pp = &(*pp)->next;
        }
    }
}

static void
__update_pub(struct libmqtt *mqtt, struct libmqtt_pub *pub, enum libmqtt_state s) {
    pub->s = s;
    pub->t = mqtt->t.now;
}

static struct libmqtt_pub *
__find_pub(struct libmqtt *mqtt, uint16_t packet_id, enum libmqtt_dir d,
           enum libmqtt_state s) {
    struct libmqtt_pub *pub;

    pub = mqtt->pub.head;
    while (pub) {
        if (pub->p.packet_id == packet_id && pub->d == d && pub->s == s) {
            return pub;
        }
        pub = pub->next;
    }

    return 0;
}

static int
__on_connack(void *ud, struct mqtt_packet *p) {
    struct libmqtt *mqtt;

    mqtt = (struct libmqtt *)ud;
    __log(mqtt, "received CONNACK (a%d, c%d)", p->v.connack.ack_flags, p->v.connack.return_code);
    if (mqtt->cb.connack)
        mqtt->cb.connack(mqtt, mqtt->ud, p->v.connack.ack_flags, p->v.connack.return_code);
    return 0;
}

static int
__on_suback(void *ud, struct mqtt_packet *p) {
    struct libmqtt *mqtt;
    int i;

    mqtt = (struct libmqtt *)ud;
    for (i = 0; i < p->v.suback.n; i++) {
        __log(mqtt, "received SUBACK (id: %"PRIu16", QoS: %d)", p->v.suback.packet_id, p->v.suback.qos[i]);
    }
    if (mqtt->cb.suback)
        mqtt->cb.suback(mqtt, mqtt->ud, p->v.suback.packet_id, p->v.suback.n, p->v.suback.qos);
    return 0;
}

static int
__on_unsuback(void *ud, struct mqtt_packet *p) {
    struct libmqtt *mqtt;

    mqtt = (struct libmqtt *)ud;
    __log(mqtt, "received UNSUBACK (id: %"PRIu16")", p->v.unsuback.packet_id);
    if (mqtt->cb.unsuback)
        mqtt->cb.unsuback(mqtt, mqtt->ud, p->v.unsuback.packet_id);
    return 0;
}

static int
__on_publish(void *ud, struct mqtt_packet *p) {
    struct libmqtt *mqtt;
    char puback[] = MQTT_PUBACK(p->v.publish.packet_id);
    char pubrec[] = MQTT_PUBREC(p->v.publish.packet_id);
    char topic[p->v.publish.topic_name.n+1];

    strncpy(topic, p->v.publish.topic_name.s, p->v.publish.topic_name.n);
    topic[p->v.publish.topic_name.n] = '\0';
    mqtt = (struct libmqtt *)ud;
    __log(mqtt, "received PUBLISH (d%d, q%d, r%d, m%d, \'%s\', ...(%d bytes))",
          p->h.dup, p->h.qos, p->h.retain, p->v.publish.packet_id, topic, p->payload.n);
    switch (p->h.qos) {
        case MQTT_QOS_0:
            if (mqtt->cb.publish)
                mqtt->cb.publish(mqtt, mqtt->ud, topic, p->h.qos, p->h.retain, p->payload.s, p->payload.n);
            return 0;
        case MQTT_QOS_1:
            if (mqtt->cb.publish)
                mqtt->cb.publish(mqtt, mqtt->ud, topic, p->h.qos, p->h.retain, p->payload.s, p->payload.n);
            if (__write(mqtt, puback, sizeof puback)) {
                return __insert_pub(mqtt, p, LIBMQTT_DIR_IN, LIBMQTT_ST_SEND_PUBACK);
            }
            __log(mqtt, "sending PUBACK (id: %"PRIu16")", p->v.publish.packet_id);
            return 0;
        case MQTT_QOS_2:
            if (__write(mqtt, pubrec, sizeof pubrec)) {
                return __insert_pub(mqtt, p, LIBMQTT_DIR_IN, LIBMQTT_ST_SEND_PUBREC);
            }
            __log(mqtt, "sending PUBREC (id: %"PRIu16")", p->v.publish.packet_id);
            return __insert_pub(mqtt, p, LIBMQTT_DIR_IN, LIBMQTT_ST_WAIT_PUBREL);
        case MQTT_QOS_F:
            return -1;
    }
    return 0;
}

static int
__on_puback(void *ud, struct mqtt_packet *p) {
    struct libmqtt *mqtt;
    struct libmqtt_pub *pub;
    uint16_t packet_id = p->v.puback.packet_id;

    mqtt = (struct libmqtt *)ud;
    __log(mqtt, "received PUBACK (id: %"PRIu16")", packet_id);
    pub = __find_pub(mqtt, packet_id, LIBMQTT_DIR_OUT, LIBMQTT_ST_WAIT_PUBACK);
    if (pub) {
        if (mqtt->cb.puback)
            mqtt->cb.puback(mqtt, mqtt->ud, packet_id);
        __delete_pub(mqtt, pub);
        return 0;
    }
    return -1;
}

static int
__on_pubrec(void *ud, struct mqtt_packet *p) {
    struct libmqtt *mqtt;
    struct libmqtt_pub *pub;
    uint16_t packet_id = p->v.pubrec.packet_id;

    mqtt = (struct libmqtt *)ud;
    __log(mqtt, "received PUBREC (id: %"PRIu16")", packet_id);
    pub = __find_pub(mqtt, packet_id, LIBMQTT_DIR_OUT, LIBMQTT_ST_WAIT_PUBREC);
    if (pub) {
        char pubrel[] = MQTT_PUBREL(packet_id);
        if (__write(mqtt, pubrel, sizeof pubrel)) {
            __update_pub(mqtt, pub, LIBMQTT_ST_SEND_PUBREL);
        } else {
            __log(mqtt, "sending PUBREL (id: %"PRIu16")", packet_id);
            __update_pub(mqtt, pub, LIBMQTT_ST_WAIT_PUBCOMP);
        }
        return 0;
    }
    return -1;
}

static int
__on_pubrel(void *ud, struct mqtt_packet *p) {
    struct libmqtt *mqtt;
    struct libmqtt_pub *pub;
    uint16_t packet_id = p->v.pubrel.packet_id;

    mqtt = (struct libmqtt *)ud;
    __log(mqtt, "received PUBREL (id: %"PRIu16")", packet_id);
    pub = __find_pub(mqtt, packet_id, LIBMQTT_DIR_IN, LIBMQTT_ST_WAIT_PUBREL);
    if (pub) {
        char pubcomp[] = MQTT_PUBCOMP(packet_id);
        if (mqtt->cb.publish)
            mqtt->cb.publish(mqtt, mqtt->ud, pub->p.topic, pub->p.qos, pub->p.retain, pub->p.payload, pub->p.length);
        if (__write(mqtt, pubcomp, sizeof pubcomp)) {
            __update_pub(mqtt, pub, LIBMQTT_ST_SEND_PUBCOMP);
        } else {
            __log(mqtt, "sending PUBCOMP (id: %"PRIu16")", packet_id);
            __delete_pub(mqtt, pub);
        }
        return 0;
    }
    return -1;
}

static int
__on_pubcomp(void *ud, struct mqtt_packet *p) {
    struct libmqtt *mqtt;
    struct libmqtt_pub *pub;
    uint16_t packet_id = p->v.pubcomp.packet_id;

    mqtt = (struct libmqtt *)ud;
    __log(mqtt, "received PUBCOMP (id: %"PRIu16")", packet_id);
    pub = __find_pub(mqtt, packet_id, LIBMQTT_DIR_OUT, LIBMQTT_ST_WAIT_PUBCOMP);
    if (pub) {
        if (mqtt->cb.puback)
            mqtt->cb.puback(mqtt, mqtt->ud, packet_id);
        __delete_pub(mqtt, pub);
        return 0;
    }
    return -1;
}

static int
__on_pingresp(void *ud, struct mqtt_packet *p) {
    struct libmqtt *mqtt;
    (void)p;

    mqtt = (struct libmqtt *)ud;
    __log(mqtt, "received PINGRESP");
    mqtt->t.ping = 0;
    return 0;
}

void libmqtt__debug(struct libmqtt *mqtt, void (* log)(void *ud, const char *str)) {
    mqtt->log = log;
}

int libmqtt__create(struct libmqtt **mqtt, const char *client_id, void *ud, struct libmqtt_cb *cb) {
    aeEventLoop *el;
    int rc;

    *mqtt = 0;
    if ((el = aeCreateEventLoop(128)) == 0) {
        rc = LIBMQTT_ERROR_MALLOC;
        goto e1;
    }

    if ((*mqtt = malloc(sizeof(struct libmqtt))) == 0) {
        rc = LIBMQTT_ERROR_MALLOC;
        goto e2;
    }
    memset(*mqtt, 0, sizeof(struct libmqtt));

    if (client_id) {
        mqtt_b_dup(&(*mqtt)->c.client_id, client_id);
    } else {
        __generate_client_id(&(*mqtt)->c.client_id);
    }
    if (mqtt_b_empty(&(*mqtt)->c.client_id)) {
        rc = LIBMQTT_ERROR_MALLOC;
        goto e3;
    }

    mqtt__parse_init(&(*mqtt)->p);
    mqtt__parse_cb(&(*mqtt)->p, CONNACK, __on_connack);
    mqtt__parse_cb(&(*mqtt)->p, SUBACK, __on_suback);
    mqtt__parse_cb(&(*mqtt)->p, UNSUBACK, __on_unsuback);
    mqtt__parse_cb(&(*mqtt)->p, PUBLISH, __on_publish);
    mqtt__parse_cb(&(*mqtt)->p, PUBACK, __on_puback);
    mqtt__parse_cb(&(*mqtt)->p, PUBREC, __on_pubrec);
    mqtt__parse_cb(&(*mqtt)->p, PUBREL, __on_pubrel);
    mqtt__parse_cb(&(*mqtt)->p, PUBCOMP, __on_pubcomp);
    mqtt__parse_cb(&(*mqtt)->p, PINGRESP, __on_pingresp);

    (*mqtt)->ud = ud;
    (*mqtt)->cb = *cb;
    (*mqtt)->el = el;
    (*mqtt)->t.ping = 0;
    (*mqtt)->t.send = 0;
    (*mqtt)->c.keep_alive = LIBMQTT_DEF_KEEPALIVE;
    (*mqtt)->c.clean_sess = 1;
    (*mqtt)->c.proto_ver = MQTT_PROTO_V4;

    return LIBMQTT_SUCCESS;

e3:
    free(*mqtt);
e2:
    aeDeleteEventLoop(el);
e1:
    return rc;
}

int libmqtt__destroy(struct libmqtt *mqtt) {
    if (!mqtt) {
        return LIBMQTT_ERROR_NULL;
    }
    aeDeleteEventLoop(mqtt->el);

    mqtt_b_free(&mqtt->c.client_id);
    mqtt_b_free(&mqtt->c.username);
    mqtt_b_free(&mqtt->c.password);
    mqtt_b_free(&mqtt->c.will_topic);
    mqtt_b_free(&mqtt->c.will_payload);
    free(mqtt->host);
    free(mqtt);
    return LIBMQTT_SUCCESS;
}

int libmqtt__keep_alive(struct libmqtt *mqtt, uint16_t keep_alive) {
    if (!mqtt) {
        return LIBMQTT_ERROR_NULL;
    }
    mqtt->c.keep_alive = keep_alive;
    return LIBMQTT_SUCCESS;
}

int libmqtt__clean_sess(struct libmqtt *mqtt, int clean_sess) {
    if (!mqtt) {
        return LIBMQTT_ERROR_NULL;
    }
    mqtt->c.clean_sess = clean_sess;
    return LIBMQTT_SUCCESS;
}

int libmqtt__version(struct libmqtt *mqtt, enum mqtt_vsn vsn) {
    if (!mqtt) {
        return LIBMQTT_ERROR_NULL;
    }
    if (!MQTT_IS_VER(vsn)) {
        return LIBMQTT_ERROR_VSN;
    }
    mqtt->c.proto_ver = vsn;
    return LIBMQTT_SUCCESS;
}

int libmqtt__auth(struct libmqtt *mqtt, const char *username, const char *password) {
    if (!mqtt) {
        return LIBMQTT_ERROR_NULL;
    }
    mqtt_b_free(&mqtt->c.username);
    mqtt_b_free(&mqtt->c.password);
    if (username) {
        mqtt_b_dup(&mqtt->c.username, username);
        if (mqtt_b_empty(&mqtt->c.username)) {
            return LIBMQTT_ERROR_MALLOC;
        }
    }
    if (password) {
        mqtt_b_dup(&mqtt->c.password, password);
        if (mqtt_b_empty(&mqtt->c.username)) {
            return LIBMQTT_ERROR_MALLOC;
        }
    }
    return LIBMQTT_SUCCESS;
}

int libmqtt__will(struct libmqtt *mqtt, int retain, enum mqtt_qos qos, const char *topic,
                  const char *payload, int payload_len) {
    if (!topic) {
        mqtt->c.will_flag = 0;
        return LIBMQTT_SUCCESS;
    }
    mqtt->c.will_flag = 1;
    mqtt->c.will_retain = retain;
    mqtt->c.will_qos = qos;
    mqtt_b_free(&mqtt->c.will_topic);
    mqtt_b_free(&mqtt->c.will_payload);
    mqtt_b_dup(&mqtt->c.will_topic, topic);
    if (payload && payload_len > 0) {
        mqtt->c.will_payload.s = malloc(payload_len);
        memcpy(mqtt->c.will_payload.s, payload, payload_len);
        mqtt->c.will_payload.n = payload_len;
    }
    return LIBMQTT_SUCCESS;
}

int libmqtt__connect(struct libmqtt *mqtt, const char *host, int port) {
    struct mqtt_packet p;
    struct mqtt_b b;
    int rc;

    if (!mqtt) {
        return LIBMQTT_ERROR_NULL;
    }
    mqtt->host = strdup(host);
    mqtt->port = port;
    if (!mqtt->host) {
        return LIBMQTT_ERROR_MALLOC;
    }

    memset(&p, 0, sizeof p);
    p.h.type = CONNECT;
    p.v.connect = mqtt->c;
    p.v.connect.proto_name.s = (char *)MQTT_PROTOCOL_NAMES[mqtt->c.proto_ver];
    p.v.connect.proto_name.n = strlen(p.v.connect.proto_name.s);

    if (mqtt__serialize(&p, &b)) {
        return LIBMQTT_ERROR_MALLOC;
    }

    if (__connect(mqtt)) {
        mqtt_b_free(&b);
        return LIBMQTT_ERROR_CONNECT;
    }
    rc = __write(mqtt, b.s, b.n);
    mqtt_b_free(&b);
    if (rc) {
        return LIBMQTT_ERROR_WRITE;
    }
    __log(mqtt, "sending CONNECT (%s, c%d, k%d, u\'%.*s\', p\'%.*s\')", MQTT_PROTOCOL_NAMES[mqtt->c.proto_ver],
          mqtt->c.clean_sess, mqtt->c.keep_alive, mqtt->c.username.n, mqtt->c.username.s,
          mqtt->c.password.n, mqtt->c.password.s);
    return LIBMQTT_SUCCESS;
}

int libmqtt__subscribe(struct libmqtt *mqtt, uint16_t *id, int count, const char *topic[], enum mqtt_qos qos[]) {
    struct mqtt_packet p;
    struct mqtt_b b;
    int rc, i;

    if (!mqtt) {
        return LIBMQTT_ERROR_NULL;
    }
    if (count > MQTT_MAX_SUB) {
        return LIBMQTT_ERROR_MAXSUB;
    }
    memset(&p, 0, sizeof p);
    p.h.type = SUBSCRIBE;
    p.v.subscribe.packet_id = __generate_packet_id(mqtt);
    for (i = 0; i < count; i++) {
        p.v.subscribe.topic_name[i].s = (char *)topic[i];
        p.v.subscribe.topic_name[i].n = strlen(topic[i]);
        p.v.subscribe.qos[i] = qos[i];
    }
    p.v.subscribe.n = count;

    if (mqtt__serialize(&p, &b)) {
        return LIBMQTT_ERROR_MALLOC;
    }

    if (id) {
        *id = p.v.subscribe.packet_id;
    }
    rc = __write(mqtt, b.s, b.n);
    mqtt_b_free(&b);
    if (rc) {
        return LIBMQTT_ERROR_WRITE;
    }
    for (i = 0; i < count; i++) {
        __log(mqtt, "Sending SUBSCRIBE (id: %"PRIu16", Topic: %s, QoS: %d)",
              p.v.subscribe.packet_id, topic[i], qos[i]);
    }
    return LIBMQTT_SUCCESS;
}

int libmqtt__unsubscribe(struct libmqtt *mqtt, uint16_t *id, int count, const char *topic[]) {
    struct mqtt_packet p;
    struct mqtt_b b;
    int rc, i;

    if (!mqtt) {
        return LIBMQTT_ERROR_NULL;
    }
    if (count > MQTT_MAX_SUB) {
        return LIBMQTT_ERROR_MAXSUB;
    }
    memset(&p, 0, sizeof p);
    p.h.type = UNSUBSCRIBE;
    p.v.unsubscribe.packet_id = __generate_packet_id(mqtt);
    for (i = 0; i < count; i++) {
        p.v.unsubscribe.topic_name[i].s = (char *)topic[i];
        p.v.unsubscribe.topic_name[i].n = strlen(topic[i]);
    }
    p.v.unsubscribe.n = count;

    if (mqtt__serialize(&p, &b)) {
        return LIBMQTT_ERROR_MALLOC;
    }

    if (id) {
        *id = p.v.unsubscribe.packet_id;
    }
    rc = __write(mqtt, b.s, b.n);
    mqtt_b_free(&b);
    if (rc) {
        return LIBMQTT_ERROR_WRITE;
    }
    for (i = 0; i < count; i++) {
        __log(mqtt, "Sending UNSUBSCRIBE (id: %"PRIu16", Topic: %s)",
              p.v.unsubscribe.packet_id, topic[i]);
    }
    return LIBMQTT_SUCCESS;
}

int libmqtt__publish(struct libmqtt *mqtt, uint16_t *id, const char *topic,
                     enum mqtt_qos qos, int retain, const char *payload, int length) {
    struct mqtt_packet p;
    struct mqtt_b b;
    enum libmqtt_state s;
    int rc;

    if (!mqtt) {
        return LIBMQTT_ERROR_NULL;
    }
    if (!MQTT_IS_QOS(qos)) {
        return LIBMQTT_ERROR_QOS;
    }
    memset(&p, 0, sizeof p);
    p.h.type = PUBLISH;
    p.h.dup = 0;
    p.h.retain = retain;
    p.h.qos = qos;
    if (qos > MQTT_QOS_0) {
        p.v.publish.packet_id = __generate_packet_id(mqtt);
    }
    p.v.publish.topic_name.s = (char *)topic;
    p.v.publish.topic_name.n = strlen(topic);
    p.payload.s = (char *)payload;
    p.payload.n = length;

    if (mqtt__serialize(&p, &b)) {
        return LIBMQTT_ERROR_MALLOC;
    }

    if (qos > MQTT_QOS_0 && id) {
        *id = p.v.publish.packet_id;
    }
    rc = __write(mqtt, b.s, b.n);
    mqtt_b_free(&b);
    if (!rc) {
        __log(mqtt, "sending PUBLISH (d%d, q%d, r%d, m%d, \'%s\', ...(%d bytes))",
              0, qos, retain, p.v.publish.packet_id, topic, length);
    }
    if (!rc && qos == MQTT_QOS_0) {
        return LIBMQTT_SUCCESS;
    }
    if (rc) {
        s = LIBMQTT_ST_SEND_PUBLUSH;
    } else if (qos == MQTT_QOS_1) {
        s = LIBMQTT_ST_WAIT_PUBACK;
    } else if (qos == MQTT_QOS_2) {
        s = LIBMQTT_ST_WAIT_PUBREC;
    } else {
        return LIBMQTT_ERROR_QOS;
    }
    if (__insert_pub(mqtt, &p, LIBMQTT_DIR_OUT, s)) {
        return LIBMQTT_ERROR_MALLOC;
    }
    return LIBMQTT_SUCCESS;
}

int libmqtt__disconnect(struct libmqtt *mqtt) {
    char b[] = MQTT_DISCONNECT;
    int rc;

    if (!mqtt) {
        return LIBMQTT_ERROR_NULL;
    }
    rc = __write(mqtt, b, sizeof b);
    shutdown(mqtt->fd, SHUT_WR);
    if (rc) {
        return LIBMQTT_ERROR_WRITE;
    }
    __log(mqtt, "sending DISCONNECT");
    return LIBMQTT_SUCCESS;
}

int libmqtt__run(struct libmqtt *mqtt) {
    if (!mqtt) {
        return LIBMQTT_ERROR_NULL;
    }
    aeMain(mqtt->el);
    return LIBMQTT_SUCCESS;
}
