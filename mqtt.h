/*
 * mqtt.h -- mqtt defines, structures and utils.
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

#ifndef _MQTT_H_
#define _MQTT_H_

#ifdef __cplusplus
extern "C" {
#endif

/* max topic/qos per subscribe or unsubscribe. */
#define MQTT_MAX_SUB 128

/* generic includes. */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__GNUC__) && (__GNUC__ >= 4)
# define MQTT_API __attribute__((visibility("default")))
#else
# define MQTT_API
#endif


enum mqtt_vsn {
    MQTT_PROTO_V3 = 0x03,
    MQTT_PROTO_V4 = 0x04,
};

#define MQTT_IS_VER(v) (v == MQTT_PROTO_V3 || v == MQTT_PROTO_V4)

static const char *MQTT_PROTOCOL_NAMES[] = {
    [MQTT_PROTO_V3] = "MQIsdp",
    [MQTT_PROTO_V4] = "MQTT",
};

enum mqtt_qos {
    MQTT_QOS_0 = 0x00,
    MQTT_QOS_1 = 0x01,
    MQTT_QOS_2 = 0x02,
    MQTT_QOS_F = 0x80,
};

#define MQTT_IS_QOS(q) (q >= MQTT_QOS_0 && q <= MQTT_QOS_2)

enum mqtt_p_type {
    RESERVED    = 0x00,
    CONNECT     = 0x01,
    CONNACK     = 0x02,
    PUBLISH     = 0x03,
    PUBACK      = 0x04,
    PUBREC      = 0x05,
    PUBREL      = 0x06,
    PUBCOMP     = 0x07,
    SUBSCRIBE   = 0x08,
    SUBACK      = 0x09,
    UNSUBSCRIBE = 0x0A,
    UNSUBACK    = 0x0B,
    PINGREQ     = 0x0C,
    PINGRESP    = 0x0D,
    DISCONNECT  = 0x0E,
};

#define MQTT_MAX_TYPE (DISCONNECT+1)

#define MQTT_IS_TYPE(t) (t >= CONNECT && t <= DISCONNECT)

static const char *MQTT_TYPE_NAMES[] = {
    [RESERVED]      = "RESERVED",
    [CONNECT]       = "CONNECT",
    [CONNACK]       = "CONNACK",
    [PUBLISH]       = "PUBLISH",
    [PUBACK]        = "PUBACK",
    [PUBREC]        = "PUBREC",
    [PUBREL]        = "PUBREL",
    [PUBCOMP]       = "PUBCOMP",
    [SUBSCRIBE]     = "SUBSCRIBE",
    [SUBACK]        = "SUBACK",
    [UNSUBSCRIBE]   = "UNSUBSCRIBE",
    [UNSUBACK]      = "UNSUBACK",
    [PINGREQ]       = "PINGREQ",
    [PINGRESP]      = "PINGRESP",
    [DISCONNECT]    = "DISCONNECT",
};

enum mqtt_connack {
    CONNACK_ACCEPTED                        = 0x00,
    CONNACK_REFUSED_PROTOCOL_VERSION        = 0x01,
    CONNACK_REFUSED_IDENTIFIER_REJECTED     = 0x02,
    CONNACK_REFUSED_SERVER_UNAVAILABLE      = 0x03,
    CONNACK_REFUSED_BAD_USERNAME_PASSWORD   = 0x04,
    CONNACK_REFUSED_NOT_AUTHORIZED          = 0x05,
};

#define MQTT_IS_CONNACK(c) (c >= CONNACK_ACCEPTED && c <= CONNACK_REFUSED_NOT_AUTHORIZED)

static const char *MQTT_CONNACK_NAMES[] = {
    [CONNACK_ACCEPTED]                      = "CONNACK_ACCEPTED",
    [CONNACK_REFUSED_PROTOCOL_VERSION]      = "CONNACK_REFUSED_PROTOCOL_VERSION",
    [CONNACK_REFUSED_IDENTIFIER_REJECTED]   = "CONNACK_REFUSED_IDENTIFIER_REJECTED",
    [CONNACK_REFUSED_SERVER_UNAVAILABLE]    = "CONNACK_REFUSED_SERVER_UNAVAILABLE",
    [CONNACK_REFUSED_BAD_USERNAME_PASSWORD] = "CONNACK_REFUSED_BAD_USERNAME_PASSWORD",
    [CONNACK_REFUSED_NOT_AUTHORIZED]        = "CONNACK_REFUSED_NOT_AUTHORIZED",
};


#define MQTT_PINGREQ            {0xc0, 0x00}
#define MQTT_PINGRESP           {0xd0, 0x00}
#define MQTT_DISCONNECT         {0xe0, 0x00}
#define MQTT_PUBACK(id)         {0x40, 0x02, (((id)&0xff00)>>8), ((id)&0x00ff)}
#define MQTT_PUBREC(id)         {0x50, 0x02, (((id)&0xff00)>>8), ((id)&0x00ff)}
#define MQTT_PUBREL(id)         {0x62, 0x02, (((id)&0xff00)>>8), ((id)&0x00ff)}
#define MQTT_PUBCOMP(id)        {0x70, 0x02, (((id)&0xff00)>>8), ((id)&0x00ff)}
#define MQTT_UNSUBACK(id)       {0xb0, 0x02, (((id)&0xff00)>>8), ((id)&0x00ff)}
#define MQTT_CONNACK(caf, crc)  {0x20, 0x02, caf, crc}

struct mqtt_p_header {
    enum mqtt_p_type type;
    enum mqtt_qos qos;
    int dup;
    int retain;
};

struct mqtt_b {
    char *s;
    int n;
};

struct mqtt_p_connect {
    struct mqtt_b client_id;
    enum mqtt_vsn proto_ver;
    struct mqtt_b proto_name;
    int will_retain;
    enum mqtt_qos will_qos;
    int will_flag;
    int clean_sess;
    uint16_t keep_alive;
    struct mqtt_b will_topic;
    struct mqtt_b will_payload;
    struct mqtt_b username;
    struct mqtt_b password;
};

struct mqtt_p_connack {
    int ack_flags;
    enum mqtt_connack return_code;
};

struct mqtt_p_publish {
    struct mqtt_b topic_name;
    uint16_t packet_id;
};

struct mqtt_p_puback {
    uint16_t packet_id;
};

struct mqtt_p_pubrec {
    uint16_t packet_id;
};

struct mqtt_p_pubrel {
    uint16_t packet_id;
};

struct mqtt_p_pubcomp {
    uint16_t packet_id;
};

struct mqtt_p_subscribe {
    uint16_t packet_id;
    struct mqtt_b topic_name[MQTT_MAX_SUB];
    enum mqtt_qos qos[MQTT_MAX_SUB];
    int n;
};

struct mqtt_p_suback {
    uint16_t packet_id;
    enum mqtt_qos qos[MQTT_MAX_SUB];
    int n;
};

struct mqtt_p_unsubscribe {
    uint16_t packet_id;
    struct mqtt_b topic_name[MQTT_MAX_SUB];
    int n;
};

struct mqtt_p_unsuback {
    uint16_t packet_id;
};

struct mqtt_p_pingreq {};

struct mqtt_p_pingresp {};

struct mqtt_p_disconnect {};

struct mqtt_packet {
    struct mqtt_p_header h;
    union {
        struct mqtt_p_connect connect;
        struct mqtt_p_connack connack;
        struct mqtt_p_publish publish;
        struct mqtt_p_puback puback;
        struct mqtt_p_pubrec pubrec;
        struct mqtt_p_pubrel pubrel;
        struct mqtt_p_pubcomp pubcomp;
        struct mqtt_p_subscribe subscribe;
        struct mqtt_p_suback suback;
        struct mqtt_p_unsubscribe unsubscribe;
        struct mqtt_p_unsuback unsuback;
        struct mqtt_p_pingreq pingreq;
        struct mqtt_p_pingresp pingresp;
        struct mqtt_p_disconnect disconnect;
    } v;
    struct mqtt_b payload;
};

enum mqtt_parser_state {
    MQTT_ST_FIXED,
    MQTT_ST_LENGTH,
    MQTT_ST_REMAIN,
};

typedef int (*mqtt_cb)(void *, struct mqtt_packet *);

struct mqtt_parser {
    int auth;
    enum mqtt_parser_state state;
    int require;
    int multiplier;
    struct mqtt_b remaining;
    struct mqtt_packet p;
    mqtt_cb cb[MQTT_MAX_TYPE];
};


static inline void
mqtt_b_dup(struct mqtt_b *b, const char *s) {
    if (s) {
        b->s = strdup(s);
        b->n = strlen(s);
    }
}

static inline void
mqtt_b_copy(struct mqtt_b *b, struct mqtt_b *s) {
    if (s->s && s->n > 0) {
        b->s = malloc(s->n);
        memcpy(b->s, s->s, s->n);
        b->n = s->n;
    }
}

static inline int
mqtt_b_empty(struct mqtt_b *b) {
    return (!b->s || !b->n);
}

static inline void
mqtt_b_free(struct mqtt_b *b) {
    if (b->s) {
        free(b->s);
        b->s = 0;
        b->n = 0;
    }
}

static inline void
mqtt_b_read_utf(struct mqtt_b *b, struct mqtt_b *r) {
    r->n = ((*(b->s) << 8) + *(b->s + 1));
    b->s += 2;
    b->n -= 2;
    if (r->n > 0) {
        r->s = b->s;
        b->s += r->n;
        b->n -= r->n;
    }
}

static inline int
mqtt_b_read_u8(struct mqtt_b *b) {
    int u8;
    u8 = *(b->s);
    b->s += 1;
    b->n -= 1;
    return u8;
}

static inline int
mqtt_b_read_u16(struct mqtt_b *b) {
    int u16;
    u16 = ((*b->s << 8) + *(b->s + 1));
    b->s += 2;
    b->n -= 2;
    return u16;
}

static inline void
mqtt_b_write_utf(struct mqtt_b *b, struct mqtt_b *r) {
    b->s[b->n++] = (r->n & 0xff00) >> 8;
    b->s[b->n++] = r->n & 0x00ff;
    if (r->n > 0) {
        memcpy(&b->s[b->n], r->s, r->n);
        b->n += r->n;
    }
}

static inline void
mqtt_b_write_u8(struct mqtt_b *b, uint8_t r) {
    b->s[b->n++] = r;
}

static inline void
mqtt_b_write_u16(struct mqtt_b *b, uint16_t r) {
    b->s[b->n++] = (r & 0xff00) >> 8;
    b->s[b->n++] = r & 0x00ff;
}

extern MQTT_API int mqtt__serialize(struct mqtt_packet *pkt, struct mqtt_b *b);

extern MQTT_API void mqtt__parse_init(struct mqtt_parser *p);
extern MQTT_API void mqtt__parse_cb(struct mqtt_parser *p, enum mqtt_p_type t, mqtt_cb cb);
extern MQTT_API int mqtt__parse(struct mqtt_parser *p, void *ud, struct mqtt_b *b);

#ifdef __cplusplus
}
#endif

#endif /* _MQTT_H_ */


#ifdef MQTT_IMPLEMENTATION

void
mqtt__parse_init(struct mqtt_parser *p) {
    memset(p, 0, sizeof *p);
    p->state = MQTT_ST_FIXED;
}

void
mqtt__parse_cb(struct mqtt_parser *p, enum mqtt_p_type t, mqtt_cb cb) {
    if (MQTT_IS_TYPE(t)) {
        p->cb[t] = cb;
    }
}

static int
__process_connect(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    struct mqtt_p_connect *c;

    c = &p->v.connect;
    if (0 == c->clean_sess && mqtt_b_empty(&c->client_id))
        return -1;
    if (c->will_flag) {
        if (mqtt_b_empty(&c->will_topic) || mqtt_b_empty(&c->will_payload))
            return -1;
        if (!MQTT_IS_QOS(c->will_qos))
            return -1;
    } else {
        if (!mqtt_b_empty(&c->will_topic) || !mqtt_b_empty(&c->will_payload))
            return -1;
        if (c->will_qos || c->will_retain)
            return -1;
    }
    return cb(ud, p);
}

static int
__process_connack(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    struct mqtt_p_connack *c;

    c = &p->v.connack;
    if (!MQTT_IS_CONNACK(c->return_code)) {
        return -1;
    }
    return cb(ud, p);
}

static int
__process_publish(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    struct mqtt_p_publish *c;

    c = &p->v.publish;
    if (!MQTT_IS_QOS(p->h.qos)) {
        return -1;
    }
    if (mqtt_b_empty(&c->topic_name)) {
        return -1;
    }
    return cb(ud, p);
}

static int
__process_puback(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    return cb(ud, p);
}

static int
__process_pubrec(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    return cb(ud, p);
}

static int
__process_pubrel(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    if (p->h.qos != MQTT_QOS_1) {
        return -1;
    }
    return cb(ud, p);
}

static int
__process_pubcomp(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    return cb(ud, p);
}

static int
__process_subscribe(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    struct mqtt_p_subscribe *c;
    int i;

    c = &p->v.subscribe;
    if (p->h.qos != MQTT_QOS_1) {
        return -1;
    }
    for (i = 0; i < c->n; i++) {
        if (mqtt_b_empty(&c->topic_name[i])) {
            return -1;
        }
    }
    return cb(ud, p);
}

static int
__process_suback(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    return cb(ud, p);
}

static int
__process_unsubscribe(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    struct mqtt_p_unsubscribe *c;
    int i;

    c = &p->v.unsubscribe;
    if (p->h.qos != MQTT_QOS_1) {
        return -1;
    }
    for (i = 0; i < c->n; i++) {
        if (mqtt_b_empty(&c->topic_name[i])) {
            return -1;
        }
    }
    return cb(ud, p);
}

static int
__process_unsuback(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    return cb(ud, p);
}

static int
__process_pingreq(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    return cb(ud, p);
}

static int
__process_pingresp(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    return cb(ud, p);
}

static int
__process_disconnect(struct mqtt_packet *p, void *ud, mqtt_cb cb) {
    return cb(ud, p);
}


static int
__parse_connect(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    int flags;

    if (remaining->n <= 2) return -1;
    mqtt_b_read_utf(remaining, &pkt->v.connect.proto_name);
    if (remaining->n < 1) return -1;
    pkt->v.connect.proto_ver = mqtt_b_read_u8(remaining);
    if (remaining->n < 1) return -1;

    flags = mqtt_b_read_u8(remaining);
    pkt->v.connect.clean_sess = ((flags >> 1) & 0x01);
    pkt->v.connect.will_flag = ((flags >> 2) & 0x01);
    pkt->v.connect.will_qos = ((flags >> 3) & 0x03);
    pkt->v.connect.will_retain = ((flags >> 5) & 0x01);

    if (remaining->n < 2) return -1;
    pkt->v.connect.keep_alive = mqtt_b_read_u16(remaining);
    if (remaining->n < 2) return -1;
    mqtt_b_read_utf(remaining, &pkt->v.connect.client_id);
    if (pkt->v.connect.will_flag) {
        if (remaining->n <= 2) return -1;
        mqtt_b_read_utf(remaining, &pkt->v.connect.will_topic);
        if (remaining->n <= 2) return -1;
        mqtt_b_read_utf(remaining, &pkt->v.connect.will_payload);
    }
    if ((flags >> 7) & 0x01) {
        if (remaining->n <= 2) return -1;
        mqtt_b_read_utf(remaining, &pkt->v.connect.username);
        if ((flags >> 6) & 0x01) {
            if (remaining->n <= 2) return -1;
            mqtt_b_read_utf(remaining, &pkt->v.connect.password);
        }
    }
    return 0;
}

static int
__parse_connack(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    if (remaining->n != 2) return -1;
    pkt->v.connack.ack_flags = mqtt_b_read_u8(remaining);
    pkt->v.connack.return_code = mqtt_b_read_u8(remaining);
    return 0;
}

static int
__parse_publish(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    if (remaining->n <= 2) return -1;
    mqtt_b_read_utf(remaining, &pkt->v.publish.topic_name);
    if (pkt->h.qos > MQTT_QOS_0) {
        if (remaining->n <= 2) return -1;
        pkt->v.publish.packet_id = mqtt_b_read_u16(remaining);
    }
    pkt->payload = *remaining;
    return 0;
}

static int
__parse_puback(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    if (remaining->n != 2) return -1;
    pkt->v.puback.packet_id = mqtt_b_read_u16(remaining);
    return 0;
}

static int
__parse_pubrec(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    if (remaining->n != 2) return -1;
    pkt->v.puback.packet_id = mqtt_b_read_u16(remaining);
    return 0;
}

static int
__parse_pubrel(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    if (remaining->n != 2) return -1;
    pkt->v.puback.packet_id = mqtt_b_read_u16(remaining);
    return 0;
}

static int
__parse_pubcomp(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    if (remaining->n != 2) return -1;
    pkt->v.puback.packet_id = mqtt_b_read_u16(remaining);
    return 0;
}

static int
__parse_subscribe(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    int rc;
    int n;

    if (remaining->n <= 2) return -1;
    pkt->v.subscribe.packet_id = mqtt_b_read_u16(remaining);

    n = 0;
    rc = 0;
    while (remaining->n > 0 && n < MQTT_MAX_SUB) {
        if (remaining->n <= 3) {
            rc = -1;
            break;
        }
        mqtt_b_read_utf(remaining, &pkt->v.subscribe.topic_name[n]);
        pkt->v.subscribe.qos[n] = mqtt_b_read_u8(remaining);
        if (remaining->n < 0) {
            rc = -1;
            break;
        }
        n++;
    }
    pkt->v.subscribe.n = n;
    return rc;
}

static int
__parse_suback(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    int rc;
    int n;

    if (remaining->n <= 2) return -1;
    pkt->v.subscribe.packet_id = mqtt_b_read_u16(remaining);

    n = 0;
    rc = 0;
    while (remaining->n > 0 && n < MQTT_MAX_SUB) {
        pkt->v.suback.qos[n] = mqtt_b_read_u8(remaining);
        n++;
    }
    pkt->v.suback.n = n;
    return rc;
}

static int
__parse_unsubscribe(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    int rc;
    int n;

    if (remaining->n <= 2) return -1;
    pkt->v.unsubscribe.packet_id = mqtt_b_read_u16(remaining);

    n = 0;
    rc = 0;
    while (remaining->n > 0 && n < MQTT_MAX_SUB) {
        if (remaining->n <= 2) {
            rc = -1;
            break;
        }
        mqtt_b_read_utf(remaining, &pkt->v.unsubscribe.topic_name[n]);
        if (remaining->n < 0) {
            rc = -1;
            break;
        }
        n++;
    }
    pkt->v.unsubscribe.n = n;
    return rc;
}

static int
__parse_unsuback(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    if (remaining->n != 2) return -1;
    pkt->v.unsuback.packet_id = mqtt_b_read_u16(remaining);
    return 0;
}

static int
__parse_pingreq(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    if (remaining->n != 0) return -1;
    return 0;
}

static int
__parse_pingresp(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    if (remaining->n != 0) return -1;
    return 0;
}

static int
__parse_disconnect(struct mqtt_packet *pkt, struct mqtt_b *remaining) {
    if (remaining->n != 0) return -1;
    return 0;
}


static int
__process(struct mqtt_parser *p, void *ud) {
    int rc;
    enum mqtt_p_type type;
    mqtt_cb cb;
    struct mqtt_b b;

    type = p->p.h.type;
    if (p->auth == 0 && (type != CONNECT && type != CONNACK)) {
        return -1;
    }
    cb = p->cb[type];
    if (!cb) {
        return -1;
    }
    b.s = p->remaining.s;
    b.n = p->remaining.n;
    switch (type) {
    case CONNECT:
        rc = __parse_connect(&p->p, &b);
        if (!rc) rc = __process_connect(&p->p, ud, cb);
        break;
    case CONNACK:
        rc = __parse_connack(&p->p, &b);
        if (!rc) rc = __process_connack(&p->p, ud, cb);
        break;
    case PUBLISH:
        rc = __parse_publish(&p->p, &b);
        if (!rc) rc = __process_publish(&p->p, ud, cb);
        break;
    case PUBACK:
        rc = __parse_puback(&p->p, &b);
        if (!rc) rc = __process_puback(&p->p, ud, cb);
        break;
    case PUBREC:
        rc = __parse_pubrec(&p->p, &b);
        if (!rc) rc = __process_pubrec(&p->p, ud, cb);
        break;
    case PUBREL:
        rc = __parse_pubrel(&p->p, &b);
        if (!rc) rc = __process_pubrel(&p->p, ud, cb);
        break;
    case PUBCOMP:
        rc = __parse_pubcomp(&p->p, &b);
        if (!rc) rc = __process_pubcomp(&p->p, ud, cb);
        break;
    case SUBSCRIBE:
        rc = __parse_subscribe(&p->p, &b);
        if (!rc) rc = __process_subscribe(&p->p, ud, cb);
        break;
    case SUBACK:
        rc = __parse_suback(&p->p, &b);
        if (!rc) rc = __process_suback(&p->p, ud, cb);
        break;
    case UNSUBSCRIBE:
        rc = __parse_unsubscribe(&p->p, &b);
        if (!rc) rc = __process_unsubscribe(&p->p, ud, cb);
        break;
    case UNSUBACK:
        rc = __parse_unsuback(&p->p, &b);
        if (!rc) rc = __process_unsuback(&p->p, ud, cb);
        break;
    case PINGREQ:
        rc = __parse_pingreq(&p->p, &b);
        if (!rc) rc = __process_pingreq(&p->p, ud, cb);
        break;
    case PINGRESP:
        rc = __parse_pingresp(&p->p, &b);
        if (!rc) rc = __process_pingresp(&p->p, ud, cb);
        break;
    case DISCONNECT:
        rc = __parse_disconnect(&p->p, &b);
        if (!rc) rc = __process_disconnect(&p->p, ud, cb);
        break;
    default:
        rc = -1;
    }
    if (rc) {
        return rc;
    }
    if (type == CONNECT || type == CONNACK) {
        p->auth = 1;
    }
    return 0;
}

int
mqtt__parse(struct mqtt_parser *p, void *ud, struct mqtt_b *b) {
    const char *c, *e;
    int offset;

    e = b->s + b->n;
    c = b->s;
    while (c < e) {
        switch (p->state) {
        case MQTT_ST_FIXED:
            p->p.h.type = (((*c) >> 4) & 0x0F);
            p->p.h.dup = (((*c) >> 3) & 0x01);
            p->p.h.qos = (((*c) >> 1) & 0x03);
            p->p.h.retain = (((*c) >> 0) & 0x01);
            p->state = MQTT_ST_LENGTH;
            p->multiplier = 1;
            p->remaining.n = 0;
            p->remaining.s = 0;
            p->require = 0;
            c++;
            break;
        case MQTT_ST_LENGTH:
            p->remaining.n += ((*c) & 127) * p->multiplier;
            p->multiplier *= 128;
            if (p->multiplier > 128 * 128 * 128) {
                return -1;
            }
            if (((*c) & 128) == 0) {
                p->require = p->remaining.n;
                if (p->require > 0) {
                    p->state = MQTT_ST_REMAIN;
                    p->remaining.s = malloc(p->remaining.n);
                    if (!p->remaining.s) {
                        return -1;
                    }
                } else {
                    int rc;

                    p->state = MQTT_ST_FIXED;
                    rc = __process(p, ud);
                    mqtt_b_free(&p->remaining);
                    if (rc)
                        return rc;
                }
            }
            c++;
            break;
        case MQTT_ST_REMAIN:
            offset = p->remaining.n - p->require;
            if (e - c >= p->require) {
                int rc;

                memcpy(p->remaining.s + offset, c, p->require);
                c += p->require;
                p->state = MQTT_ST_FIXED;
                rc = __process(p, ud);
                mqtt_b_free(&p->remaining);
                if (rc)
                    return rc;
            } else {
                memcpy(p->remaining.s + offset, c, e - c);
                p->require -= e - c;
                c = e;
            }
            break;
        }
    }
    return 0;
}

static int
__pack_remain_length(int length, char l[]) {
    int n = 0;
    do {
        char c;
        c = length % 128;
        length /= 128;
        if (length > 0)
            c |= 128;
        l[n++] = c;
    } while (length > 0);
    return n;
}

static int
__serialize_connect(struct mqtt_packet *pkt, struct mqtt_b *b) {
    int r_l;
    int flags;
    int l_len;
    char l[4];
    int i;

    flags = 0;
    r_l = 8 + pkt->v.connect.proto_name.n;
    r_l += pkt->v.connect.client_id.n;
    r_l += pkt->v.connect.username.n;
    r_l += pkt->v.connect.password.n;
    if (pkt->v.connect.username.n > 0) {
        flags |= (1 << 7);
        r_l += 2;
        if (pkt->v.connect.password.n > 0) {
            flags |= (1 << 6);
            r_l += 2;
        }
    }
    if (pkt->v.connect.will_flag) {
        r_l += 2 + pkt->v.connect.will_topic.n;
        r_l += 2 + pkt->v.connect.will_payload.n;
        flags |= (1 << 2);
        if (pkt->v.connect.will_retain)
            flags |= (1 << 5);
        flags |= ((pkt->v.connect.will_qos & 0x03) << 3);
    }
    if (pkt->v.connect.clean_sess)
        flags |= (1 << 1);
    l_len = __pack_remain_length(r_l, l);
    b->n = l_len + r_l + 1;
    b->s = malloc(b->n);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0x10);
    for (i = 0; i < l_len; i++)
        mqtt_b_write_u8(b, l[i]);
    mqtt_b_write_utf(b, &pkt->v.connect.proto_name);
    mqtt_b_write_u8(b, (uint8_t)pkt->v.connect.proto_ver);
    mqtt_b_write_u8(b, (uint8_t)flags);
    mqtt_b_write_u16(b, pkt->v.connect.keep_alive);
    mqtt_b_write_utf(b, &pkt->v.connect.client_id);
    if (pkt->v.connect.will_flag) {
        mqtt_b_write_utf(b, &pkt->v.connect.will_topic);
        mqtt_b_write_utf(b, &pkt->v.connect.will_payload);
    }
    if (pkt->v.connect.username.n > 0) {
        mqtt_b_write_utf(b, &pkt->v.connect.username);
        if (pkt->v.connect.password.n > 0)
            mqtt_b_write_utf(b, &pkt->v.connect.password);
    }
    return 0;
}

static int
__serialize_connack(struct mqtt_packet *pkt, struct mqtt_b *b) {
    b->s = malloc(4);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0x20);
    mqtt_b_write_u8(b, 0x02);
    mqtt_b_write_u8(b, (uint8_t)pkt->v.connack.ack_flags);
    mqtt_b_write_u8(b, (uint8_t)pkt->v.connack.return_code);
    return 0;
}

static int
__serialize_publish(struct mqtt_packet *pkt, struct mqtt_b *b) {
    int r_l;
    int l_len;
    char l[4];
    uint8_t h;
    int i;

    h = 0x30;
    if (pkt->h.dup)
        h |= (1 << 3);
    h |= (pkt->h.qos << 1);
    if (pkt->h.retain)
        h |= (1 << 0);
    r_l = 2 + pkt->v.publish.topic_name.n + pkt->payload.n;
    if (pkt->h.qos > MQTT_QOS_0)
        r_l += 2;
    l_len = __pack_remain_length(r_l, l);
    b->n = l_len + r_l + 1;
    b->s = malloc(b->n);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, h);
    for (i = 0; i < l_len; i++)
        mqtt_b_write_u8(b, l[i]);
    mqtt_b_write_utf(b, &pkt->v.publish.topic_name);
    if (pkt->h.qos > MQTT_QOS_0)
        mqtt_b_write_u16(b, pkt->v.publish.packet_id);
    memcpy(&b->s[b->n], pkt->payload.s, pkt->payload.n);
    b->n += pkt->payload.n;
    return 0;
}

static int
__serialize_puback(struct mqtt_packet *pkt, struct mqtt_b *b) {
    b->s = malloc(4);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0x40);
    mqtt_b_write_u8(b, 0x02);
    mqtt_b_write_u16(b, pkt->v.puback.packet_id);
    return 0;
}

static int
__serialize_pubrec(struct mqtt_packet *pkt, struct mqtt_b *b) {
    b->s = malloc(4);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0x50);
    mqtt_b_write_u8(b, 0x02);
    mqtt_b_write_u16(b, pkt->v.pubrec.packet_id);
    return 0;
}

static int
__serialize_pubrel(struct mqtt_packet *pkt, struct mqtt_b *b) {
    b->s = malloc(4);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0x62);
    mqtt_b_write_u8(b, 0x02);
    mqtt_b_write_u16(b, pkt->v.pubrel.packet_id);
    return 0;
}

static int
__serialize_pubcomp(struct mqtt_packet *pkt, struct mqtt_b *b) {
    b->s = malloc(4);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0x70);
    mqtt_b_write_u8(b, 0x02);
    mqtt_b_write_u16(b, pkt->v.pubcomp.packet_id);
    return 0;
}

static int
__serialize_subscribe(struct mqtt_packet *pkt, struct mqtt_b *b) {
    int r_l;
    int l_len;
    char l[4];
    int i;

    r_l = 2;
    for (i = 0; i < pkt->v.subscribe.n; i++)
        r_l += 2 + pkt->v.subscribe.topic_name[i].n + 1;
    l_len = __pack_remain_length(r_l, l);
    b->n = l_len + r_l + 1;
    b->s = malloc(b->n);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0x82);
    for (i = 0; i < l_len; i++)
        mqtt_b_write_u8(b, l[i]);
    mqtt_b_write_u16(b, pkt->v.subscribe.packet_id);
    for (i = 0; i < pkt->v.subscribe.n; i++) {
        mqtt_b_write_utf(b, &pkt->v.subscribe.topic_name[i]);
        mqtt_b_write_u8(b, pkt->v.subscribe.qos[i]);
    }
    return 0;
}

static int
__serialize_suback(struct mqtt_packet *pkt, struct mqtt_b *b) {
    int r_l;
    int l_len;
    char l[4];
    int i;

    r_l = pkt->v.suback.n + 2;
    l_len = __pack_remain_length(r_l, l);
    b->n = l_len + r_l + 1;
    b->s = malloc(b->n);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0x90);
    for (i = 0; i < l_len; i++)
        mqtt_b_write_u8(b, l[i]);
    mqtt_b_write_u16(b, pkt->v.suback.packet_id);
    for (i = 0; i < pkt->v.suback.n; i++)
        mqtt_b_write_u8(b, pkt->v.suback.qos[i]);
    return 0;
}

static int
__serialize_unsubscribe(struct mqtt_packet *pkt, struct mqtt_b *b) {
    int r_l;
    int l_len;
    char l[4];
    int i;

    r_l = 2;
    for (i = 0; i < pkt->v.unsubscribe.n; i++)
        r_l += 2 + pkt->v.unsubscribe.topic_name[i].n;
    l_len = __pack_remain_length(r_l, l);
    b->n = l_len + r_l + 1;
    b->s = malloc(b->n);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0xa2);
    for (i = 0; i < l_len; i++)
        mqtt_b_write_u8(b, l[i]);
    mqtt_b_write_u16(b, pkt->v.unsubscribe.packet_id);
    for (i = 0; i < pkt->v.unsubscribe.n; i++) {
        mqtt_b_write_utf(b, &pkt->v.unsubscribe.topic_name[i]);
    }
    return 0;
}

static int
__serialize_unsuback(struct mqtt_packet *pkt, struct mqtt_b *b) {
    b->s = malloc(4);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0xb0);
    mqtt_b_write_u8(b, 0x02);
    mqtt_b_write_u16(b, pkt->v.unsuback.packet_id);
    return 0;
}

static int
__serialize_pingreq(struct mqtt_packet *pkt, struct mqtt_b *b) {
    b->s = malloc(2);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0xc0);
    mqtt_b_write_u8(b, 0x00);
    return 0;
}

static int
__serialize_pingresp(struct mqtt_packet *pkt, struct mqtt_b *b) {
    b->s = malloc(2);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0xd0);
    mqtt_b_write_u8(b, 0x00);
    return 0;
}

static int
__serialize_disconnect(struct mqtt_packet *pkt, struct mqtt_b *b) {
    b->s = malloc(2);
    if (!b->s) return -1;
    b->n = 0;
    mqtt_b_write_u8(b, 0xe0);
    mqtt_b_write_u8(b, 0x00);
    return 0;
}

int
mqtt__serialize(struct mqtt_packet *pkt, struct mqtt_b *b) {
    b->n = 0;
    b->s = 0;
    switch (pkt->h.type) {
    case CONNECT:
        return __serialize_connect(pkt, b);
    case CONNACK:
        return __serialize_connack(pkt, b);
    case PUBLISH:
        return __serialize_publish(pkt, b);
    case PUBACK:
        return __serialize_puback(pkt, b);
    case PUBREC:
        return __serialize_pubrec(pkt, b);
    case PUBREL:
        return __serialize_pubrel(pkt, b);
    case PUBCOMP:
        return __serialize_pubcomp(pkt, b);
    case SUBSCRIBE:
        return __serialize_subscribe(pkt, b);
    case SUBACK:
        return __serialize_suback(pkt, b);
    case UNSUBSCRIBE:
        return __serialize_unsubscribe(pkt, b);
    case UNSUBACK:
        return __serialize_unsuback(pkt, b);
    case PINGREQ:
        return __serialize_pingreq(pkt, b);
    case PINGRESP:
        return __serialize_pingresp(pkt, b);
    case DISCONNECT:
        return __serialize_disconnect(pkt, b);
    default:
        return -1;
    }
}

#endif // MQTT_IMPLEMENTATION
