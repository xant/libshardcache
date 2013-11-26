#include <fbuf.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <errno.h>

#define HAVE_UINT64_T
#include <siphash.h>

#include "messaging.h"
#include "connections.h"
#include "groupcache.h"

int read_message(int fd, char *auth, fbuf_t *out, groupcache_hdr_t *ohdr) {
    uint16_t clen;
    int initial_len = fbuf_used(out);;
    int reading_message = 0;
    char hdr;
    sip_hash *shash = sip_hash_new(auth, 2, 4);
    for(;;) {
        int rb;

        if (reading_message == 0) {
            rb = read_socket(fd, &hdr, 1);
            if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
                sip_hash_free(shash);
                return -1;
            }
            if (hdr != GROUPCACHE_HDR_GET &&
                hdr != GROUPCACHE_HDR_SET &&
                hdr != GROUPCACHE_HDR_DEL &&
                hdr != GROUPCACHE_HDR_EVI &&
                hdr != GROUPCACHE_HDR_RES)
            {
                sip_hash_free(shash);
                return -1;
            }
            sip_hash_update(shash, &hdr, 1);
            if (ohdr)
                *ohdr = hdr;
            reading_message = 1;
        }

        rb = read_socket(fd, (char *)&clen, 2);
        // XXX - bug if read only one bit at this point
        if (rb == 2) {
            sip_hash_update(shash, (char *)&clen, 2);
            uint16_t chunk_len = ntohs(clen);

            if (chunk_len == 0) {
                char sig[GROUPCACHE_MSG_SIG_LEN];
                int ofx = 0;
                do {
                    rb = read_socket(fd, &sig[ofx], GROUPCACHE_MSG_SIG_LEN-ofx);
                    if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
                        fbuf_set_used(out, initial_len);
                        sip_hash_free(shash);
                        return -1;
                    } 
                    ofx += rb;
                } while (ofx != GROUPCACHE_MSG_SIG_LEN);

                uint64_t digest;
                if (!sip_hash_final_integer(shash, &digest)) {
                    // TODO - Error Messages
                    fbuf_set_used(out, initial_len);
                    sip_hash_free(shash);
                    return -1;
                }

#ifdef GROUPCACHE_DEBUG
                int i;
                printf("computed digest for received data: (%s) ", auth);
                for (i=0; i<8; i++) {
                    printf("%02x", (unsigned char)((char *)&digest)[i]);
                }
                printf("\n");

                printf("digest from received data: ");
                uint8_t *remote = sig;
                for (i=0; i<8; i++) {
                    printf("%02x", remote[i]);
                }
                printf("\n");
#endif

                if (memcmp(&digest, &sig, GROUPCACHE_MSG_SIG_LEN) != 0) {
                    fbuf_set_used(out, initial_len);
                    sip_hash_free(shash);
                    return -1;
                    // AUTH FAILED
                }
                sip_hash_free(shash);
                return 0;
            }

            while (chunk_len != 0) {
                char buf[chunk_len];
                rb = read_socket(fd, buf, chunk_len);
                if (rb == -1) {
                    if (errno != EINTR && errno != EAGAIN) {
                        // ERROR 
                        fbuf_set_used(out, initial_len);
                        sip_hash_free(shash);
                        return -1;
                    }
                } else if (rb == 0) {
                    fbuf_set_used(out, initial_len);
                    sip_hash_free(shash);
                    return -1;
                }
                chunk_len -= rb;
                fbuf_add_binary(out, buf, rb);
                sip_hash_update(shash, buf, rb);
                if (fbuf_used(out) > GROUPCACHE_MSG_MAX_RECORD_LEN) {
                    // we have exceeded the maximum size for a record
                    // let's abort this request
                    fprintf(stderr, "Maximum record size exceeded (%dMB)", GROUPCACHE_MSG_MAX_RECORD_LEN >> 20);
                    fbuf_set_used(out, initial_len);
                    sip_hash_free(shash);
                    return -1;
                }
            }
        } else if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
            // ERROR 
            break;
        }
    }
    sip_hash_free(shash);
    return -1;
}

int _chunkize_buffer(void *buf, size_t blen, fbuf_t *out) {
    int orig_used = fbuf_used(out);
    do {
        int writelen = (blen > (size_t)UINT16_MAX) ? UINT16_MAX : blen;
        blen -= writelen;
        uint16_t size = htons(writelen);
        int wb = fbuf_add_binary(out, (char *)&size, 2);
        if (wb == -1) {
            return -1;
        } else if (wb == 2) {
            int wrote = 0;
            while (wrote != writelen) {
                wb = fbuf_add_binary(out, buf + wrote, writelen - wrote);
                if (wb == -1) {
                    // discard what written so far
                    fbuf_set_used(out, orig_used);
                    return -1;
                }
                wrote += wb;
            }
            if (wrote == writelen) {
                uint16_t terminator = 0;
                fbuf_add_binary(out, (char *)&terminator, 2);
                return 0;
            }
        }
    } while (blen != 0);
    return -1;
}

int build_message(char hdr, void *k, size_t klen, void *v, size_t vlen, fbuf_t *out) {
    fbuf_clear(out);
    fbuf_add_binary(out, &hdr, 1);
    if (k && klen) {
        if (_chunkize_buffer(k, klen, out) != 0)
            return -1;
    }
    if (hdr == GROUPCACHE_HDR_SET && v && vlen) {
        if (_chunkize_buffer(v, vlen, out) != 0)
            return -1;
    }
    return 0;
}

int write_message(int fd, char *auth, char hdr, void *k, size_t klen, void *v, size_t vlen)  {

    fbuf_t msg = FBUF_STATIC_INITIALIZER;
    if (build_message(hdr, k, klen, v, vlen, &msg) != 0) {
        // TODO - Error Messages
        fbuf_destroy(&msg);
        return -1;
    }

    if (auth) {
        uint64_t digest;
        size_t dlen = sizeof(digest);
        sip_hash *shash = sip_hash_new(auth, 2, 4);
        sip_hash_digest_integer(shash, fbuf_data(&msg), fbuf_used(&msg), &digest);
        sip_hash_free(shash);
        fbuf_add_binary(&msg, (char *)&digest, dlen);

#ifdef GROUPCACHE_DEBUG
        int i;
        printf("sending message: ");
        for (i = 0; i < fbuf_used(&msg) - dlen; i++) {
            printf("%02x", (unsigned char)(fbuf_data(&msg))[i]);
        }
        printf("\n");

        printf("computed digest: ");
        for (i=0; i < dlen; i++) {
            printf("%02x", (unsigned char)((char *)&digest)[i]);
        }
        printf("\n");
#endif
    }

    while(fbuf_used(&msg) > 0) {
        int wb = fbuf_write(&msg, fd, 0);
        if (wb == 0 || (wb == -1 && errno != EINTR && errno != EAGAIN)) {
            fbuf_destroy(&msg);
            return -1;
        }
    }
    fbuf_destroy(&msg);
    return 0;
}


int delete_from_peer(char *peer, char *auth, void *key, size_t klen, int owner) {
    char *brkt = NULL;
    char *addr = strdup(peer);
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : GROUPCACHE_PORT_DEFAULT;
    int fd = open_connection(host, port, 30);
    free(addr);

    if (fd >= 0) {

        int rc = write_message(fd, auth, owner ? GROUPCACHE_HDR_DEL : GROUPCACHE_HDR_EVI, key, klen, NULL, 0);

        if (rc == 0) {
            groupcache_hdr_t hdr = 0;
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == GROUPCACHE_HDR_RES && rc == 0) {
#ifdef DEBUG_GROUPCACHE
                fprintf(stderr, "Got (set) response from peer %s : %s\n", peer, fbuf_data(&resp));
#endif
                close(fd);
                fbuf_destroy(&resp);
                return 0;
            } else {
                // TODO - Error messages
            }
            fbuf_destroy(&resp);
        }
        close(fd);
    }
    return -1;
}


int send_to_peer(char *peer, char *auth, void *key, size_t klen, void *value, size_t vlen) {
    char *brkt = NULL;
    char *addr = strdup(peer);
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : GROUPCACHE_PORT_DEFAULT;
    int fd = open_connection(host, port, 30);
    free(addr);

    if (fd >= 0) {
        int rc = write_message(fd, auth, GROUPCACHE_HDR_SET, key, klen, value, vlen);
        if (rc != 0) {
            close(fd);
            return -1;
        }

        if (rc == 0) {
            groupcache_hdr_t hdr = 0;
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            errno = 0;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == GROUPCACHE_HDR_RES && rc == 0) {
#ifdef DEBUG_GROUPCACHE
                fprintf(stderr, "Got (set) response from peer %s : %s\n", peer, fbuf_data(&resp));
#endif
                close(fd);
                fbuf_destroy(&resp);
                return 0;
            } else {
                fprintf(stderr, "Bad response (%02x) from %s : %s\n", hdr, peer, strerror(errno));
            }
            fbuf_destroy(&resp);
        } else {
            fprintf(stderr, "Error reading from socket %d (%s) : %s\n", fd, peer, strerror(errno));
        }
        close(fd);
    }
    return -1;
}

int fetch_from_peer(char *peer, char *auth, void *key, size_t len, fbuf_t *out) {
    char *brkt = NULL;
    char *addr = strdup(peer);
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : GROUPCACHE_PORT_DEFAULT;
    int fd = open_connection(host, port, 30);
    free(addr);

    if (fd >= 0) {
        int rc = write_message(fd, auth, GROUPCACHE_HDR_GET, key, len, NULL, 0);
        if (rc == 0) {
            groupcache_hdr_t hdr = 0;
            int rc = read_message(fd, auth, out, &hdr);
            if (hdr == GROUPCACHE_HDR_RES && rc == 0) {
#ifdef DEBUG_GROUPCACHE
                fprintf(stderr, "Got new data from peer %s : %s => %s \n", peer, key, fbuf_data(out));
#endif
                close(fd);
                return 0;
            } else {
                // TODO - Error messages
            }
        }
        close(fd);
    }
    return -1;
}


