#include <sys/types.h>

/* protocol specification
 *
 *  GET_MESSAGE      : <AUTH><HDR><KEY>
 *  SET_MESSAGE      : <AUTH><HDR><KEY><VALUE>
 *  DEL_MESSAGE      : <AUTH><HDR><KEY>
 *  EVI_MESSAGE      : <AUTH><HDR><KEY>
 *  RESP_MESSAGE     : <HDR><VALUE>
 *
 *  AUTH      : <BYTE>[20]
 *  HDR       : MSG_GET | MSG_SET | MSG_DEL | MSG_RESP
 *  MSG_GET   : 0x01
 *  MSG_SET   : 0x02
 *  MSG_DEL   : 0x03
 *  MSG_EVI   : 0x04
 *  MSG_RESP  : 0x11
 *  KEY       : <RECORD>
 *  VALUE     : <RECORD>
 *  RECORD    : <SIZE><DATA>[<SIZE><DATA>...]<EOR>
 *  SIZE      : <HIGH_BYTE><LOW_BYTE>
 *  DATA      : <BYTE>...<BYTE>
 *  EOR       : <NULL_BYTE><NULL_BYTE>
 *  HIGH_BYTE : BYTE
 *  LOW_BYTE  : BYTE
 *  NULL_BYTE : 0x00
 *  BYTE      : 0x00 - 0xFF
 */

#define GROUPCACHE_SIG_LEN 8

typedef enum {
    GROUPCACHE_HDR_GET  = 0x01,
    GROUPCACHE_HDR_SET  = 0x02,
    GROUPCACHE_HDR_DEL  = 0x03,
    GROUPCACHE_HDR_EVI  = 0x04,
    GROUPCACHE_HDR_RES  = 0x11
} groupcache_hdr_t;

int read_message(int fd, char *auth, fbuf_t *out, groupcache_hdr_t *hdr);
int write_message(int fd, char *auth, char hdr, void *k, size_t klen, void *v, size_t vlen);
int delete_from_peer(char *peer, char *auth, void *key, size_t klen, int owner);
int send_to_peer(char *peer, char *auth, void *key, size_t klen, void *value, size_t vlen);
int fetch_from_peer(char *peer, char *auth, void *key, size_t len, fbuf_t *out);

