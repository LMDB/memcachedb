/*
 *  MemcacheDB - A distributed key-value storage system designed for persistence:
 *
 *      https://gitorious.org/mdb/memcachedb
 *
 *  Based on the BerkeleyDB version at:
 *
 *      http://memcachedb.googlecode.com
 *
 *  The source code of Memcachedb is most based on Memcached:
 *
 *      http://danga.com/memcached/
 *
 *  Copyright 2012 Howard Chu.  All rights reserved.
 *  Copyright 2008 Steve Chu.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Steve Chu <stvchu@gmail.com>
 *      Howard Chu <hyc@symas.com>
 *
 */
 
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <event.h>
#include <netdb.h>
#include <inttypes.h>
#include <mdb.h>

#define DATA_BUFFER_SIZE 2048
#define UDP_READ_BUFFER_SIZE 65536
#define UDP_MAX_PAYLOAD_SIZE 1400
#define UDP_HEADER_SIZE 8
#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)
/* I'm told the max legnth of a 64-bit num converted to string is 20 bytes.
 * Plus a few for spaces, \r\n, \0 */
#define SUFFIX_SIZE 24

/** Initial size of list of items being returned by "get". */
#define ITEM_LIST_INITIAL 200

/** Initial size of the sendmsg() scatter/gather array. */
#define IOV_LIST_INITIAL 400

/** Initial number of sendmsg() argument structures to allocate. */
#define MSG_LIST_INITIAL 10

/** High water marks for buffer shrinking */
#define READ_BUFFER_HIGHWAT 8192
#define ITEM_LIST_HIGHWAT 400
#define IOV_LIST_HIGHWAT 600
#define MSG_LIST_HIGHWAT 100

#define MAX_REP_PRIORITY 1000000
#define MAX_REP_ACK_POLICY 6
#define MAX_REP_NSITES 1000

/* Default path for a database, and its env home */
#define DBHOME "/data1/memcachedb"

#define RGET_MAX_ITEMS 100

/* Get a consistent bool type */
#if HAVE_STDBOOL_H
# include <stdbool.h>
#else
  typedef enum {false = 0, true = 1} bool;
#endif

#if HAVE_STDINT_H
# include <stdint.h>
#else
 typedef unsigned char             uint8_t;
#endif

/* unistd.h is here */
#if HAVE_UNISTD_H
# include <unistd.h>
#endif

/* 64-bit Portable printf */
/* printf macros for size_t, in the style of inttypes.h */
#ifdef _LP64
#define __PRIS_PREFIX "z"
#else
#define __PRIS_PREFIX
#endif

/* Use these macros after a % in a printf format string
   to get correct 32/64 bit behavior, like this:
   size_t size = records.size();
   printf("%"PRIuS"\n", size); */

#define PRIdS __PRIS_PREFIX "d"
#define PRIxS __PRIS_PREFIX "x"
#define PRIuS __PRIS_PREFIX "u"
#define PRIXS __PRIS_PREFIX "X"
#define PRIoS __PRIS_PREFIX "o"

struct stats {
    uint32_t      curr_conns;
    uint32_t      total_conns;
    uint32_t      conn_structs;
    uint64_t      get_cmds;
    uint64_t      set_cmds;
    uint64_t      get_hits;
    uint64_t      get_misses;
    time_t        started;          /* when the process was started */
    uint64_t      bytes_read;
    uint64_t      bytes_written;
};

#define MAX_VERBOSITY_LEVEL 2

struct settings {
    size_t item_buf_size;
    int maxconns;
    int port;
    int udpport;
    char *inter;
    int verbose;
    char *socketpath;   /* path to unix socket if using local socket */
    int access;  /* access mask (a la chmod) for unix domain socket */
    int num_threads;        /* number of libevent threads to run */
};

extern struct stats stats;
extern struct settings settings;

struct mdb_settings {
    char *env_home;    /* db env home dir path */
    int txn_nosync;    /* DB_TXN_NOSYNC flag, if 1 will lose transaction's durability for performance */
    int chkpoint_val;  /* do checkpoint every *db_chkpoint_val* second, 0 for disable */
    u_int32_t db_flags; /* database open flags */
    u_int32_t env_flags; /* env open flags */
};

extern struct mdb_settings mdb_settings;

typedef struct ditem {
	uint8_t			nsuffix;	/* length of flags-and-length string */
	char			buf[2];
	/* " flags length\r\n"
	 * data with terminating \r\n
	 */
} ditem;

typedef struct item {
	MDB_val key;
	MDB_val data;
} item;


/* warning: don't use these macros with a function, as it evals its arg twice */
#define ITEM_key(item) ((char*)(item)->key.mv_data)
#define ITEM_suflen(item) (((ditem *)((item)->data.mv_data))->nsuffix)
#define ITEM_suffix(item) (((ditem *)((item)->data.mv_data))->buf)
#define ITEM_data(item) ((char *)(ITEM_suffix(item) + ITEM_suflen(item)))
#define ITEM_dlen(item) ((item)->data.mv_size - ITEM_suflen(item) - 1)
#define ITEM_end(item) ((char *)item->data.mv_data + item->data.mv_size)
#if 0
#define ITEM_key(item) ((char*)&((item)->end[0]))
#define ITEM_suffix(item) ((char*) &((item)->end[0]) + (item)->nkey + 1)
#define ITEM_data(item) ((char*) &((item)->end[0]) + (item)->nkey + 1 + (item)->nsuffix)
#define ITEM_ntotal(item) (sizeof(struct _stritem) + (item)->nkey + 1 + (item)->nsuffix + (item)->nbytes)
#endif

enum conn_states {
    conn_listening,  /** the socket which listens for connections */
    conn_read,       /** reading in a command line */
    conn_write,      /** writing out a simple response */
    conn_nread,      /** reading in a fixed number of bytes */
    conn_swallow,    /** swallowing unnecessary bytes w/o storing */
    conn_closing,    /** closing this connection */
    conn_mwrite,     /** writing out many items sequentially */
};

#define NREAD_ADD 1
#define NREAD_SET 2
#define NREAD_REPLACE 3
#define NREAD_APPEND 4
#define NREAD_PREPEND 5

typedef struct conn conn;
struct conn {
    int    sfd;
    int    state;
    struct event event;
    short  ev_flags;
    short  which;   /** which events were just triggered */

    char   *rbuf;   /** buffer to read commands into */
    char   *rcurr;  /** but if we parsed some already, this is where we stopped */
    int    rsize;   /** total allocated size of rbuf */
    int    rbytes;  /** how much data, starting from rcur, do we have unparsed */

    char   *wbuf;
    char   *wcurr;
    int    wsize;
    int    wbytes;
    int    write_and_go; /** which state to go into after finishing current write */
    void   *write_and_free; /** free this memory after finishing writing */

    char   *ritem;  /** when we read in an item's value, it goes here */
    int    rlbytes;

    /* data for the nread state */

    /**
     * item is used to hold an item structure created after reading the command
     * line of set/add/replace commands, but before we finished reading the actual
     * data. The data is read into ITEM_data(item) to avoid extra copying.
     */

    void   *item;     /* for commands set/add/replace  */
    int    item_comm; /* which one is it: set/add/replace */

    /* data for the swallow state */
    int    sbytes;    /* how many bytes to swallow */

    /* data for the mwrite state */
    struct iovec *iov;
    int    iovsize;   /* number of elements allocated in iov[] */
    int    iovused;   /* number of elements used in iov[] */

    struct msghdr *msglist;
    int    msgsize;   /* number of elements allocated in msglist[] */
    int    msgused;   /* number of elements used in msglist[] */
    int    msgcurr;   /* element in msglist[] being transmitted now */
    int    msgbytes;  /* number of bytes in current msg */

    /* data for UDP clients */
    bool   udp;       /* is this is a UDP "connection" */
    int    request_id; /* Incoming UDP request ID, if this is a UDP "connection" */
    struct sockaddr request_addr; /* Who sent the most recent request */
    socklen_t request_addr_size;
    unsigned char *hdrbuf; /* udp packet headers */
    int    hdrsize;   /* number of headers' worth of space is allocated */
    conn   *next;     /* Used for generating a list of conn structures */
};

/*
 * Functions
 */

/* mdb management */
void mdb_settings_init(void);
void mdb_setup(void);
void start_chkpoint_thread(void);
void mdb_shutdown(void);
void mdb_chkpoint(void);

/* item management */
void item_init(void);
item *do_item_from_freelist(void);
int do_item_add_to_freelist(item *it);
item *item_alloc1(MDB_val *key, const int flags, const int nbytes);
int item_alloc_put(MDB_txn *txn, MDB_val *key, const int flags, const int nbytes, item *it);
item *item_alloc2(size_t ntotal);
int item_free(item *it);
int item_get(MDB_txn *txn, MDB_val *key, item *it);
int item_put(MDB_txn *txn, item *it);
int item_delete(MDB_txn *txn, MDB_val *key);
int item_exists(MDB_txn *txn, MDB_val *key);
int item_cget(MDB_cursor *cursorp, MDB_val *key, item *it, u_int32_t flags);

/* mdb related stats */
void stats_mdb(char *temp);

/* conn management */
conn *do_conn_from_freelist();
bool do_conn_add_to_freelist(conn *c);
conn *conn_new(const int sfd, const int init_state, const int event_flags, const int read_buffer_size, const bool is_udp, struct event_base *base);

char *add_delta(const bool incr, const int64_t delta, char *buf, MDB_val *key);
int store_item(item *item, int comm);

/*
 * In multithreaded mode, we wrap certain functions with lock management and
 * replace the logic of some other functions. All wrapped functions have
 * "mt_" and "do_" variants. In multithreaded mode, the plain version of a
 * function is #define-d to the "mt_" variant, which often just grabs a
 * lock and calls the "do_" function. In singlethreaded mode, the "do_"
 * function is called directly.
 *
 * Functions such as the libevent-related calls that need to do cross-thread
 * communication in multithreaded mode (rather than actually doing the work
 * in the current thread) are called via "dispatch_" frontends, which are
 * also #define-d to directly call the underlying code in singlethreaded mode.
 */
#ifdef USE_THREADS

void thread_init(int nthreads, struct event_base *main_base);
int  dispatch_event_add(int thread, conn *c);
void dispatch_conn_new(int sfd, int init_state, int event_flags, int read_buffer_size, int is_udp);

/* Lock wrappers for cache functions that are called from main loop. */
conn *mt_conn_from_freelist(void);
bool  mt_conn_add_to_freelist(conn *c);
int   mt_is_listen_thread(void);
item *mt_item_from_freelist(void);
int mt_item_add_to_freelist(item *it);
void  mt_stats_lock(void);
void  mt_stats_unlock(void);

# define conn_from_freelist()        mt_conn_from_freelist()
# define conn_add_to_freelist(x)     mt_conn_add_to_freelist(x)
# define is_listen_thread()          mt_is_listen_thread()
# define item_from_freelist()        mt_item_from_freelist()
# define item_add_to_freelist(x)     mt_item_add_to_freelist(x)

# define STATS_LOCK()                mt_stats_lock()
# define STATS_UNLOCK()              mt_stats_unlock()

#else /* !USE_THREADS */

# define conn_from_freelist()         do_conn_from_freelist()
# define conn_add_to_freelist(x)      do_conn_add_to_freelist(x)
# define dispatch_conn_new(x,y,z,a,b) conn_new(x,y,z,a,b,main_base)
# define dispatch_event_add(t,c)      event_add(&(c)->event, 0)
# define is_listen_thread()           1
# define item_from_freelist()         do_item_from_freelist()
# define item_add_to_freelist(x)      do_item_add_to_freelist(x)
# define thread_init(x,y)             0

# define STATS_LOCK()                /**/
# define STATS_UNLOCK()              /**/

#endif /* !USE_THREADS */

extern MDB_env *env;
extern MDB_dbi dbi;
extern int daemon_quit;
