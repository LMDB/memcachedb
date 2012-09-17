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
 
#include "memcachedb.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>

#define MAX_ITEM_FREELIST_LENGTH 4000
#define INIT_ITEM_FREELIST_LENGTH 500

static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes, char *suffix, uint8_t *nsuffix);

static item **freeitem;
static int freeitemtotal;
static int freeitemcurr;

void item_init(void) {
    freeitemtotal = INIT_ITEM_FREELIST_LENGTH;
    freeitemcurr  = 0;

    freeitem = (item **)malloc( sizeof(item *) * freeitemtotal );
    if (freeitem == NULL) {
        perror("malloc()");
    }
    return;
}

/*
 * Returns a item buffer from the freelist, if any. Sholud call
 * item_from_freelist for thread safty.
 * */
item *do_item_from_freelist(void) {
    item *s;

    if (freeitemcurr > 0) {
        s = freeitem[--freeitemcurr];
    } else {
        /* If malloc fails, let the logic fall through without spamming
         * STDERR on the server. */
        s = (item *)malloc( settings.item_buf_size );
        if (s != NULL){
            memset(s, 0, settings.item_buf_size);
        }
    }

    return s;
}

/*
 * Adds a item to the freelist. Should call 
 * item_add_to_freelist for thread safty.
 */
int do_item_add_to_freelist(item *it) {
    if (freeitemcurr < freeitemtotal) {
        freeitem[freeitemcurr++] = it;
        return 0;
    } else {
        if (freeitemtotal >= MAX_ITEM_FREELIST_LENGTH){
            return 1;
        }
        /* try to enlarge free item buffer array */
        item **new_freeitem = (item **)realloc(freeitem, sizeof(item *) * freeitemtotal * 2);
        if (new_freeitem) {
            freeitemtotal *= 2;
            freeitem = new_freeitem;
            freeitem[freeitemcurr++] = it;
            return 0;
        }
    }
    return 1;
}

#define SUFFLEN	40

/**
 * Generates the variable-sized part of the header for an object.
 *
 * key     - The key
 * nkey    - The length of the key
 * flags   - key flags
 * nbytes  - Number of bytes to hold value and addition CRLF terminator
 * suffix  - Buffer for the "VALUE" line suffix (flags, size).
 * nsuffix - The length of the suffix is stored here.
 *
 * Returns the total size of the header.
 */
static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes,
                     char *suffix, uint8_t *nsuffix) {
    *nsuffix = (uint8_t) snprintf(suffix, SUFFLEN, " %d %d\r\n", flags, nbytes - 2);
    return sizeof(item) + nkey + *nsuffix + nbytes;
}

/*
 * alloc a item buffer, and init it.
 */
item *item_alloc1(MDB_val *key, const int flags, const int nbytes) {
    uint8_t nsuffix;
    item *it;
    char suffix[SUFFLEN];
    size_t ntotal = item_make_header(key->mv_size + 1, flags, nbytes, suffix, &nsuffix);

    if (ntotal > settings.item_buf_size){
        it = (item *)malloc(ntotal);
        if (it == NULL){
            return NULL;
        }
        memset(it, 0, ntotal);
        if (settings.verbose > 1) {
            fprintf(stderr, "alloc a item buffer from malloc.\n");
        }
    }else{
        it = item_from_freelist();
        if (it == NULL){
            return NULL;
        }
        if (settings.verbose > 1) {
            fprintf(stderr, "alloc a item buffer from freelist.\n");
        }
    }

    it->key.mv_size = key->mv_size;
	it->key.mv_data = (void *)(it+1);
	it->data.mv_size = nbytes + nsuffix + 1;
	it->data.mv_data = (char *)it->key.mv_data + key->mv_size+1;
    strcpy(ITEM_key(it), key->mv_data);
    memcpy(ITEM_suffix(it), suffix, (size_t)nsuffix);
    ITEM_suflen(it) = nsuffix;
    return it;
}

/*
 * alloc a item buffer only.
 */
item *item_alloc2(size_t ntotal) {
    item *it;
    if (ntotal > settings.item_buf_size){
        it = (item *)malloc(ntotal);
        if (it == NULL){
            return NULL;
        }
        memset(it, 0, ntotal);
        if (settings.verbose > 1) {
            fprintf(stderr, "alloc a item buffer from malloc.\n");
        }
    }else{
        it = item_from_freelist();
        if (it == NULL){
            return NULL;
        }
        if (settings.verbose > 1) {
            fprintf(stderr, "alloc a item buffer from freelist.\n");
        }
    }

    return it;
}

/*
 * alloc a item buffer, and init it.
 */
int item_alloc_put(MDB_txn *txn, MDB_val *key, const int flags, const int nbytes, item *it) {
    uint8_t nsuffix;
    char suffix[SUFFLEN];
	int ret;

	it->key = *key;
    it->data.mv_size = item_make_header(0, flags, nbytes, suffix, &nsuffix) - sizeof(item);
	ret = mdb_put(txn, dbi, key, &it->data, MDB_RESERVE);
	if (!ret) {
		memcpy(ITEM_suffix(it), suffix, (size_t)nsuffix);
		ITEM_suflen(it) = nsuffix;
	}
    return ret;
}

/*
 * alloc a item buffer only.
 */
/*
 * free a item buffer. here 'it' must be a full item.
 */

int item_free(item *it) {
    size_t ntotal = 0;
    if (NULL == it)
        return 0;

    /* ntotal may be wrong, if 'it' is not a full item. */
    ntotal = ITEM_ntotal(it);
    if (ntotal > settings.item_buf_size){
        if (settings.verbose > 1) {
            fprintf(stderr, "ntotal: %"PRIuS", use free() directly.\n", ntotal);
        }
        free(it);   
    }else{
        if (0 != item_add_to_freelist(it)) {
            if (settings.verbose > 1) {
                fprintf(stderr, "ntotal: %"PRIuS", add a item buffer to freelist fail, use free() directly.\n", ntotal);
            }
            free(it);   
        }else{
            if (settings.verbose > 1) {
                fprintf(stderr, "ntotal: %"PRIuS", add a item buffer to freelist.\n", ntotal);
            }
        }
    }
    return 0;
}

int item_get(MDB_txn *txn, MDB_val *key, item *it) {
    int ret;
    
    /* try to get a item from mdb */
	ret = mdb_get(txn, dbi, key, &it->data);
	if (!ret) {
		it->key = *key;
		return 0;
	}
	if (ret == MDB_NOTFOUND)
		return 1;
	if (settings.verbose > 1) {
		fprintf(stderr, "mdb_get: %s\n", mdb_strerror(ret));
	}
    return -1;
}

int item_cget(MDB_cursor *cursorp, MDB_val *key, item *it, u_int32_t flags){
	int ret;
    /* try to get a item from mdb */
	ret = mdb_cursor_get(cursorp, key, &it->data, flags);
	if (!ret) {
		it->key = *key;
		return 0;
	}
	if (ret == MDB_NOTFOUND)
		return 1;
	if (settings.verbose > 1) {
		fprintf(stderr, "mdb_cursor_get: %s\n", mdb_strerror(ret));
	}
	return -1;
}

/* 0 for Success
   -1 for SERVER_ERROR
*/
int item_put(MDB_txn *txn, item *it){
	int ret;
	ret = mdb_put(txn, dbi, &it->key, &it->data, 0);
	if (!ret)
		return 0;
	if (settings.verbose > 1) {
		fprintf(stderr, "mdb_put: %s\n", mdb_strerror(ret));
	}
	return -1;
}

/* 0 for Success
   1 for NOT_FOUND
   -1 for SERVER_ERROR
*/
int item_delete(MDB_txn *txn, MDB_val *key) {
	int ret;
    ret = mdb_del(txn, dbi, key, NULL);
    if (ret == 0)
        return 0;
    if(ret == MDB_NOTFOUND)
        return 1;
    if (settings.verbose > 1) {
        fprintf(stderr, "mdb_del: %s\n", mdb_strerror(ret));
    }
    return -1;
}

/*
1 for exists
0 for non-exist
*/
int item_exists(MDB_txn *txn, MDB_val *key) {
    int ret;
	MDB_val data;
    
    ret = mdb_get(txn, dbi, key, &data);
    if (ret == 0){
        return 1;
    }
    return 0;
}
