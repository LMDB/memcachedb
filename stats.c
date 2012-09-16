/*
 *  MemcacheDB - A distributed key-value storage system designed for persistent:
 *
 *      http://memcachedb.googlecode.com
 *
 *  The source code of Memcachedb is most based on Memcached:
 *
 *      http://danga.com/memcached/
 *
 *  Copyright 2008 Steve Chu.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Steve Chu <stvchu@gmail.com>
 *
 */
  
#include "memcachedb.h"
#include <stdlib.h>
#include <mdb.h>

void stats_mdb(char *temp){
    char *pos = temp;
    int ret;
    u_int32_t gbytes = 0;
    u_int32_t bytes = 0;
    int ncache = 0;
    
    char *env_home;
    
    /* get bdb version */
    pos += sprintf(pos, "STAT db_ver %s\r\n", mdb_version(NULL, NULL, NULL));

    /* get page size */
    if((ret = dbp->get_pagesize(dbp, &bdb_settings.page_size)) == 0){
        pos += sprintf(pos, "STAT page_size %u\r\n", bdb_settings.page_size);
    }

    /* get env dir */
    if((ret = env->get_home(env, (const char **)&env_home)) == 0){
        pos += sprintf(pos, "STAT env_home %s\r\n", env_home);
    }

    /* get cache size */
    if((ret = env->get_cachesize(env, &gbytes, &bytes, &ncache)) == 0){
        pos += sprintf(pos, "STAT cache_size %u/%u/%d\r\n", gbytes, bytes, ncache);
    }
    
    pos += sprintf(pos, "STAT txn_nosync %d\r\n", bdb_settings.txn_nosync);
    pos += sprintf(pos, "STAT chkpoint_val %d\r\n", bdb_settings.chkpoint_val);
    pos += sprintf(pos, "END");
}
