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
#include <stdlib.h>
#include <lmdb.h>

void stats_mdb(char *temp){
    char *pos = temp;
    int ret;
    u_int32_t gbytes = 0;
    u_int32_t bytes = 0;
    int ncache = 0;
	MDB_stat mstat;
    
    char *env_home;
    
    /* get mdb version */
    pos += sprintf(pos, "STAT db_ver %s\r\n", mdb_version(NULL, NULL, NULL));

    /* get env stats */
	mdb_env_stat(env, &mstat);
    pos += sprintf(pos, "STAT page_size %u\r\n", mstat.ms_psize);
    pos += sprintf(pos, "STAT branch_pages %"PRIuS"\r\n", mstat.ms_branch_pages);
    pos += sprintf(pos, "STAT leaf_pages %"PRIuS"\r\n", mstat.ms_leaf_pages);
    pos += sprintf(pos, "STAT overflow_pages %"PRIuS"\r\n", mstat.ms_overflow_pages);
    pos += sprintf(pos, "STAT items %"PRIuS"\r\n", mstat.ms_entries);

    /* get env dir */
    if((ret = mdb_env_get_path(env, (const char **)&env_home)) == 0){
        pos += sprintf(pos, "STAT env_home %s\r\n", env_home);
    }

    /* get cache size */
    pos += sprintf(pos, "STAT cache_size %" PRIu64"\r\n", mdb_settings.cache_size);
    
    pos += sprintf(pos, "STAT txn_nosync %d\r\n", mdb_settings.txn_nosync);
    pos += sprintf(pos, "STAT chkpoint_val %d\r\n", mdb_settings.chkpoint_val);
    pos += sprintf(pos, "END");
}
