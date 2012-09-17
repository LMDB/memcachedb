/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  Copyright 2012 Howard Chu.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Howard Chu <hyc@symas.com>
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
#include <signal.h>
#include <mdb.h>

static void *mdb_chkpoint_thread __P((void *));


static pthread_t chk_ptid;

void mdb_settings_init(void)
{
    mdb_settings.env_home = DBHOME;
    mdb_settings.cache_size = 1 * 1024 * 1024 * 1024; /* default is 1GB */
    mdb_settings.txn_nosync = 0; /* default DB_TXN_NOSYNC is off */
    mdb_settings.chkpoint_val = 60 * 5;
    mdb_settings.db_flags = DB_CREATE | DB_AUTO_COMMIT;
    mdb_settings.env_flags = DB_CREATE
                          | DB_INIT_LOCK 
                          | DB_THREAD 
                          | DB_INIT_MPOOL 
                          | DB_INIT_LOG 
                          | DB_INIT_TXN
                          | DB_RECOVER;
}

void mdb_env_init(void){
    int ret;
    /* db env init */
    if ((ret = db_env_create(&env, 0)) != 0) {
        fprintf(stderr, "db_env_create: %s\n", db_strerror(ret));
        exit(EXIT_FAILURE);
    }

    /* set err&msg display */
    env->set_errpfx(env, PACKAGE);
    /* env->set_errfile(env, stderr); */
    /* env->set_msgfile(env, stderr); */
	env->set_errcall(env, mdb_err_callback);
  	env->set_msgcall(env, mdb_msg_callback);

    /* set BerkeleyDB verbose*/
    if (settings.verbose > 1) {
        if ((ret = env->set_verbose(env, DB_VERB_FILEOPS_ALL, 1)) != 0) {
            fprintf(stderr, "env->set_verbose[DB_VERB_FILEOPS_ALL]: %s\n",
                    db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        if ((ret = env->set_verbose(env, DB_VERB_DEADLOCK, 1)) != 0) {
            fprintf(stderr, "env->set_verbose[DB_VERB_DEADLOCK]: %s\n",
                    db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        if ((ret = env->set_verbose(env, DB_VERB_RECOVERY, 1)) != 0) {
            fprintf(stderr, "env->set_verbose[DB_VERB_RECOVERY]: %s\n",
                    db_strerror(ret));
            exit(EXIT_FAILURE);
        }
    }
    
    /* set log dir*/
    if (mdb_settings.log_home != NULL){
        /* if no log dir existed, we create it */
        if (0 != access(mdb_settings.log_home, F_OK)) {
            if (0 != mkdir(mdb_settings.log_home, 0750)) {
                fprintf(stderr, "mkdir log_home error:[%s]\n", mdb_settings.log_home);
                exit(EXIT_FAILURE);
            }
        }
        env->set_lg_dir(env, mdb_settings.log_home);
    }
    
    /* set MPOOL size */
    if (sizeof(void *) == 4 && mdb_settings.cache_size > (1024uLL * 1024uLL * 1024uLL * 2uLL)) {
        fprintf(stderr, "32bit box only max 2GB memory pool allowed\n");
        exit(EXIT_FAILURE);
    }
    env->set_cachesize(env, (u_int32_t) (mdb_settings.cache_size / (1024uLL * 1024uLL * 1024uLL)), 
                            (u_int32_t) (mdb_settings.cache_size % (1024uLL * 1024uLL * 1024uLL)), 
                            (int) (mdb_settings.cache_size / (1024uLL * 1024uLL * 1024uLL * 4uLL) + 1uLL) );
    
    /* set DB_TXN_NOSYNC flag */
    if (mdb_settings.txn_nosync){
        env->set_flags(env, DB_TXN_NOSYNC, 1);
    }

    /* set locking */
    env->set_lk_max_lockers(env, 20000);
    env->set_lk_max_locks(env, 20000);
    env->set_lk_max_objects(env, 20000);

    /* at least max active transactions */
  	env->set_tx_max(env, 10000);

    /* set transaction log buffer */
    env->set_lg_bsize(env, mdb_settings.txn_lg_bsize);
    
    /* NOTICE:
    If set, Berkeley DB will automatically remove log files that are no longer needed.
    Automatic log file removal is likely to make catastrophic recovery impossible.
    Replication applications will rarely want to configure automatic log file removal as it
    increases the likelihood a master will be unable to satisfy a client's request 
    for a recent log record.
     */
    if (!mdb_settings.is_replicated && mdb_settings.log_auto_remove){
        fprintf(stderr, "log_auto_remove\n");        
        env->log_set_config(env, DB_LOG_AUTO_REMOVE, 1);
    }
    
    /* if no home dir existed, we create it */
    if (0 != access(mdb_settings.env_home, F_OK)) {
        if (0 != mkdir(mdb_settings.env_home, 0750)) {
            fprintf(stderr, "mkdir env_home error:[%s]\n", mdb_settings.env_home);
            exit(EXIT_FAILURE);
        }
    }
    
    if(mdb_settings.is_replicated) {
        mdb_settings.env_flags |= DB_INIT_REP;

        /* verbose messages that help us to debug */
        if (settings.verbose > 1) {
            if ((ret = env->set_verbose(env, DB_VERB_REPLICATION, 1)) != 0) {
                fprintf(stderr, "env->set_verbose[DB_VERB_REPLICATION]: %s\n",
                        db_strerror(ret));
                exit(EXIT_FAILURE);
            }
        }

        /* set up the event hook, so we can do logging when something happens */
        env->set_event_notify(env, mdb_event_callback);

        /* ack policy can have a great impact in performance, lantency and consistency */
        env->repmgr_set_ack_policy(env, mdb_settings.rep_ack_policy);

        /* timeout configs */
        env->rep_set_timeout(env, DB_REP_ACK_TIMEOUT, mdb_settings.rep_ack_timeout);
        env->rep_set_timeout(env, DB_REP_CHECKPOINT_DELAY, mdb_settings.rep_chkpoint_delay);
        env->rep_set_timeout(env, DB_REP_CONNECTION_RETRY, mdb_settings.rep_conn_retry);
        env->rep_set_timeout(env, DB_REP_ELECTION_TIMEOUT, mdb_settings.rep_elect_timeout);
        env->rep_set_timeout(env, DB_REP_ELECTION_RETRY, mdb_settings.rep_elect_retry);

        /* notice:
           The "monitor" time should always be at least a little bit longer than the "send" time */
        env->rep_set_timeout(env, DB_REP_HEARTBEAT_MONITOR, mdb_settings.rep_heartbeat_monitor);
        env->rep_set_timeout(env, DB_REP_HEARTBEAT_SEND, mdb_settings.rep_heartbeat_send);

        /* Bulk transfers simply cause replication messages to accumulate
           in a buffer until a triggering event occurs.  
	    	   Bulk transfer occurs when:
           1. Bulk transfers are configured for the master environment, and
           2. the message buffer is full or
           3. a permanent record (for example, a transaction commit or a checkpoint record)
		   is placed in the buffer for the replica.
        */
        env->rep_set_config(env, DB_REP_CONF_BULK, mdb_settings.rep_bulk);

        /* we now never use master lease */
        /*
        env->rep_set_timeout(env, DB_REP_LEASE_TIMEOUT, mdb_settings.rep_lease_timeout);
        env->rep_set_config(env, DB_REP_CONF_LEASE, mdb_settings.rep_lease);
        env->rep_set_clockskew(env, mdb_settings.rep_fast_clock, mdb_settings.rep_slow_clock);
        */

        env->rep_set_priority(env, mdb_settings.rep_priority);
        env->rep_set_request(env, mdb_settings.rep_req_min, mdb_settings.rep_req_max);
		env->rep_set_limit(env, mdb_settings.rep_limit_gbytes, mdb_settings.rep_limit_bytes);

        /* publish the local site communication channel */
        if ((ret = env->repmgr_set_local_site(env, mdb_settings.rep_localhost, mdb_settings.rep_localport, 0)) != 0) {
            fprintf(stderr, "repmgr_set_local_site[%s:%d]: %s\n", 
                    mdb_settings.rep_localhost, mdb_settings.rep_localport, db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        /* add a remote site, mostly this is a master */
        if(NULL != mdb_settings.rep_remotehost) {
            if ((ret = env->repmgr_add_remote_site(env, mdb_settings.rep_remotehost, mdb_settings.rep_remoteport, NULL, 0)) != 0) {
                fprintf(stderr, "repmgr_add_remote_site[%s:%d]: %s\n", 
                        mdb_settings.rep_remotehost, mdb_settings.rep_remoteport, db_strerror(ret));
                exit(EXIT_FAILURE);
            }
        }
        /* nsite is important for electing, default nvotes is (nsite/2 + 1)
           if nsite is equel to 2, then nvotes is 1 */
        if ((ret = env->rep_set_nsites(env, mdb_settings.rep_nsites)) != 0) {
            fprintf(stderr, "rep_set_nsites: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }
    }

    if ((ret = env->open(env, mdb_settings.env_home, mdb_settings.env_flags, 0)) != 0) {
        fprintf(stderr, "db_env_open: %s\n", db_strerror(ret));
        exit(EXIT_FAILURE);
    }

    if(mdb_settings.is_replicated) {
        /* repmgr_start must run after daemon !!!*/
        if ((ret = env->repmgr_start(env, 3, mdb_settings.rep_start_policy)) != 0) {
            fprintf(stderr, "env->repmgr_start: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        /* sleep 5 second for electing or client startup */
        if (mdb_settings.rep_start_policy == DB_REP_ELECTION ||
            mdb_settings.rep_start_policy == DB_REP_CLIENT) {
            sleep(5);
        }
    }
}


void mdb_db_open(void){
    int ret;
    int db_open = 0;
    /* for replicas to get a full master copy, then open db */
    while(!db_open) {
        /* if replica, just scratch the db file from a master */
        if (1 == mdb_settings.is_replicated){
            if (MDB_CLIENT == mdb_settings.rep_whoami) {
                mdb_settings.db_flags = DB_AUTO_COMMIT;
            }else if (MDB_MASTER == mdb_settings.rep_whoami) {
                mdb_settings.db_flags = DB_CREATE | DB_AUTO_COMMIT;
            }else{
                /* do nothing */
            }
        }

        mdb_db_close();

        if ((ret = db_create(&dbp, env, 0)) != 0) {
            fprintf(stderr, "db_create: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        /* set page size */
        if((ret = dbp->set_pagesize(dbp, mdb_settings.page_size)) != 0){
            fprintf(stderr, "dbp->set_pagesize: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }

        /* try to open db*/
        ret = dbp->open(dbp, NULL, mdb_settings.db_file, NULL, mdb_settings.db_type, mdb_settings.db_flags, 0664);         
        switch (ret){
        case 0:
            db_open = 1;
            break;
        case ENOENT:
        case DB_LOCK_DEADLOCK:
        case DB_REP_LOCKOUT:
            fprintf(stderr, "db_open: %s\n", db_strerror(ret));
            sleep(3);
            break;
        default:
            fprintf(stderr, "db_open: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }
    }

}

void start_chkpoint_thread(void){
    if (mdb_settings.chkpoint_val > 0){
        /* Start a checkpoint thread. */
        if ((errno = pthread_create(
            &chk_ptid, NULL, mdb_chkpoint_thread, (void *)env)) != 0) {
            fprintf(stderr,
                "failed spawning checkpoint thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}

void start_memp_trickle_thread(void){
    if (mdb_settings.memp_trickle_val > 0){
        /* Start a memp_trickle thread. */
        if ((errno = pthread_create(
            &mtri_ptid, NULL, mdb_memp_trickle_thread, (void *)env)) != 0) {
            fprintf(stderr,
                "failed spawning memp_trickle thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}

void start_dl_detect_thread(void){
    if (mdb_settings.dldetect_val > 0){
        /* Start a deadlock detecting thread. */
        if ((errno = pthread_create(
            &dld_ptid, NULL, mdb_dl_detect_thread, (void *)env)) != 0) {
            fprintf(stderr,
                "failed spawning deadlock thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}

static void *mdb_chkpoint_thread(void *arg)
{
    DB_ENV *dbenv;
    int ret;
    dbenv = arg;
    if (settings.verbose > 1) {
        dbenv->errx(dbenv, "checkpoint thread created: %lu, every %d seconds", 
                           (u_long)pthread_self(), mdb_settings.chkpoint_val);
    }
    for (;; sleep(mdb_settings.chkpoint_val)) {
        if ((ret = dbenv->txn_checkpoint(dbenv, 0, 0, 0)) != 0) {
            dbenv->err(dbenv, ret, "checkpoint thread");
        }
        dbenv->errx(dbenv, "checkpoint thread: a txn_checkpoint is done");
    }
    return (NULL);
}

static void *mdb_memp_trickle_thread(void *arg)
{
    DB_ENV *dbenv;
    int ret, nwrotep;
    dbenv = arg;
    if (settings.verbose > 1) {
        dbenv->errx(dbenv, "memp_trickle thread created: %lu, every %d seconds, %d%% pages should be clean.", 
                           (u_long)pthread_self(), mdb_settings.memp_trickle_val,
                           mdb_settings.memp_trickle_percent);
    }
    for (;; sleep(mdb_settings.memp_trickle_val)) {
        if ((ret = dbenv->memp_trickle(dbenv, mdb_settings.memp_trickle_percent, &nwrotep)) != 0) {
            dbenv->err(dbenv, ret, "memp_trickle thread");
        }
        dbenv->errx(dbenv, "memp_trickle thread: writing %d dirty pages", nwrotep);
    }
    return (NULL);
}

static void *mdb_dl_detect_thread(void *arg)
{
    DB_ENV *dbenv;
    struct timeval t;
    dbenv = arg;
    if (settings.verbose > 1) {
        dbenv->errx(dbenv, "deadlock detecting thread created: %lu, every %d millisecond",
                           (u_long)pthread_self(), mdb_settings.dldetect_val);
    }
    while (!daemon_quit) {
        t.tv_sec = 0;
        t.tv_usec = mdb_settings.dldetect_val;
        (void)dbenv->lock_detect(dbenv, 0, DB_LOCK_YOUNGEST, NULL);
        /* select is a more accurate sleep timer */
        (void)select(0, NULL, NULL, NULL, &t);
    }
    return (NULL);
}

static void mdb_event_callback(DB_ENV *env, u_int32_t which, void *info)
{
    switch (which) {
    case DB_EVENT_PANIC:
        env->errx(env, "evnet: DB_EVENT_PANIC, we got panic, recovery should be run.");
        break;
    case DB_EVENT_REP_CLIENT:
        env->errx(env, "event: DB_EVENT_REP_CLIENT, I<%s:%d> am now a replication client.", 
                       mdb_settings.rep_localhost, mdb_settings.rep_localport);
        mdb_settings.rep_whoami = MDB_CLIENT;
        break;
    case DB_EVENT_REP_ELECTED:
        env->errx(env, "event: DB_EVENT_REP_ELECTED, I<%s:%d> has just won an election.", 
                       mdb_settings.rep_localhost, mdb_settings.rep_localport);
        break;
    case DB_EVENT_REP_MASTER:
        env->errx(env, "event: DB_EVENT_REP_MASTER, I<%s:%d> am now a replication master.", 
                       mdb_settings.rep_localhost, mdb_settings.rep_localport);
        mdb_settings.rep_whoami = MDB_MASTER;
        mdb_settings.rep_master_eid = BDB_EID_SELF;
        break;
    case DB_EVENT_REP_NEWMASTER:
        mdb_settings.rep_master_eid = *(int*)info;
        env->errx(env, "event: DB_EVENT_REP_NEWMASTER, a new master<eid: %d> has been established, "
                       "but not me<%s:%d>", mdb_settings.rep_master_eid,
                       mdb_settings.rep_localhost, mdb_settings.rep_localport);
        break;
    case DB_EVENT_REP_PERM_FAILED:
        env->errx(env, "event: DB_EVENT_REP_PERM_FAILED, insufficient acks, "
                       "the master will flush the txn log buffer");
        break;
    case DB_EVENT_REP_STARTUPDONE: 
        if (mdb_settings.rep_whoami == MDB_CLIENT){
            env->errx(env, "event: DB_EVENT_REP_STARTUPDONE, I has completed startup synchronization and"
                           " is now processing live log records received from the master.");
        }
        break;
    case DB_EVENT_WRITE_FAILED:
        env->errx(env, "event: DB_EVENT_WRITE_FAILED, I wrote to stable storage failed.");
        break;
    default:
        env->errx(env, "ignoring event %d", which);
    }
}

static void mdb_err_callback(const DB_ENV *dbenv, const char *errpfx, const char *msg){
	time_t curr_time = time(NULL);
	char time_str[32];
	strftime(time_str, 32, "%c", localtime(&curr_time));
	fprintf(stderr, "[%s] [%s] \"%s\"\n", errpfx, time_str, msg);
}

static void mdb_msg_callback(const DB_ENV *dbenv, const char *msg){
	time_t curr_time = time(NULL);
	char time_str[32];
	strftime(time_str, 32, "%c", localtime(&curr_time));
	fprintf(stderr, "[%s] [%s] \"%s\"\n", PACKAGE, time_str, msg);
}

/* for atexit cleanup */
void mdb_db_close(void){
    int ret = 0;

    if (dbp != NULL) {
        ret = dbp->close(dbp, 0);
        if (0 != ret){
            fprintf(stderr, "dbp->close: %s\n", db_strerror(ret));
        }else{
            dbp = NULL;
            fprintf(stderr, "dbp->close: OK\n");
        }
    }
}

/* for atexit cleanup */
void mdb_env_close(void){
    int ret = 0;
    if (env != NULL) {
        ret = env->close(env, 0);
        if (0 != ret){
            fprintf(stderr, "env->close: %s\n", db_strerror(ret));
        }else{
            env = NULL;
            fprintf(stderr, "env->close: OK\n");
        }
    }
}

/* for atexit cleanup */
void mdb_chkpoint(void)
{
    int ret = 0;
    if (env != NULL){
        ret = env->txn_checkpoint(env, 0, 0, 0); 
        if (0 != ret){
            fprintf(stderr, "env->txn_checkpoint: %s\n", db_strerror(ret));
        }else{
            fprintf(stderr, "env->txn_checkpoint: OK\n");
        }
    }
}

