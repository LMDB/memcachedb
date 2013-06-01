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
#include <lmdb.h>

static void *mdb_chkpoint_thread __P((void *));


static pthread_t chk_ptid;

void mdb_settings_init(void)
{
    mdb_settings.env_home = DBHOME;
    mdb_settings.cache_size = 1 * 1024 * 1024 * 1024; /* default is 1GB */
    mdb_settings.txn_nosync = 0; /* default DB_TXN_NOSYNC is off */
    mdb_settings.chkpoint_val = 60 * 5;
    mdb_settings.env_flags = MDB_WRITEMAP;
}

void mdb_setup(void){
    int ret;
	unsigned int readers;
	MDB_txn *txn;

    /* db env init */
    if ((ret = mdb_env_create(&env)) != 0) {
        fprintf(stderr, "mdb_env_create: %s\n", mdb_strerror(ret));
        exit(EXIT_FAILURE);
    }

    /* set map size */
    if (sizeof(void *) == 4 && mdb_settings.cache_size > (1024uLL * 1024uLL * 1024uLL * 2uLL)) {
        fprintf(stderr, "32bit box only max 2GB memory pool allowed\n");
        exit(EXIT_FAILURE);
    }
	ret = mdb_env_set_mapsize(env, mdb_settings.cache_size);
    
	/* check readers */
	mdb_env_get_maxreaders(env, &readers);
	if (settings.num_threads > readers) {
		readers = settings.num_threads;
		ret = mdb_env_set_maxreaders(env, readers);
	}


    /* if no home dir existed, we create it */
    if (0 != access(mdb_settings.env_home, F_OK)) {
        if (0 != mkdir(mdb_settings.env_home, 0750)) {
            fprintf(stderr, "mkdir env_home error:[%s]\n", mdb_settings.env_home);
            exit(EXIT_FAILURE);
        }
    }
    
    if ((ret = mdb_env_open(env, mdb_settings.env_home, mdb_settings.env_flags, 0640)) != 0) {
        fprintf(stderr, "mdb_env_open: %s\n", mdb_strerror(ret));
        exit(EXIT_FAILURE);
    }

    /* set DB_TXN_NOSYNC flag */
    if (mdb_settings.txn_nosync){
        mdb_env_set_flags(env, MDB_NOSYNC, 1);
    }

    if ((ret = mdb_txn_begin(env, NULL, 0, &txn)) != 0) {
        fprintf(stderr, "mdb_txn_begin: %s\n", mdb_strerror(ret));
        exit(EXIT_FAILURE);
    }

    if ((ret = mdb_open(txn, NULL, 0, &dbi)) != 0) {
        fprintf(stderr, "mdb_open: %s\n", mdb_strerror(ret));
        exit(EXIT_FAILURE);
    }
	mdb_txn_commit(txn);
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


static void *mdb_chkpoint_thread(void *arg)
{
    MDB_env *dbenv;
    int ret;
    dbenv = arg;
    if (settings.verbose > 1) {
        fprintf(stderr, "checkpoint thread created: %lu, every %d seconds\n", 
                           (u_long)pthread_self(), mdb_settings.chkpoint_val);
    }
    for (;; sleep(mdb_settings.chkpoint_val)) {
        if ((ret = mdb_env_sync(dbenv, 1)) != 0) {
            fprintf(stderr, "checkpoint thread: %s\n", mdb_strerror(ret));
        }
        fprintf(stderr, "checkpoint thread: a checkpoint is done\n");
    }
    return (NULL);
}

/* for atexit cleanup */
void mdb_shutdown(void){
    if (env != NULL) {
		mdb_env_close(env);
		env = NULL;
    }
}

/* for atexit cleanup */
void mdb_chkpoint(void)
{
    int ret = 0;
    if (env != NULL){
        ret = mdb_env_sync(env, 1);
        if (0 != ret){
            fprintf(stderr, "mdb_env_sync: %s\n", mdb_strerror(ret));
        }else{
            fprintf(stderr, "mdb_env_sync: OK\n");
        }
    }
}

