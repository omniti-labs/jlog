/*
 * Copyright (c) 2001-2007 OmniTI, Inc. All rights reserved
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF OMNITI
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 *
 */

#ifndef _JLOG_HASH_H
#define _JLOG_HASH_H

#include "jlog_config.h"

typedef void (*JLogHashFreeFunc)(void *);

typedef struct _jlog_hash_bucket {
  const char *k;
  int klen;
  void *data;
  struct _jlog_hash_bucket *next;
} jlog_hash_bucket;

typedef struct {
  jlog_hash_bucket **buckets;
  u_int32_t table_size;
  u_int32_t initval;
  u_int32_t num_used_buckets;
  u_int32_t size;
  unsigned dont_rebucket:1;
  unsigned _spare:31;
} jlog_hash_table;

#define JLOG_HASH_EMPTY { NULL, 0, 0, 0, 0, 0, 0 }

typedef struct {
  void *p2;
  int p1;
} jlog_hash_iter;

#define JLOG_HASH_ITER_ZERO { NULL, 0 }

void jlog_hash_init(jlog_hash_table *h);
/* NOTE! "k" and "data" MUST NOT be transient buffers, as the hash table
 * implementation does not duplicate them.  You provide a pair of
 * JLogHashFreeFunc functions to free up their storage when you call
 * jlog_hash_delete(), jlog_hash_delete_all() or jlog_hash_destroy().
 * */
int jlog_hash_store(jlog_hash_table *h, const char *k, int klen, void *data);
int jlog_hash_replace(jlog_hash_table *h, const char *k, int klen, void *data,
                      JLogHashFreeFunc keyfree, JLogHashFreeFunc datafree);
int jlog_hash_retrieve(jlog_hash_table *h, const char *k, int klen, void **data);
int jlog_hash_delete(jlog_hash_table *h, const char *k, int klen,
                     JLogHashFreeFunc keyfree, JLogHashFreeFunc datafree);
void jlog_hash_delete_all(jlog_hash_table *h, JLogHashFreeFunc keyfree,
                          JLogHashFreeFunc datafree);
void jlog_hash_destroy(jlog_hash_table *h, JLogHashFreeFunc keyfree,
                       JLogHashFreeFunc datafree);

/* This is an iterator and requires the hash to not be written to during the
   iteration process.
   To use:
     jlog_hash_iter iter = JLOG_HASH_ITER_ZERO;

     const char *k;
     int klen;
     void *data;

     while(jlog_hash_next(h, &iter, &k, &klen, &data)) {
       .... use k, klen and data ....
     }
*/
int jlog_hash_next(jlog_hash_table *h, jlog_hash_iter *iter,
                   const char **k, int *klen, void **data);
int jlog_hash_firstkey(jlog_hash_table *h, const char **k, int *klen);
int jlog_hash_nextkey(jlog_hash_table *h, const char **k, int *klen, const char *lk, int lklen);

/* This function serves no real API use sans calculating expected buckets
   for keys (or extending the hash... which is unsupported) */
u_int32_t jlog_hash__hash(const char *k, u_int32_t length, u_int32_t initval);
jlog_hash_bucket *jlog_hash__new_bucket(const char *k, int klen, void *data);
void jlog_hash__rebucket(jlog_hash_table *h, int newsize);
#endif
