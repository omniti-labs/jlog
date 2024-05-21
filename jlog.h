/*
 * Copyright (c) 2005-2008, Message Systems, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following
 *      disclaimer in the documentation and/or other materials provided
 *      with the distribution.
 *    * Neither the name Message Systems, Inc. nor the names
 *      of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written
 *      permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef _JLOG_H
#define _JLOG_H

#include "jlog_config.h"

#ifndef JLOG_API
# ifdef _WIN32
#  ifdef JLOG_EXPORTS
#   define JLOG_API(x) __declspec(dllexport) x
#  else
#   define JLOG_API(x) __declspec(dllimport) x
#  endif
# else
#  ifdef __cplusplus
#    define JLOG_API(x)  extern "C" x
#  else
#    define JLOG_API(x)  x
#  endif
# endif
#endif

struct _jlog_ctx;
struct _jlog_message_header;
struct _jlog_id;

typedef struct _jlog_ctx jlog_ctx;

typedef struct _jlog_message_header {
  u_int32_t reserved;
  u_int32_t tv_sec;
  u_int32_t tv_usec;
  u_int32_t mlen;
} jlog_message_header;

typedef struct _jlog_message_header_compressed {
  u_int32_t reserved;
  u_int32_t tv_sec;
  u_int32_t tv_usec;
  u_int32_t mlen;
  u_int32_t compressed_len;
} jlog_message_header_compressed;

typedef struct _jlog_id {
  u_int32_t log;
  u_int32_t marker;
} jlog_id;

#define JLOG_ID_ADVANCE(id) (id)->marker++

typedef struct _jlog_message {
  jlog_message_header_compressed *header;
  u_int32_t mess_len;
  void *mess;
  jlog_message_header_compressed aligned_header;
} jlog_message;

typedef enum {
  JLOG_BEGIN,
  JLOG_END
} jlog_position;

typedef enum {
  JLOG_UNSAFE,
  JLOG_ALMOST_SAFE,
  JLOG_SAFE
} jlog_safety;

typedef enum {
  JLOG_ERR_SUCCESS = 0,
  JLOG_ERR_ILLEGAL_INIT,
  JLOG_ERR_ILLEGAL_OPEN,
  JLOG_ERR_OPEN,
  JLOG_ERR_NOTDIR,
  JLOG_ERR_CREATE_PATHLEN,
  JLOG_ERR_CREATE_EXISTS,
  JLOG_ERR_CREATE_MKDIR,
  JLOG_ERR_CREATE_META,
  JLOG_ERR_CREATE_PRE_COMMIT,
  JLOG_ERR_LOCK,
  JLOG_ERR_IDX_OPEN,
  JLOG_ERR_IDX_SEEK,
  JLOG_ERR_IDX_CORRUPT,
  JLOG_ERR_IDX_WRITE,
  JLOG_ERR_IDX_READ,
  JLOG_ERR_FILE_OPEN,
  JLOG_ERR_FILE_SEEK,
  JLOG_ERR_FILE_CORRUPT,
  JLOG_ERR_FILE_READ,
  JLOG_ERR_FILE_WRITE,
  JLOG_ERR_META_OPEN,
  JLOG_ERR_PRE_COMMIT_OPEN,
  JLOG_ERR_ILLEGAL_WRITE,
  JLOG_ERR_ILLEGAL_CHECKPOINT,
  JLOG_ERR_INVALID_SUBSCRIBER,
  JLOG_ERR_ILLEGAL_LOGID,
  JLOG_ERR_SUBSCRIBER_EXISTS,
  JLOG_ERR_CHECKPOINT,
  JLOG_ERR_NOT_SUPPORTED,
  JLOG_ERR_CLOSE_LOGID,
} jlog_err;

typedef enum {
  JLOG_COMPRESSION_NULL = 0,
  JLOG_COMPRESSION_LZ4 = 0x01
} jlog_compression_provider_choice;

typedef enum {
  JLOG_READ_METHOD_MMAP = 0,
  JLOG_READ_METHOD_PREAD
} jlog_read_method_type;

typedef void (*jlog_error_func) (void *ctx, const char *msg, ...);

JLOG_API(jlog_ctx *) jlog_new(const char *path);
JLOG_API(const char *) jlog_err_string(int);
JLOG_API(void)      jlog_set_error_func(jlog_ctx *ctx, jlog_error_func Func, void *ptr); 
JLOG_API(size_t)    jlog_raw_size(jlog_ctx *ctx);
JLOG_API(int)       jlog_ctx_init(jlog_ctx *ctx);
JLOG_API(int)       jlog_get_checkpoint(jlog_ctx *ctx, const char *s, jlog_id *id);
JLOG_API(int)       jlog_ctx_list_subscribers_dispose(jlog_ctx *ctx, char **subs);
JLOG_API(int)       jlog_ctx_list_subscribers(jlog_ctx *ctx, char ***subs);

JLOG_API(int)       jlog_ctx_err(jlog_ctx *ctx);
JLOG_API(const char *) jlog_ctx_err_string(jlog_ctx *ctx);
JLOG_API(int)       jlog_ctx_errno(jlog_ctx *ctx);
JLOG_API(int)       jlog_ctx_open_writer(jlog_ctx *ctx);
JLOG_API(int)       jlog_ctx_open_reader(jlog_ctx *ctx, const char *subscriber);
JLOG_API(int)       jlog_ctx_close(jlog_ctx *ctx);

JLOG_API(int)       jlog_ctx_alter_mode(jlog_ctx *ctx, int mode);
JLOG_API(int)       jlog_ctx_alter_journal_size(jlog_ctx *ctx, size_t size);
JLOG_API(int)       jlog_ctx_repair(jlog_ctx *ctx, int aggressive);
JLOG_API(int)       jlog_ctx_alter_safety(jlog_ctx *ctx, jlog_safety safety);
JLOG_API(int)       jlog_ctx_alter_read_method(jlog_ctx *ctx, jlog_read_method_type method);

/**
 * Control whether this jlog process should use multi-process safe file locks when performing 
 * reads or writes.  If you do not intend to use your jlog from multiple processes, you can 
 * call this function with a zero for the `mproc` argument.  Multi-process safety defaults to being 
 * on.
 * 
 * \sa jlog_ctx_set_pre_commit_buffer_size
 */
JLOG_API(int)       jlog_ctx_set_multi_process(jlog_ctx *ctx, uint8_t mproc);

/**
 * must be called after jlog_new and before the 'open' functions
 * defaults to using JLOG_COMPRESSION_LZ4
 */
JLOG_API(int)       jlog_ctx_set_use_compression(jlog_ctx *ctx, uint8_t use);

/**
 * Switch to another compression provider.  This has no effect on an already created jlog
 * as you cannot change compression after a jlog is inited.  If you want to change
 * the compression provider you must call this before `jlog_ctx_init`, ala:
 * 
 * ```ctx = jlog_new(path);
 * jlog_ctx_set_use_compression(ctx, 1);
 * jlog_ctx_set_compression_provider(ctx, JLOG_COMPRESSION_LZ4);
 * jlog_ctx_alter_journal_size(ctx, 1024000);
 * jlog_ctx_set_pre_commit_buffer_size(ctx, 1024 * 1024);
 * if(jlog_ctx_init(ctx) != 0) {
 * ...```
 * 
 */
JLOG_API(int)       jlog_ctx_set_compression_provider(jlog_ctx *ctx, jlog_compression_provider_choice provider);

/**
 * Turn on the use of a pre-commit buffer.  This will gain you increased throughput through reduction of 
 * `pwrite/v` syscalls.  Note however, care must be taken.  This is only safe for single writer
 * setups.  Merely setting multi-process to on does not protect the pre-commit space from being
 * corrupted by another writing process.  It is safe to use the pre-commit buffer if you have multiple 
 * reader processes and a single writer process, or if you read and write from within the same process.
 * 
 * If you intend to use multiple writing processes, you need to set the pre-commit buffer size to
 * zero (the default for safety).
 * 
 * There is a tradeoff here between throughput for jlog and read side latency.
 * Because reads only happen on materialized rows (rows stored in actual jlog files), a large 
 * pre_commit buffer size will delay the materialization of the log entries in the actual
 * storage files and therefore delay the read side.  
 * 
 * You should set this appropriately for your write throughput and read latency requirements
 * based on the rate you expect to be writing things to the log and the size of the average
 * logged item.
 * 
 * This must be called before `jlog_ctx_open_writer`
 * 
 */
JLOG_API(int)       jlog_ctx_set_pre_commit_buffer_size(jlog_ctx *ctx, size_t s);

/**
 * Provided to deal with read side latency problem.  If you intend to have a larg-ish pre-commit
 * buffer to have high throughput but have variability in your throughput there are times when
 * the rows won't be committed for the read side to see.  This call is provided to flush
 * the pre-commit buffer whenever you want.  Normally you would wire this up to a timer event.
 */
JLOG_API(int)       jlog_ctx_flush_pre_commit_buffer(jlog_ctx *ctx);

JLOG_API(int)       jlog_ctx_add_subscriber(jlog_ctx *ctx, const char *subscriber,
                                            jlog_position whence);
JLOG_API(int)       jlog_ctx_add_subscriber_copy_checkpoint(jlog_ctx *ctx, 
                                                            const char *new_subscriber,
                                                            const char *old_subscriber);
JLOG_API(int)       jlog_ctx_set_subscriber_checkpoint(jlog_ctx *ctx, const char *subscriber,
                                            const jlog_id *checkpoint);
JLOG_API(int)       jlog_ctx_remove_subscriber(jlog_ctx *ctx, const char *subscriber);

JLOG_API(int)       jlog_ctx_write(jlog_ctx *ctx, const void *message, size_t mess_len);
JLOG_API(int)       jlog_ctx_write_message(jlog_ctx *ctx, jlog_message *msg, struct timeval *when);
JLOG_API(int)       jlog_ctx_read_interval(jlog_ctx *ctx,
                                           jlog_id *first_mess, jlog_id *last_mess);
JLOG_API(int)       jlog_ctx_read_message(jlog_ctx *ctx, const jlog_id *, jlog_message *);
JLOG_API(int)       jlog_ctx_bulk_read_messages(jlog_ctx *ctx, const jlog_id *, const int, jlog_message *);
JLOG_API(int)       jlog_ctx_read_checkpoint(jlog_ctx *ctx, const jlog_id *checkpoint);
JLOG_API(int)       jlog_snprint_logid(char *buff, int n, const jlog_id *checkpoint);

JLOG_API(int)       jlog_pending_readers(jlog_ctx *ctx, u_int32_t log, u_int32_t *earliest_ptr);
JLOG_API(int)       __jlog_pending_readers(jlog_ctx *ctx, u_int32_t log);
JLOG_API(int)       jlog_ctx_first_log_id(jlog_ctx *ctx, jlog_id *id);
JLOG_API(int)       jlog_ctx_last_log_id(jlog_ctx *ctx, jlog_id *id);
JLOG_API(int)       jlog_ctx_last_storage_log(jlog_ctx *ctx, uint32_t *logid);
JLOG_API(int)       jlog_ctx_advance_id(jlog_ctx *ctx, jlog_id *cur, 
                                        jlog_id *start, jlog_id *finish);
JLOG_API(int)       jlog_clean(const char *path);

#endif
