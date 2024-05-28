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

#ifndef _JLOG_PRIVATE_H
#define _JLOG_PRIVATE_H
/* vim:se ts=2 sw=2 et: */

#include "jlog_config.h"
#include "jlog.h"
#include "jlog_io.h"

#define DEFAULT_FILE_MODE 0640
#define DEFAULT_UNIT_LIMIT (4*1024*1024)
                         /* 4 Megabytes */
#define DEFAULT_HDR_MAGIC 0x663A7318
#define DEFAULT_HDR_MAGIC_COMPRESSION 0x15106A00
#define DEFAULT_SAFETY JLOG_ALMOST_SAFE
#define DEFAULT_READ_MESSAGE_TYPE JLOG_READ_METHOD_MMAP
#define INDEX_EXT ".idx"
#define MAXLOGPATHLEN (MAXPATHLEN - (8+sizeof(INDEX_EXT)))

static const char __jlog_hexchars[] = "0123456789abcdef";

typedef enum {
  JLOG_NEW = 0,
  JLOG_INIT,
  JLOG_READ,
  JLOG_APPEND,
  JLOG_INVALID
} jlog_mode;

struct _jlog_meta_info {
  u_int32_t storage_log;
  u_int32_t unit_limit;
  u_int32_t safety;
  u_int32_t hdr_magic;
};

struct _jlog_ctx {
  struct _jlog_meta_info *meta;
  pthread_mutex_t write_lock;
  jlog_read_method_type read_method;
  int       meta_is_mapped;
  int       pre_commit_is_mapped;
  uint8_t   multi_process;
  uint8_t   pre_commit_buffer_size_specified;
  void      *pre_commit_buffer;
  void      *pre_commit_pos;
  void      *pre_commit_end;
  size_t    pre_commit_buffer_len;
  size_t    desired_pre_commit_buffer_len;
  uint32_t  *pre_commit_pointer;
  struct _jlog_meta_info pre_init; /* only used before we're opened */
  jlog_mode context_mode;
  char      *path;
  int       file_mode;
  u_int32_t current_log;
  jlog_file *data;
  jlog_file *index;
  jlog_file *checkpoint;
  jlog_file *metastore;
  jlog_file *pre_commit;
  void     *mmap_base;
  size_t    mmap_len;
  size_t    data_file_size;
  uint8_t   reader_is_initialized;
  void     *compressed_data_buffer;
  size_t    compressed_data_buffer_len;
  char     *subscriber_name;
  int       last_error;
  int       last_errno;
  jlog_error_func error_func;
  void *error_ctx;

  /**
   * Store the last read message in the case of use_compression == 1.
   * There is an expectation from jlog user's that they are handed
   * mmap'd memory directly and that they don't have to free it, so we
   * have to maintain a block of memory where we can decompress messages
   * and hand them back.  The contract being that everytime jlog_ctx_read_message
   * is called, we overwrite this memory with the new message
   */
  size_t    mess_data_size;
  char      *mess_data;
};

/* macros */

#define STRLOGID(s, logid) do { \
  int __i; \
  for(__i=0;__i<8;__i++) \
    (s)[__i] = __jlog_hexchars[((logid) >> (32 - ((__i+1)*4))) & 0xf]; \
  (s)[__i] = '\0'; \
} while(0)

#define STRSETDATAFILE(ctx, file, log) do { \
  int __len; \
  __len = strlen((ctx)->path); \
  memcpy((file), (ctx)->path, __len); \
  (file)[__len] = IFS_CH; \
  STRLOGID((file)+(__len+1), log); \
} while(0)

#define SYS_FAIL_EX(a, dowarn) do { \
  if (ctx) { \
    ctx->last_error = (a); \
    ctx->last_errno = errno; \
    if(ctx->error_func && dowarn) { \
      ctx->error_func(ctx->error_ctx, \
                      "JLOG-%d error: %d (%s) errno: %d (%s)\n", __LINE__, \
                      ctx->last_error, jlog_ctx_err_string(ctx), \
                      ctx->last_errno, strerror(ctx->last_errno)); \
    } \
  } \
  goto finish; \
} while(0)

#define SYS_FAIL(a) SYS_FAIL_EX(a, 1)

/**
 * repairs a damaged datafile
 * @return 0 OK, >0 number of damaged segments removed, -1 repair failed
 * @internal
 */
JLOG_API(int) jlog_repair_datafile(jlog_ctx *ctx, u_int32_t log);
/**
 * prints detailed info about the log segment to stderr
 * @return 0 OK, 1 segment damaged, -1 other error
 * @internal
 */
JLOG_API(int) jlog_inspect_datafile(jlog_ctx *ctx, u_int32_t log, int verbose);
/**
 * fetches the last marker in the index and the closedness thereof
 * @return 0 OK, -1 error
 * @internal
 */
JLOG_API(int) jlog_idx_details(jlog_ctx *ctx, u_int32_t log,
                               u_int32_t *marker, int *closed);


#ifdef _WIN32
#include "ec_win32.h"
#endif

#endif
