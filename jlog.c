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

/*****************************************************************

  Journaled logging... append only.

      (1) find current file, or allocate a file, extendible and mark
          it current.
 
      (2) Write records to it, records include their size, so
          a simple inspection can detect and incomplete trailing
          record.
    
      (3) Write append until the file reaches a certain size.

      (4) Allocate a file, extensible.

      (5) RESYNC INDEX on 'finished' file (see reading:3) and postpend
          an offset '0' to the index.
    
      (2) goto (1)
    
  Reading journals...

      (1) find oldest checkpoint of all subscribers, remove all older files.

      (2) (file, last_read) = find_checkpoint for this subscriber

      (3) RESYNC INDEX:
          open record index for file, seek to the end -  off_t.
          this is the offset of the last noticed record in this file.
          open file, seek to this point, roll forward writing the index file
          _do not_ write an offset for the last record unless it is found
          complete.

      (4) read entries from last_read+1 -> index of record index

*/

#include <stdio.h>

#include "jlog_config.h"
#include "jlog_private.h"
#include "jlog_compress.h"
#if HAVE_UNISTD_H
#include <unistd.h>
#endif
#if HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif
#if HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#if HAVE_DIRENT_H
#include <dirent.h>
#endif
#if HAVE_FCNTL_H
#include <fcntl.h>
#endif
#if HAVE_ERRNO_H
#include <errno.h>
#endif
#if HAVE_TIME_H
#include <time.h>
#endif
#if HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif

#include "fassert.h"
#include <pthread.h>
#include <assert.h>

#define BUFFERED_INDICES 1024
#define PRE_COMMIT_BUFFER_SIZE_DEFAULT 0
#define IS_COMPRESS_MAGIC_HDR(hdr) ((hdr & DEFAULT_HDR_MAGIC_COMPRESSION) == DEFAULT_HDR_MAGIC_COMPRESSION)
#define IS_COMPRESS_MAGIC(ctx) IS_COMPRESS_MAGIC_HDR((ctx)->meta->hdr_magic)

static jlog_file *__jlog_open_writer(jlog_ctx *ctx);
static int __jlog_close_writer(jlog_ctx *ctx);
static jlog_file *__jlog_open_reader(jlog_ctx *ctx, u_int32_t log);
static int __jlog_close_reader(jlog_ctx *ctx);
static int __jlog_close_checkpoint(jlog_ctx *ctx);
static jlog_file *__jlog_open_indexer(jlog_ctx *ctx, u_int32_t log);
static int __jlog_close_indexer(jlog_ctx *ctx);
static int __jlog_resync_index(jlog_ctx *ctx, u_int32_t log, jlog_id *last, int *c);
static jlog_file *__jlog_open_named_checkpoint(jlog_ctx *ctx, const char *cpname, int flags);
static int __jlog_mmap_reader(jlog_ctx *ctx, u_int32_t log);
static int __jlog_munmap_reader(jlog_ctx *ctx);
static int __jlog_setup_reader(jlog_ctx *ctx, u_int32_t log, u_int8_t force_mmap);
static int __jlog_teardown_reader(jlog_ctx *ctx);
static int __jlog_metastore_atomic_increment(jlog_ctx *ctx);
static int __jlog_get_storage_bounds(jlog_ctx *ctx, unsigned int *earliest, unsigned *latest);
static int repair_metastore(jlog_ctx *ctx, const char *pth, unsigned int lat);
static int validate_metastore(const struct _jlog_meta_info *info, struct _jlog_meta_info *out);

int jlog_snprint_logid(char *b, int n, const jlog_id *id) {
  return snprintf(b, n, "%08x:%08x", id->log, id->marker);
}

int jlog_repair_datafile(jlog_ctx *ctx, u_int32_t log)
{
  jlog_message_header_compressed hdr;
  size_t hdr_size = sizeof(jlog_message_header);
  uint32_t *message_disk_len = &hdr.mlen;
  if (IS_COMPRESS_MAGIC(ctx)) {
    hdr_size = sizeof(jlog_message_header_compressed);
    message_disk_len = &hdr.compressed_len;
  }
  char *this, *next, *afternext = NULL, *mmap_end;
  int i, invalid_count = 0;
  struct {
    off_t start, end;
  } *invalid = NULL;
  off_t orig_len, src, dst, len;

#define TAG_INVALID(s, e) do { \
  if (invalid_count) \
    invalid = realloc(invalid, (invalid_count + 1) * sizeof(*invalid)); \
  else \
    invalid = malloc(sizeof(*invalid)); \
  invalid[invalid_count].start = s - (char *)ctx->mmap_base; \
  invalid[invalid_count].end = e - (char *)ctx->mmap_base; \
  invalid_count++; \
} while (0)

  ctx->last_error = JLOG_ERR_SUCCESS;

  /* we want the reader's open logic because this runs in the read path
   * the underlying fds are always RDWR anyway */
  __jlog_open_reader(ctx, log);
  if (!ctx->data) {
    ctx->last_error = JLOG_ERR_FILE_OPEN;
    ctx->last_errno = errno;
    return -1;
  }
  if (!jlog_file_lock(ctx->data)) {
    ctx->last_error = JLOG_ERR_LOCK;
    ctx->last_errno = errno;
    return -1;
  }
  if (__jlog_setup_reader(ctx, log, 1) != 0)
    SYS_FAIL(ctx->last_error);

  orig_len = ctx->mmap_len;
  mmap_end = (char*)ctx->mmap_base + ctx->mmap_len;
  /* these values will cause us to fall right into the error clause and
   * start searching for a valid header from offset 0 */
  this = (char*)ctx->mmap_base - hdr_size;
  hdr.reserved = ctx->meta->hdr_magic;
  hdr.mlen = 0;

  while (this + hdr_size <= mmap_end) {
    next = this + hdr_size + *message_disk_len;
    if (next <= (char *)ctx->mmap_base) goto error;
    if (next == mmap_end) {
      this = next;
      break;
    }
    if (next + hdr_size > mmap_end) goto error;
    memcpy(&hdr, next, hdr_size);
    if (hdr.reserved != ctx->meta->hdr_magic) goto error;
    this = next;
    continue;
  error:
    for (next = this + hdr_size; next + hdr_size <= mmap_end; next++) {
      memcpy(&hdr, next, hdr_size);
      if (hdr.reserved == ctx->meta->hdr_magic) {
        afternext = next + hdr_size + *message_disk_len;
        if (afternext <= (char *)ctx->mmap_base) continue;
        if (afternext == mmap_end) break;
        if (afternext + hdr_size > mmap_end) continue;
        memcpy(&hdr, afternext, hdr_size);
        if (hdr.reserved == ctx->meta->hdr_magic) break;
      }
    }
    /* correct for while loop entry condition */
    if (this < (char *)ctx->mmap_base) this = ctx->mmap_base;
    if (next + hdr_size > mmap_end) break;
    if (next > this) TAG_INVALID(this, next);
    this = afternext;
  }
  if (this != mmap_end) TAG_INVALID(this, mmap_end);

#undef TAG_INVALID

#define MOVE_SEGMENT do { \
  char cpbuff[4096]; \
  off_t chunk; \
  while(len > 0) { \
    chunk = len; \
    if (chunk > sizeof(cpbuff)) chunk = sizeof(cpbuff); \
    if (!jlog_file_pread(ctx->data, &cpbuff, chunk, src)) \
      SYS_FAIL(JLOG_ERR_FILE_READ); \
    if (!jlog_file_pwrite(ctx->data, &cpbuff, chunk, dst)) \
      SYS_FAIL(JLOG_ERR_FILE_WRITE); \
    src += chunk; \
    dst += chunk; \
    len -= chunk; \
  } \
} while (0)

  if (invalid_count > 0) {
    __jlog_teardown_reader(ctx);
    dst = invalid[0].start;
    for (i = 0; i < invalid_count - 1; ) {
      src = invalid[i].end;
      len = invalid[++i].start - src;
      MOVE_SEGMENT;
    }
    src = invalid[invalid_count - 1].end;
    len = orig_len - src;
    if (len > 0) MOVE_SEGMENT;
    if (!jlog_file_truncate(ctx->data, dst))
      SYS_FAIL(JLOG_ERR_FILE_WRITE);
  }

#undef MOVE_SEGMENT

finish:
  jlog_file_unlock(ctx->data);
  if (invalid) free(invalid);
  if (ctx->last_error != JLOG_ERR_SUCCESS) return -1;
  return invalid_count;
}

int jlog_inspect_datafile(jlog_ctx *ctx, u_int32_t log, int verbose)
{
  jlog_message_header_compressed hdr;
  size_t hdr_size = sizeof(jlog_message_header);
  uint32_t *message_disk_len = &hdr.mlen;
  char *this, *next, *mmap_end;
  int i;
  time_t timet;
  struct tm tm;
  char tbuff[128];

  if (IS_COMPRESS_MAGIC(ctx)) {
    hdr_size = sizeof(jlog_message_header_compressed);
    message_disk_len = &hdr.compressed_len;
  }

  ctx->last_error = JLOG_ERR_SUCCESS;

  __jlog_open_reader(ctx, log);
  if (!ctx->data)
    SYS_FAIL(JLOG_ERR_FILE_OPEN);
  if (__jlog_setup_reader(ctx, log, 1) != 0)
    SYS_FAIL(ctx->last_error);

  mmap_end = (char*)ctx->mmap_base + ctx->mmap_len;
  this = ctx->mmap_base;
  i = 0;
  while (this + hdr_size <= mmap_end) {
    int initial = 1;
    memcpy(&hdr, this, hdr_size);
    i++;
    if (hdr.reserved != ctx->meta->hdr_magic) {
      fprintf(stderr, "Message %d at [%ld] has invalid reserved value %u\n",
              i, (long int)(this - (char *)ctx->mmap_base), hdr.reserved);
      return 1;
    }

#define PRINTMSGHDR do { if(initial) { \
  fprintf(stderr, "Message %d at [%ld] of (%lu+%u)", i, \
          (long int)(this - (char *)ctx->mmap_base), \
          (long unsigned int)hdr_size, *message_disk_len); \
  initial = 0; \
} } while(0)

    if(verbose) {
      PRINTMSGHDR;
    }

    next = this + hdr_size + *message_disk_len;
    if (next <= (char *)ctx->mmap_base) {
      PRINTMSGHDR;
      fprintf(stderr, " WRAPPED TO NEGATIVE OFFSET!\n");
      return 1;
    }
    if (next > mmap_end) {
      PRINTMSGHDR;
      fprintf(stderr, " OFF THE END!\n");
      return 1;
    }

    timet = hdr.tv_sec;
    localtime_r(&timet, &tm);
    strftime(tbuff, sizeof(tbuff), "%c", &tm);
    if(verbose) fprintf(stderr, "\n\ttime: %s\n\tmlen: %u\n", tbuff, hdr.mlen);
    this = next;
  }
  if (this < mmap_end) {
    fprintf(stderr, "%ld bytes of junk at the end\n",
            (long int)(mmap_end - this));
    return 1;
  }

  return 0;
finish:
  return -1;
}

int jlog_idx_details(jlog_ctx *ctx, u_int32_t log,
                     u_int32_t *marker, int *closed)
{
  off_t index_len;
  u_int64_t index;

  __jlog_open_indexer(ctx, log);
  if (!ctx->index)
    SYS_FAIL(JLOG_ERR_IDX_OPEN);
  if ((index_len = jlog_file_size(ctx->index)) == -1)
    SYS_FAIL(JLOG_ERR_IDX_SEEK);
  if (index_len % sizeof(u_int64_t))
    SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
  if (index_len > sizeof(u_int64_t)) {
    if (!jlog_file_pread(ctx->index, &index, sizeof(u_int64_t),
                         index_len - sizeof(u_int64_t)))
    {
      SYS_FAIL(JLOG_ERR_IDX_READ);
    }
    if (index) {
      *marker = index_len / sizeof(u_int64_t);
      *closed = 0;
    } else {
      *marker = (index_len / sizeof(u_int64_t)) - 1;
      *closed = 1;
    }
  } else {
    *marker = index_len / sizeof(u_int64_t);
    *closed = 0;
  }

  return 0;
finish:
  return -1;
}

static int __jlog_unlink_datafile(jlog_ctx *ctx, u_int32_t log) {
  char file[MAXPATHLEN];
  int len;

  memset(file, 0, sizeof(file));
  if(ctx->current_log == log) {
    __jlog_close_reader(ctx);
    __jlog_close_indexer(ctx);
  }

  STRSETDATAFILE(ctx, file, log);
#ifdef DEBUG
  fprintf(stderr, "unlinking %s\n", file);
#endif
  unlink(file);

  len = strlen(file);
  if((len + sizeof(INDEX_EXT)) > sizeof(file)) return -1;
  memcpy(file + len, INDEX_EXT, sizeof(INDEX_EXT));
#ifdef DEBUG
  fprintf(stderr, "unlinking %s\n", file);
#endif
  unlink(file);
  return 0;
}

static int __jlog_open_metastore(jlog_ctx *ctx, int create)
{
  char file[MAXPATHLEN];
  int len;

  memset(file, 0, sizeof(file));
#ifdef DEBUG
  fprintf(stderr, "__jlog_open_metastore\n");
#endif
  len = strlen(ctx->path);
  if((len + 1 /* IFS_CH */ + 9 /* "metastore" */ + 1) > MAXPATHLEN) {
#ifdef ENAMETOOLONG
    ctx->last_errno = ENAMETOOLONG;
#endif
    FASSERT(ctx, 0, "__jlog_open_metastore: filename too long");
    ctx->last_error = JLOG_ERR_CREATE_META;
    return -1;
  }
  memcpy(file, ctx->path, len);
  file[len++] = IFS_CH;
  memcpy(&file[len], "metastore", 10); /* "metastore" + '\0' */

  ctx->metastore = jlog_file_open(file, create ? O_CREAT : O_RDWR, ctx->file_mode, ctx->multi_process);

  if (!ctx->metastore) {
    ctx->last_errno = errno;
    FASSERT(ctx, 0, "__jlog_open_metastore: file create failed");
    ctx->last_error = JLOG_ERR_CREATE_META;
    return -1;
  }

  return 0;
}

static char *
__jlog_pre_commit_file_name(jlog_ctx *ctx)
{
  char file[MAXPATHLEN] = {0};
  int len = 0;

#ifdef DEBUG
  fprintf(stderr, "__jlog_open_pre_commit\n");
#endif
  len = strlen(ctx->path);
  if((len + 1 /* IFS_CH */ + 10 /* "pre_commit" */ + 1) > MAXPATHLEN) {
#ifdef ENAMETOOLONG
    ctx->last_errno = ENAMETOOLONG;
#endif
    FASSERT(ctx, 0, "__jlog_open_pre_commit: filename too long");
    ctx->last_error = JLOG_ERR_CREATE_PRE_COMMIT;
    return NULL;
  }
  memcpy(file, ctx->path, len);
  file[len++] = IFS_CH;
  memcpy(&file[len], "pre_commit", 11); /* "pre_commit" + '\0' */

  return strdup(file);
}

static int __jlog_open_pre_commit(jlog_ctx *ctx)
{
#ifdef DEBUG
  fprintf(stderr, "__jlog_open_pre_commit\n");
#endif
  char *file = __jlog_pre_commit_file_name(ctx);
  if (file == NULL) {
    return -1;
  }

  ctx->pre_commit = jlog_file_open(file, O_CREAT, ctx->file_mode, ctx->multi_process);

  if (!ctx->pre_commit) {
    ctx->last_errno = errno;
    FASSERT(ctx, 0, "__jlog_open_pre_commit: file create failed");
    ctx->last_error = JLOG_ERR_CREATE_PRE_COMMIT;
    free(file);
    return -1;
  }
  free(file);
  return 0;
}


/* exported */
int __jlog_pending_readers(jlog_ctx *ctx, u_int32_t log) {
  return jlog_pending_readers(ctx, log, NULL);
}
int jlog_pending_readers(jlog_ctx *ctx, u_int32_t log,
                         u_int32_t *earliest_out) {
  int readers;
  DIR *dir;
  struct dirent *ent;
  char file[MAXPATHLEN];
  int len, seen = 0;
  u_int32_t earliest = 0;
  jlog_id id;

  memset(file, 0, sizeof(file));
  readers = 0;

  dir = opendir(ctx->path);
  if (!dir) return -1;

  len = strlen(ctx->path);
  if(len + 2 > sizeof(file)) {
    closedir(dir);
    return -1;
  }
  memcpy(file, ctx->path, len);
  file[len++] = IFS_CH;
  file[len] = '\0';

  while ((ent = readdir(dir))) {
    if (ent->d_name[0] == 'c' && ent->d_name[1] == 'p' && ent->d_name[2] == '.') {
      jlog_file *cp;
      int dlen;

      dlen = strlen(ent->d_name);
      if((len + dlen + 1) > sizeof(file)) continue;
      memcpy(file + len, ent->d_name, dlen + 1); /* include \0 */
#ifdef DEBUG
      fprintf(stderr, "Checking if %s needs %s...\n", ent->d_name, ctx->path);
#endif
      if ((cp = jlog_file_open(file, 0, ctx->file_mode, ctx->multi_process))) {
        if (jlog_file_lock(cp)) {
          (void) jlog_file_pread(cp, &id, sizeof(id), 0);
#ifdef DEBUG
          fprintf(stderr, "\t%u <= %u (pending reader)\n", id.log, log);
#endif
          if (!seen) {
            earliest = id.log;
            seen = 1;
          }
          else {
            if(id.log < earliest) {
              earliest = id.log;
            }
          }
          if (id.log <= log) {
            readers++;
          }
          jlog_file_unlock(cp);
        }
        jlog_file_close(cp);
      }
    }
  }
  closedir(dir);
  if(earliest_out) *earliest_out = earliest;
  return readers;
}
struct _jlog_subs {
  char **subs;
  int used;
  int allocd;
};

int jlog_ctx_list_subscribers_dispose(jlog_ctx *ctx, char **subs) {
  char *s;
  int i = 0;
  if(subs) {
    while((s = subs[i++]) != NULL) free(s);
    free(subs);
  }
  return 0;
}

int jlog_ctx_list_subscribers(jlog_ctx *ctx, char ***subs) {
  struct _jlog_subs js = { NULL, 0, 0 };
  DIR *dir;
  struct dirent *ent;
  unsigned char file[MAXPATHLEN];
  char *p;
  int len;

  memset(file, 0, sizeof(file));
  js.subs = calloc(16, sizeof(char *));
  js.allocd = 16;

  dir = opendir(ctx->path);
  if (!dir) return -1;
  while ((ent = readdir(dir))) {
    if (ent->d_name[0] == 'c' && ent->d_name[1] == 'p' && ent->d_name[2] == '.') {

      for (len = 0, p = ent->d_name + 3; *p;) {
        unsigned char c;
        int i;

        for (c = 0, i = 0; i < 16; i++) {
          if (__jlog_hexchars[i] == *p) {
            c = i << 4;
            break;
          }
        }
        p++;
        for (i = 0; i < 16; i++) {
          if (__jlog_hexchars[i] == *p) {
            c |= i;
            break;
          }
        }
        p++;
        file[len++] = c;
      }
      file[len] = '\0';

      js.subs[js.used++] = strdup((char *)file);
      if(js.used == js.allocd) {
        js.allocd *= 2;
        js.subs = realloc(js.subs, js.allocd*sizeof(char *));
      }
      js.subs[js.used] = NULL;
    }
  }
  closedir(dir);
  *subs = js.subs;
  return js.used;
}

static int __jlog_save_metastore(jlog_ctx *ctx, int ilocked)
{
#ifdef DEBUG
  fprintf(stderr, "__jlog_save_metastore\n");
#endif

  if(ctx->context_mode == JLOG_READ) {
    FASSERT(ctx, 0, "__jlog_save_metastore: illegal call in JLOG_READ mode");
    ctx->last_error = JLOG_ERR_ILLEGAL_WRITE;
    ctx->last_errno = EPERM;
    return -1;
  }

  if (!ilocked && !jlog_file_lock(ctx->metastore)) {
    FASSERT(ctx, 0, "__jlog_save_metastore: cannot get lock");
    ctx->last_error = JLOG_ERR_LOCK;
    return -1;
  }

  if(ctx->meta_is_mapped) {
    int rv, flags = MS_INVALIDATE;
    if(ctx->meta->safety == JLOG_SAFE) flags |= MS_SYNC;
    rv = msync((void *)(ctx->meta), sizeof(*ctx->meta), flags);
    FASSERT(ctx, rv >= 0, "jlog_save_metastore");
    if (!ilocked) jlog_file_unlock(ctx->metastore);
    if ( rv < 0 )
      ctx->last_error = JLOG_ERR_FILE_WRITE;
    return rv;
  }
  else {
    if (!jlog_file_pwrite(ctx->metastore, ctx->meta, sizeof(*ctx->meta), 0)) {
      if (!ilocked) jlog_file_unlock(ctx->metastore);
      FASSERT(ctx, 0, "jlog_file_pwrite failed");
      ctx->last_error = JLOG_ERR_FILE_WRITE;
      return -1;
    }
    if (ctx->meta->safety == JLOG_SAFE) {
      jlog_file_sync(ctx->metastore);
    }
  }

  if (!ilocked) jlog_file_unlock(ctx->metastore);
  return 0;
}

static int __jlog_restore_metastore(jlog_ctx *ctx, int ilocked, int readonly)
{
  void *base = NULL;
  size_t len = 0;
  if(ctx->meta_is_mapped) return 0;
#ifdef DEBUG
  fprintf(stderr, "__jlog_restore_metastore\n");
#endif

  if (!ilocked && !jlog_file_lock(ctx->metastore)) {
    FASSERT(ctx, 0, "__jlog_restore_metastore: cannot get lock");
    ctx->last_error = JLOG_ERR_LOCK;
    return -1;
  }

  if(ctx->meta_is_mapped == 0) {
    int rv;
    if (readonly == 1) {
      rv = jlog_file_map_read(ctx->metastore, &base, &len);
    } else {
      rv = jlog_file_map_rdwr(ctx->metastore, &base, &len);
    }

    FASSERT(ctx, rv == 1, "jlog_file_map_r*");
    if(rv != 1) {
      if (!ilocked) jlog_file_unlock(ctx->metastore);
      ctx->last_error = JLOG_ERR_OPEN;
      return -1;
    }
    if(len == 12) {
      /* old metastore format doesn't have the new magic hdr in it
       * we need to extend it by four bytes, but we know the hdr was
       * previously 0, so we write out zero.
       */
      u_int32_t dummy = 0;
      jlog_file_pwrite(ctx->metastore, &dummy, sizeof(dummy), 12);
      if (readonly == 1) {
        rv = jlog_file_map_read(ctx->metastore, &base, &len);
      } else {
        rv = jlog_file_map_rdwr(ctx->metastore, &base, &len);
      }
      unsigned int ear, lat;
      if(!__jlog_get_storage_bounds(ctx, &ear, &lat) ||
         !repair_metastore(ctx, NULL, lat)) {
        if (!ilocked) jlog_file_unlock(ctx->metastore);
        ctx->last_error = JLOG_ERR_OPEN;
        return -1;
      }
    }
    else {
      struct _jlog_meta_info *meta = base;
      if(len != sizeof(*meta) || !validate_metastore(meta, NULL)) {
        if(!repair_metastore(ctx, NULL, meta->storage_log)) {
          if (!ilocked) jlog_file_unlock(ctx->metastore);
          ctx->last_error = JLOG_ERR_OPEN;
          return -1;
        }
      }
    }
    FASSERT(ctx, rv == 1, "jlog_file_map_r*");
    if(rv != 1 || len != sizeof(*ctx->meta)) {
      if (!ilocked) jlog_file_unlock(ctx->metastore);
      ctx->last_error = JLOG_ERR_OPEN;
      return -1;
    }
    ctx->meta = base;
    ctx->meta_is_mapped = 1;

    if (IS_COMPRESS_MAGIC(ctx)) {
      jlog_set_compression_provider(ctx->meta->hdr_magic & 0xFF);
    }
  }

  if (!ilocked) jlog_file_unlock(ctx->metastore);

  if(ctx->meta != &ctx->pre_init)
    ctx->pre_init.hdr_magic = ctx->meta->hdr_magic;
  return 0;
}

static int __jlog_map_pre_commit(jlog_ctx *ctx)
{
  off_t pre_commit_size = 0;
#ifdef DEBUG
  fprintf(stderr, "__jlog_map_pre_commit\n");
#endif

  if(ctx->pre_commit_is_mapped == 1) {
    return 0;
  }

  if (!jlog_file_lock(ctx->pre_commit)) {
    FASSERT(ctx, 0, "__jlog_map_pre_commit: cannot get lock");
    ctx->last_error = JLOG_ERR_LOCK;
    return -1;
  }

  pre_commit_size = jlog_file_size(ctx->pre_commit);
  if (pre_commit_size == 0) {
    /* fill the pre_commit file with zero'd memory to hold incoming messages for block writes */
    /* add space for the offset in the file at the front of the buffer */
    char *space = calloc(1, ctx->desired_pre_commit_buffer_len + sizeof(uint32_t));
    if (!jlog_file_pwrite(ctx->pre_commit, space, ctx->desired_pre_commit_buffer_len + sizeof(uint32_t), 0)) {
      jlog_file_unlock(ctx->pre_commit);
      FASSERT(ctx, 0, "jlog_file_pwrite failed");
      ctx->last_error = JLOG_ERR_FILE_WRITE;
      free(space);
      return -1;
    }
    if (ctx->meta->safety == JLOG_SAFE) {
      jlog_file_sync(ctx->pre_commit);
    }

    free(space);
  }
  
  /* now map it */
  if (jlog_file_map_rdwr(ctx->pre_commit, (void **)&ctx->pre_commit_pointer, &ctx->pre_commit_buffer_len) == 0) {
      jlog_file_unlock(ctx->pre_commit);
      FASSERT(ctx, 0, "jlog_file_map_rdwr failed");
      ctx->last_error = JLOG_ERR_PRE_COMMIT_OPEN;
      return -1;
  }

  /* move the writable buffer past the offset pointer */
  ctx->pre_commit_buffer = (void *)ctx->pre_commit_pointer + sizeof(*ctx->pre_commit_pointer);
  /* the end is the total size minus the space for the leading write pointer location */
  ctx->pre_commit_end = ctx->pre_commit_buffer + ctx->pre_commit_buffer_len - sizeof(*ctx->pre_commit_pointer);

  /* restore the current pos */
  ctx->pre_commit_pos = ctx->pre_commit_buffer + *ctx->pre_commit_pointer;

  /* we're good */
  ctx->pre_commit_is_mapped = 1;
  jlog_file_unlock(ctx->pre_commit);
  return 0;
}


int jlog_get_checkpoint(jlog_ctx *ctx, const char *s, jlog_id *id) {
  jlog_file *f;
  int rv = -1;

  if(ctx->subscriber_name && (s == NULL || !strcmp(ctx->subscriber_name, s))) {
    if(!ctx->checkpoint) {
      ctx->checkpoint = __jlog_open_named_checkpoint(ctx, ctx->subscriber_name, 0);
    }
    f = ctx->checkpoint;
  } else
    f = __jlog_open_named_checkpoint(ctx, s, 0);

  if (f) {
    if (jlog_file_lock(f)) {
      if (jlog_file_pread(f, id, sizeof(*id), 0)) rv = 0;
      jlog_file_unlock(f);
    }
  }
  if (f && f != ctx->checkpoint) jlog_file_close(f);
  return rv;
}

static int __jlog_set_checkpoint(jlog_ctx *ctx, const char *s, const jlog_id *id)
{
  jlog_file *f;
  int rv = -1;
  jlog_id old_id;
  u_int32_t log;

  if(ctx->subscriber_name && !strcmp(ctx->subscriber_name, s)) {
    if(!ctx->checkpoint) {
      ctx->checkpoint = __jlog_open_named_checkpoint(ctx, s, 0);
    }
    f = ctx->checkpoint;
  } else
    f = __jlog_open_named_checkpoint(ctx, s, 0);

  if(!f) return -1;
  if (!jlog_file_lock(f))
    goto failset;

  if (jlog_file_size(f) == 0) {
    /* we're setting it for the first time, no segments were pending on it */
    old_id.log = id->log;
  } else {
    if (!jlog_file_pread(f, &old_id, sizeof(old_id), 0))
      goto failset;
  }
  if (!jlog_file_pwrite(f, id, sizeof(*id), 0)) {
    FASSERT(ctx, 0, "jlog_file_pwrite failed in jlog_set_checkpoint");
    ctx->last_error = JLOG_ERR_FILE_WRITE;
    goto failset;
  }
  if (ctx->meta->safety == JLOG_SAFE) {
    jlog_file_sync(f);
  }
  jlog_file_unlock(f);
  rv = 0;

  for (log = old_id.log; log < id->log; log++) {
    if (__jlog_pending_readers(ctx, log) == 0) {
      __jlog_unlink_datafile(ctx, log);
    }
  }

 failset:
  if (f && f != ctx->checkpoint) jlog_file_close(f);
  return rv;
}

static int __jlog_close_metastore(jlog_ctx *ctx) {
  if (ctx->metastore) {
    jlog_file_close(ctx->metastore);
    ctx->metastore = NULL;
  }
  if (ctx->meta_is_mapped) {
    munmap((void *)ctx->meta, sizeof(*ctx->meta));
    ctx->meta = &ctx->pre_init;
    ctx->meta_is_mapped = 0;
  }
  return 0;
}

static int __jlog_close_pre_commit(jlog_ctx *ctx) {
  if (ctx->pre_commit) {
    jlog_file_close(ctx->pre_commit);
    ctx->pre_commit = NULL;
  }
  if (ctx->pre_commit_is_mapped) {
    munmap((void *)ctx->pre_commit_pointer, ctx->pre_commit_buffer_len);
    ctx->pre_commit_pointer = NULL;
    ctx->pre_commit_buffer = NULL;
    ctx->pre_commit_end = NULL;
    ctx->pre_commit_buffer_len = 0;
    ctx->pre_commit_is_mapped = 0;
  }
  return 0;
}

/* path is assumed to be MAXPATHLEN */
static char *compute_checkpoint_filename(jlog_ctx *ctx, const char *subscriber, char *name)
{
  const char *sub;
  int len;

  /* build checkpoint filename */
  len = strlen(ctx->path);
  memcpy(name, ctx->path, len);
  name[len++] = IFS_CH;
  name[len++] = 'c';
  name[len++] = 'p';
  name[len++] = '.';
  for (sub = subscriber; *sub; ) {
    name[len++] = __jlog_hexchars[((*sub & 0xf0) >> 4)];
    name[len++] = __jlog_hexchars[(*sub & 0x0f)];
    sub++;
  }
  name[len] = '\0';

#ifdef DEBUG
  fprintf(stderr, "checkpoint %s filename is %s\n", subscriber, name);
#endif
  return name;
}

static jlog_file *__jlog_open_named_checkpoint(jlog_ctx *ctx, const char *cpname, int flags)
{
  char name[MAXPATHLEN];
  compute_checkpoint_filename(ctx, cpname, name);
  return jlog_file_open(name, flags, ctx->file_mode, ctx->multi_process);
}

static jlog_file *__jlog_open_reader(jlog_ctx *ctx, u_int32_t log) {
  char file[MAXPATHLEN];

  memset(file, 0, sizeof(file));
  if(ctx->current_log != log) {
    __jlog_close_reader(ctx);
    __jlog_close_indexer(ctx);
  }
  if(ctx->data) {
    return ctx->data;
  }
  STRSETDATAFILE(ctx, file, log);
#ifdef DEBUG
  fprintf(stderr, "opening log file[ro]: '%s'\n", file);
#endif
  ctx->data = jlog_file_open(file, 0, ctx->file_mode, ctx->multi_process);
  ctx->current_log = log;
  return ctx->data;
}

static int __jlog_munmap_reader(jlog_ctx *ctx) {
  if(ctx->mmap_base) {
    munmap(ctx->mmap_base, ctx->mmap_len);
    ctx->mmap_base = NULL;
    ctx->mmap_len = 0;
  }
  return 0;
}

static int __jlog_mmap_reader(jlog_ctx *ctx, u_int32_t log) {
  if(ctx->current_log == log && ctx->mmap_base) return 0;
  __jlog_open_reader(ctx, log);
  if(!ctx->data)
    return -1;
  if (!jlog_file_map_read(ctx->data, &ctx->mmap_base, &ctx->mmap_len)) {
    ctx->mmap_base = NULL;
    ctx->last_error = JLOG_ERR_FILE_READ;
    ctx->last_errno = errno;
    return -1;
  }
  ctx->data_file_size = ctx->mmap_len;
  return 0;
}

static int __jlog_setup_reader(jlog_ctx *ctx, u_int32_t log, u_int8_t force_mmap) {
  jlog_read_method_type read_method = ctx->read_method;

  switch (read_method) {
    case JLOG_READ_METHOD_MMAP:
      {
        int rv = __jlog_mmap_reader(ctx, log);
        if (rv != 0) {
          return -1;
        }
      }
      ctx->reader_is_initialized = 1;
      return 0;
    case JLOG_READ_METHOD_PREAD:
      if (force_mmap) {
        int rv = __jlog_mmap_reader(ctx, log);
        if (rv != 0) {
          return -1;
        }
      }
      else {
        off_t file_size = jlog_file_size(ctx->data);
        if (file_size < 0) {
          SYS_FAIL(JLOG_ERR_FILE_SEEK);
        }
        ctx->data_file_size = file_size;
      }
      ctx->reader_is_initialized = 1;
      return 0;
    default:
     SYS_FAIL(JLOG_ERR_NOT_SUPPORTED);
     break;
  }
 finish:
  return -1;
}

static int __jlog_teardown_reader(jlog_ctx *ctx) {
  ctx->reader_is_initialized = 0;
  ctx->data_file_size = 0;
  return __jlog_munmap_reader(ctx);
}

static jlog_file *__jlog_open_writer(jlog_ctx *ctx) {
  char file[MAXPATHLEN] = {0};

  if(ctx->data) {
    /* Still open */
    return ctx->data;
  }

  FASSERT(ctx, ctx != NULL, "__jlog_open_writer");
  if(!jlog_file_lock(ctx->metastore))
    SYS_FAIL(JLOG_ERR_LOCK);
  int x;
  x = __jlog_restore_metastore(ctx, 1, 0);
  if(x) {
    FASSERT(ctx, x == 0, "__jlog_open_writer calls jlog_restore_metastore");
    SYS_FAIL(JLOG_ERR_META_OPEN);
  }
  ctx->current_log =  ctx->meta->storage_log;
  STRSETDATAFILE(ctx, file, ctx->current_log);
#ifdef DEBUG
  fprintf(stderr, "opening log file[rw]: '%s'\n", file);
#endif
  ctx->data = jlog_file_open(file, O_CREAT, ctx->file_mode, ctx->multi_process);
  FASSERT(ctx, ctx->data != NULL, "__jlog_open_writer calls jlog_file_open");
  if ( ctx->data == NULL )
    ctx->last_error = JLOG_ERR_FILE_OPEN;
  else
    ctx->last_error = JLOG_ERR_SUCCESS;
 finish:
  jlog_file_unlock(ctx->metastore);
  return ctx->data;
}

static int __jlog_close_writer(jlog_ctx *ctx) {
  if (ctx->data) {
    jlog_file_sync(ctx->data);
    jlog_file_close(ctx->data);
    ctx->data = NULL;
  }
  return 0;
}

static int __jlog_close_reader(jlog_ctx *ctx) {
  __jlog_teardown_reader(ctx);
  if (ctx->data) {
    jlog_file_close(ctx->data);
    ctx->data = NULL;
  }
  return 0;
}

static int __jlog_close_checkpoint(jlog_ctx *ctx) {
  if (ctx->checkpoint) {
    jlog_file_close(ctx->checkpoint);
    ctx->checkpoint = NULL;
  }
  return 0;
}

static jlog_file *__jlog_open_indexer(jlog_ctx *ctx, u_int32_t log) {
  char file[MAXPATHLEN];
  int len;

  memset(file, 0, sizeof(file));
  if(ctx->current_log != log) {
    __jlog_close_reader(ctx);
    __jlog_close_indexer(ctx);
  }
  if(ctx->index) {
    return ctx->index;
  }
  STRSETDATAFILE(ctx, file, log);

  len = strlen(file);
  if((len + sizeof(INDEX_EXT)) > sizeof(file)) return NULL;
  memcpy(file + len, INDEX_EXT, sizeof(INDEX_EXT));
#ifdef DEBUG
  fprintf(stderr, "opening index file: '%s'\n", file);
#endif
  ctx->index = jlog_file_open(file, O_CREAT, ctx->file_mode, ctx->multi_process);
  ctx->current_log = log;
  return ctx->index;
}

static int __jlog_close_indexer(jlog_ctx *ctx) {
  if (ctx->index) {
    jlog_file_close(ctx->index);
    ctx->index = NULL;
  }
  return 0;
}

static int
___jlog_resync_index(jlog_ctx *ctx, u_int32_t log, jlog_id *last, int *closed) 
{
  u_int64_t indices[BUFFERED_INDICES];
  jlog_message_header_compressed logmhdr;
  uint32_t *message_disk_len = &logmhdr.mlen;
  off_t index_off, data_off, data_len, recheck_data_len;
  size_t hdr_size = sizeof(jlog_message_header);
  u_int64_t index;
  int i, second_try = 0;

  if (IS_COMPRESS_MAGIC(ctx)) {
    hdr_size = sizeof(jlog_message_header_compressed);
    message_disk_len = &logmhdr.compressed_len;
  }

  ctx->last_error = JLOG_ERR_SUCCESS;
  if(closed) *closed = 0;

  __jlog_open_reader(ctx, log);
  if (!ctx->data) {
    ctx->last_error = JLOG_ERR_FILE_OPEN;
    ctx->last_errno = errno;
    return -1;
  }

#define RESTART do { \
  if (second_try == 0) { \
    jlog_file_truncate(ctx->index, index_off); \
    jlog_file_unlock(ctx->index); \
    second_try = 1; \
    ctx->last_error = JLOG_ERR_SUCCESS; \
    goto restart; \
  } \
  SYS_FAIL(JLOG_ERR_IDX_CORRUPT); \
} while (0)

restart:
  __jlog_open_indexer(ctx, log);
  if (!ctx->index) {
    ctx->last_error = JLOG_ERR_IDX_OPEN;
    ctx->last_errno = errno;
    return -1;
  }
  if (!jlog_file_lock(ctx->index)) {
    ctx->last_error = JLOG_ERR_LOCK;
    ctx->last_errno = errno;
    return -1;
  }

  data_off = 0;
  if ((data_len = jlog_file_size(ctx->data)) == -1)
    SYS_FAIL(JLOG_ERR_FILE_SEEK);
  if (data_len == 0 && log < ctx->meta->storage_log) {
    __jlog_unlink_datafile(ctx, log);
    ctx->last_error = JLOG_ERR_FILE_OPEN;
    ctx->last_errno = ENOENT;
    return -1;
  }
  if ((index_off = jlog_file_size(ctx->index)) == -1)
    SYS_FAIL(JLOG_ERR_IDX_SEEK);

  if (index_off % sizeof(u_int64_t)) {
#ifdef DEBUG
    fprintf(stderr, "corrupt index [%llu]\n", index_off);
#endif
    RESTART;
  }

  if (index_off > sizeof(u_int64_t)) {
    if (!jlog_file_pread(ctx->index, &index, sizeof(index),
                         index_off - sizeof(u_int64_t)))
    {
      SYS_FAIL(JLOG_ERR_IDX_READ);
    }
    if (index == 0) {
      /* This log file has been "closed" */
#ifdef DEBUG
      fprintf(stderr, "index closed\n");
#endif
      if(last) {
        last->log = log;
        last->marker = (index_off / sizeof(u_int64_t)) - 1;
      }
      if(closed) *closed = 1;
      goto finish;
    } else {
      if (index > data_len) {
#ifdef DEBUG
        fprintf(stderr, "index told me to seek somehwere I can't\n");
#endif
        RESTART;
      }
      data_off = index;
    }
  }

  if (index_off > 0) {
    /* We are adding onto a partial index so we must advance a record */
    if (!jlog_file_pread(ctx->data, &logmhdr, hdr_size, data_off))
      SYS_FAIL(JLOG_ERR_FILE_READ);
    if ((data_off += hdr_size + *message_disk_len) > data_len) {
#ifdef DEBUG
      fprintf(stderr, "index overshoots %zd + %zd + %zd > %zd\n",
              data_off - hdr_size - *message_disk_len, hdr_size, *message_disk_len, data_len);
#endif
      RESTART;
    }
  }

  i = 0;
  while (data_off + hdr_size <= data_len) {
    off_t next_off = data_off;

    if (!jlog_file_pread(ctx->data, &logmhdr, hdr_size, data_off))
      SYS_FAIL(JLOG_ERR_FILE_READ);
    if (logmhdr.reserved != ctx->meta->hdr_magic) {
#ifdef DEBUG
      fprintf(stderr, "logmhdr.reserved == %d\n", logmhdr.reserved);
#endif
      SYS_FAIL(JLOG_ERR_FILE_CORRUPT);
    }
    if ((next_off += hdr_size + *message_disk_len) > data_len)
      break;

    /* Write our new index offset */
    indices[i++] = data_off;
    if(i >= BUFFERED_INDICES) {
#ifdef DEBUG
      fprintf(stderr, "writing %i offsets\n", i);
#endif
      if (!jlog_file_pwrite(ctx->index, indices, i * sizeof(u_int64_t), index_off))
        RESTART;
      index_off += i * sizeof(u_int64_t);
      i = 0;
    }
    data_off = next_off;
  }
  if(i > 0) {
#ifdef DEBUG
    fprintf(stderr, "writing %i offsets\n", i);
#endif
    if (!jlog_file_pwrite(ctx->index, indices, i * sizeof(u_int64_t), index_off))
      RESTART;
    index_off += i * sizeof(u_int64_t);
  }
  if(last) {
    last->log = log;
    last->marker = index_off / sizeof(u_int64_t);
  }
  if(log < ctx->meta->storage_log) {

    /* 
     * the writer may have moved on and incremented the storage_log
     * while we were building this index.  This doesn't mean 
     * that this index file is complete because the writer
     * probably wrote out data to the data file before incrementing
     * the storage_log.  
     * 
     * We need to recheck the data file length
     * and ensure it's not larger than when we checked it earlier
     * only if the data_len has not changed can we consider this index closed.
     * 
     * If the data file length has changed, simply RESTART and rebuild this index file
     */
    if ((recheck_data_len = jlog_file_size(ctx->data)) == -1) {
      SYS_FAIL(JLOG_ERR_FILE_SEEK);
    }

    if (recheck_data_len != data_len) {
#ifdef DEBUG
      fprintf(stderr, "data len changed, %llu -> %llu\n", data_off, recheck_data_len);
#endif
      RESTART;
    }

    if (data_off != data_len) {
#ifdef DEBUG
      fprintf(stderr, "closing index, but %llu != %llu\n", data_off, data_len);
#endif
      SYS_FAIL(JLOG_ERR_FILE_CORRUPT);
    }

    /* Special case: if we are closing, we next write a '0'
     * we can't write the closing marker if the data segment had no records
     * in it, since it will be confused with an index to offset 0 by the
     * next reader; this only happens when segments are repaired */
    if (index_off) {
      index = 0;
      if (!jlog_file_pwrite(ctx->index, &index, sizeof(u_int64_t), index_off)) {
#ifdef DEBUG
        fprintf(stderr, "null index\n");
#endif
        RESTART;
      }
      index_off += sizeof(u_int64_t);
    }
    if(closed) *closed = 1;
  }
#undef RESTART

finish:
  jlog_file_unlock(ctx->index);
#ifdef DEBUG
  fprintf(stderr, "index is %s\n", closed?(*closed?"closed":"open"):"unknown");
#endif
  if(ctx->last_error == JLOG_ERR_SUCCESS) return 0;
  return -1;
}

static int __jlog_resync_index(jlog_ctx *ctx, u_int32_t log, jlog_id *last, int *closed) {
  int attempts, rv = -1;
  for(attempts=0; attempts<4; attempts++) {
    rv = ___jlog_resync_index(ctx, log, last, closed);
    if(ctx->last_error == JLOG_ERR_SUCCESS) break;
    if(ctx->last_error == JLOG_ERR_FILE_OPEN ||
       ctx->last_error == JLOG_ERR_IDX_OPEN) break;

    /* We can't fix the file if someone may write to it again */
    if(log >= ctx->meta->storage_log) break;

    jlog_file_lock(ctx->index);
    /* it doesn't really matter what jlog_repair_datafile returns
     * we'll keep retrying anyway */
    jlog_repair_datafile(ctx, log);
    jlog_file_truncate(ctx->index, 0);
    jlog_file_unlock(ctx->index);
  }
  return rv;
}

jlog_ctx *jlog_new(const char *path) {
  jlog_ctx *ctx;
  ctx = calloc(1, sizeof(*ctx));
  ctx->meta = &ctx->pre_init;
  ctx->pre_init.unit_limit = DEFAULT_UNIT_LIMIT;
  ctx->pre_init.safety = DEFAULT_SAFETY;
  ctx->pre_init.hdr_magic = DEFAULT_HDR_MAGIC;
  ctx->file_mode = DEFAULT_FILE_MODE;
  ctx->read_method = DEFAULT_READ_MESSAGE_TYPE;
  ctx->context_mode = JLOG_NEW;
  ctx->path = strdup(path);
  ctx->desired_pre_commit_buffer_len = PRE_COMMIT_BUFFER_SIZE_DEFAULT;
  ctx->pre_commit_buffer_size_specified = 0;
  ctx->multi_process = 1;
  pthread_mutex_init(&ctx->write_lock, NULL);
  //  fassertxsetpath(path);
  return ctx;
}

void jlog_set_error_func(jlog_ctx *ctx, jlog_error_func Func, void *ptr) {
  ctx->error_func = Func;
  ctx->error_ctx = ptr;
}

size_t jlog_raw_size(jlog_ctx *ctx) {
  DIR *d;
  struct dirent *de;
  size_t totalsize = 0;
  int ferr, len;
  char filename[MAXPATHLEN] = {0};

  d = opendir(ctx->path);
  if(!d) return 0;
  len = strlen(ctx->path);
  memcpy(filename, ctx->path, len);
  filename[len++] = IFS_CH;
  while((de = readdir(d)) != NULL) {
    struct stat sb;
    int dlen;

    dlen = strlen(de->d_name);
    if((len + dlen + 1) > sizeof(filename)) continue;
    memcpy(filename+len, de->d_name, dlen + 1); /* include \0 */
    while((ferr = stat(filename, &sb)) == -1 && errno == EINTR);
    if(ferr == 0 && S_ISREG(sb.st_mode)) totalsize += sb.st_size;
  }
  closedir(d);
  return totalsize;
}

const char *jlog_ctx_err_string(jlog_ctx *ctx) {
  return jlog_err_string(ctx->last_error);
}
const char *jlog_err_string(int err) {
  switch (err) {
#define MSG_O_MATIC(x)  case x: return #x;
    MSG_O_MATIC( JLOG_ERR_SUCCESS);
    MSG_O_MATIC( JLOG_ERR_ILLEGAL_INIT);
    MSG_O_MATIC( JLOG_ERR_ILLEGAL_OPEN);
    MSG_O_MATIC( JLOG_ERR_OPEN);
    MSG_O_MATIC( JLOG_ERR_NOTDIR);
    MSG_O_MATIC( JLOG_ERR_CREATE_PATHLEN);
    MSG_O_MATIC( JLOG_ERR_CREATE_EXISTS);
    MSG_O_MATIC( JLOG_ERR_CREATE_MKDIR);
    MSG_O_MATIC( JLOG_ERR_CREATE_META);
    MSG_O_MATIC( JLOG_ERR_LOCK);
    MSG_O_MATIC( JLOG_ERR_IDX_OPEN);
    MSG_O_MATIC( JLOG_ERR_IDX_SEEK);
    MSG_O_MATIC( JLOG_ERR_IDX_CORRUPT);
    MSG_O_MATIC( JLOG_ERR_IDX_WRITE);
    MSG_O_MATIC( JLOG_ERR_IDX_READ);
    MSG_O_MATIC( JLOG_ERR_FILE_OPEN);
    MSG_O_MATIC( JLOG_ERR_FILE_SEEK);
    MSG_O_MATIC( JLOG_ERR_FILE_CORRUPT);
    MSG_O_MATIC( JLOG_ERR_FILE_READ);
    MSG_O_MATIC( JLOG_ERR_FILE_WRITE);
    MSG_O_MATIC( JLOG_ERR_META_OPEN);
    MSG_O_MATIC( JLOG_ERR_ILLEGAL_WRITE);
    MSG_O_MATIC( JLOG_ERR_ILLEGAL_CHECKPOINT);
    MSG_O_MATIC( JLOG_ERR_INVALID_SUBSCRIBER);
    MSG_O_MATIC( JLOG_ERR_ILLEGAL_LOGID);
    MSG_O_MATIC( JLOG_ERR_SUBSCRIBER_EXISTS);
    MSG_O_MATIC( JLOG_ERR_CHECKPOINT);
    MSG_O_MATIC( JLOG_ERR_NOT_SUPPORTED);
    MSG_O_MATIC( JLOG_ERR_CLOSE_LOGID);
    default: return "Unknown";
  }
}

int jlog_ctx_err(jlog_ctx *ctx) {
  return ctx->last_error;
}

int jlog_ctx_errno(jlog_ctx *ctx) {
  return ctx->last_errno;
}

int jlog_ctx_alter_safety(jlog_ctx *ctx, jlog_safety safety) {
  if(ctx->meta->safety == safety) return 0;
  if(ctx->context_mode == JLOG_APPEND ||
     ctx->context_mode == JLOG_NEW) {
    ctx->meta->safety = safety;
    if(ctx->context_mode == JLOG_APPEND) {
      if(__jlog_save_metastore(ctx, 0) != 0) {
        FASSERT(ctx, 0, "jlog_ctx_alter_safety calls jlog_save_metastore");
        SYS_FAIL(JLOG_ERR_CREATE_META);
      }
    }
    return 0;
  }
 finish:
  return -1;
}

int jlog_ctx_set_multi_process(jlog_ctx *ctx, uint8_t mp) {
  ctx->multi_process = mp;
  return 0;
}

int jlog_ctx_set_use_compression(jlog_ctx *ctx, uint8_t use) {
  if (use != 0) {
    ctx->pre_init.hdr_magic = DEFAULT_HDR_MAGIC_COMPRESSION | JLOG_COMPRESSION_LZ4;
    jlog_set_compression_provider(JLOG_COMPRESSION_LZ4);
  } else {
    ctx->pre_init.hdr_magic = DEFAULT_HDR_MAGIC;
  }    
  return 0;
}

int jlog_ctx_set_compression_provider(jlog_ctx *ctx, jlog_compression_provider_choice cp) {
  if ((ctx->pre_init.hdr_magic & DEFAULT_HDR_MAGIC_COMPRESSION) == DEFAULT_HDR_MAGIC_COMPRESSION) {
    /* compression mode is on, set the proper flag */
    ctx->pre_init.hdr_magic = DEFAULT_HDR_MAGIC_COMPRESSION | cp;
    jlog_set_compression_provider(cp);
  }
  return 0;
}

int jlog_ctx_set_pre_commit_buffer_size(jlog_ctx *ctx, size_t s) {
  ctx->desired_pre_commit_buffer_len = s;
  ctx->pre_commit_buffer_size_specified = 1;
  return 0;
}

static int 
_jlog_ctx_flush_pre_commit_buffer_no_lock(jlog_ctx *ctx)
{
  off_t current_offset = 0;

  /* noop if we aren't using a pre_commit buffer */
  if (ctx->pre_commit_buffer_len == 0) {
    return 0;
  }

  ctx->last_error = JLOG_ERR_SUCCESS;
  if(ctx->context_mode != JLOG_APPEND) {
    ctx->last_error = JLOG_ERR_ILLEGAL_WRITE;
    ctx->last_errno = EPERM;
    return -1;
  }
 begin:
  __jlog_open_writer(ctx);
  if(!ctx->data) {
    ctx->last_error = JLOG_ERR_FILE_OPEN;
    ctx->last_errno = errno;
    return -1;
  }

  if (!jlog_file_lock(ctx->data)) {
    ctx->last_error = JLOG_ERR_LOCK;
    ctx->last_errno = errno;
    return -1;
  }

  if ((current_offset = jlog_file_size(ctx->data)) == -1)
    SYS_FAIL(JLOG_ERR_FILE_SEEK);
  if(ctx->meta->unit_limit <= current_offset) {
    jlog_file_unlock(ctx->data);
    __jlog_close_writer(ctx);
    __jlog_metastore_atomic_increment(ctx);
    goto begin;
  }

  /* we have to flush our pre_commit_buffer out to the real log */
  if (!jlog_file_pwrite(ctx->data, ctx->pre_commit_buffer, 
                        ctx->pre_commit_pos - ctx->pre_commit_buffer, 
                        current_offset)) {
    FASSERT(ctx, 0, "jlog_file_pwrite failed in jlog_ctx_write_message");
    SYS_FAIL(JLOG_ERR_FILE_WRITE);
  }

  current_offset += ctx->pre_commit_pos - ctx->pre_commit_buffer;

  /* rewind the pre_commit_buffer to beginning */
  ctx->pre_commit_pos = ctx->pre_commit_buffer;
  /* ensure we save this in the mmapped data */
  *ctx->pre_commit_pointer = 0;

  if(ctx->meta->unit_limit <= current_offset) {
    jlog_file_unlock(ctx->data);
    __jlog_close_writer(ctx);
    __jlog_metastore_atomic_increment(ctx);
    return 0;
  }
 finish:
  jlog_file_unlock(ctx->data);
  if(ctx->last_error == JLOG_ERR_SUCCESS) return 0;
  return -1;
}

int jlog_ctx_flush_pre_commit_buffer(jlog_ctx *ctx) 
{
  int rv;
  if (ctx->pre_commit_buffer_len == 0) return 0;
  pthread_mutex_lock(&ctx->write_lock);
  rv = _jlog_ctx_flush_pre_commit_buffer_no_lock(ctx);
  pthread_mutex_unlock(&ctx->write_lock);
  return rv;
}

int jlog_ctx_alter_journal_size(jlog_ctx *ctx, size_t size) {
  if(ctx->meta->unit_limit == size) return 0;
  if(ctx->context_mode == JLOG_APPEND ||
     ctx->context_mode == JLOG_NEW) {
    ctx->meta->unit_limit = size;
    if(ctx->context_mode == JLOG_APPEND) {
      if(__jlog_save_metastore(ctx, 0) != 0) {
        FASSERT(ctx, 0, "jlog_ctx_alter_journal_size calls jlog_save_metastore");
        SYS_FAIL(JLOG_ERR_CREATE_META);
      }
    }
    return 0;
  }
 finish:
  return -1;
}
int jlog_ctx_alter_mode(jlog_ctx *ctx, int mode) {
  ctx->file_mode = mode;
  return 0;
}
int jlog_ctx_alter_read_method(jlog_ctx *ctx, jlog_read_method_type method) {
  /* Cannot change read method mid-processing */
  if (ctx->reader_is_initialized) {
    return -1;
  }
  ctx->read_method = method;
  return 0;
}
int jlog_ctx_open_writer(jlog_ctx *ctx) {
  int rv;
  struct stat sb;

  pthread_mutex_lock(&ctx->write_lock);
  ctx->last_error = JLOG_ERR_SUCCESS;
  if(ctx->context_mode != JLOG_NEW) {
    ctx->last_error = JLOG_ERR_ILLEGAL_OPEN;
    pthread_mutex_unlock(&ctx->write_lock);
    return -1;
  }
  ctx->context_mode = JLOG_APPEND;
  while((rv = stat(ctx->path, &sb)) == -1 && errno == EINTR);
  if(rv == -1) SYS_FAIL(JLOG_ERR_OPEN);
  if(!S_ISDIR(sb.st_mode)) SYS_FAIL(JLOG_ERR_NOTDIR);
  FASSERT(ctx, ctx != NULL, "jlog_ctx_open_writer");
  if(__jlog_open_metastore(ctx, 0) != 0) {
    FASSERT(ctx, 0, "jlog_ctx_open_writer calls jlog_open_metastore");
    SYS_FAIL(JLOG_ERR_META_OPEN);
  }
  if(__jlog_restore_metastore(ctx, 0, 0)) {
    FASSERT(ctx, 0, "jlog_ctx_open_writer calls jlog_restore_metastore");
    SYS_FAIL(JLOG_ERR_META_OPEN);
  }
  if (__jlog_open_pre_commit(ctx) != 0) {
    FASSERT(ctx, 0, "jlog_ctx_open_writer calls jlog_open_pre_commit");
    SYS_FAIL(JLOG_ERR_PRE_COMMIT_OPEN);
  }

  if (__jlog_map_pre_commit(ctx) != 0) {
    FASSERT(ctx, 0, "jlog_ctx_open_writer calls jlog_map_pre_commit");
    SYS_FAIL(JLOG_ERR_PRE_COMMIT_OPEN);
  }

  if (ctx->pre_commit_buffer_size_specified && ctx->pre_commit_buffer_len != ctx->desired_pre_commit_buffer_len) {
   
    _jlog_ctx_flush_pre_commit_buffer_no_lock(ctx);

    /* unmap it */
    __jlog_close_pre_commit(ctx);
    
    /* unlink the file */
    char *fn = __jlog_pre_commit_file_name(ctx);
    if (unlink(fn) != 0) {
      FASSERT(ctx, 0, "jlog_ctx_open_writer cannot unlink old pre_commit file");
      SYS_FAIL(JLOG_ERR_PRE_COMMIT_OPEN);
    } 
    free(fn);

    /* recreate on new size */
   if (__jlog_open_pre_commit(ctx) != 0) {
     FASSERT(ctx, 0, "jlog_ctx_open_writer calls jlog_open_pre_commit");
     SYS_FAIL(JLOG_ERR_PRE_COMMIT_OPEN);
   }

   if (__jlog_map_pre_commit(ctx) != 0) {
     FASSERT(ctx, 0, "jlog_ctx_open_writer calls jlog_map_pre_commit");
     SYS_FAIL(JLOG_ERR_PRE_COMMIT_OPEN);
   }
  }
    
 finish:
  pthread_mutex_unlock(&ctx->write_lock);
  if(ctx->last_error == JLOG_ERR_SUCCESS) return 0;
  ctx->context_mode = JLOG_INVALID;
  return -1;
}
int jlog_ctx_open_reader(jlog_ctx *ctx, const char *subscriber) {
  int rv;
  struct stat sb;
  jlog_id dummy;

  ctx->last_error = JLOG_ERR_SUCCESS;
  if(ctx->context_mode != JLOG_NEW) {
    ctx->last_error = JLOG_ERR_ILLEGAL_OPEN;
    return -1;
  }
  ctx->context_mode = JLOG_READ;
  ctx->subscriber_name = strdup(subscriber);
  while((rv = stat(ctx->path, &sb)) == -1 && errno == EINTR);
  if(rv == -1) SYS_FAIL(JLOG_ERR_OPEN);
  if(!S_ISDIR(sb.st_mode)) SYS_FAIL(JLOG_ERR_NOTDIR);
  FASSERT(ctx, ctx != NULL, "__jlog_ctx_open_reader");
  if(__jlog_open_metastore(ctx, 0) != 0) {
    FASSERT(ctx, 0, "jlog_ctx_open_reader calls jlog_open_metastore");
    SYS_FAIL(JLOG_ERR_META_OPEN);
  }
  if(jlog_get_checkpoint(ctx, ctx->subscriber_name, &dummy))
    SYS_FAIL(JLOG_ERR_INVALID_SUBSCRIBER);
  if(__jlog_restore_metastore(ctx, 0, 1)) {
    FASSERT(ctx, 0, "jlog_ctx_open_reader calls jlog_restore_metastore");
    SYS_FAIL(JLOG_ERR_META_OPEN);
  }
 finish:
  if(ctx->last_error == JLOG_ERR_SUCCESS) return 0;
  ctx->context_mode = JLOG_INVALID;
  return -1;
}

int jlog_ctx_init(jlog_ctx *ctx) {
  int rv;
  struct stat sb;
  int dirmode;

  ctx->multi_process = 1;
  ctx->last_error = JLOG_ERR_SUCCESS;
  if(strlen(ctx->path) > MAXLOGPATHLEN-1) {
    ctx->last_error = JLOG_ERR_CREATE_PATHLEN;
    return -1;
  }
  if(ctx->context_mode != JLOG_NEW) {
    ctx->last_error = JLOG_ERR_ILLEGAL_INIT;
    return -1;
  }
  ctx->context_mode = JLOG_INIT;
  while((rv = stat(ctx->path, &sb)) == -1 && errno == EINTR);
  if(rv == 0 || errno != ENOENT) {
    SYS_FAIL_EX(JLOG_ERR_CREATE_EXISTS, 0);
  }
  dirmode = ctx->file_mode;
  if(dirmode & 0400) dirmode |= 0100;
  if(dirmode & 040) dirmode |= 010;
  if(dirmode & 04) dirmode |= 01;
  if(mkdir(ctx->path, dirmode) == -1)
    SYS_FAIL(JLOG_ERR_CREATE_MKDIR);
  chmod(ctx->path, dirmode);
  // fassertxsetpath(ctx->path);
  /* Setup our initial state and store our instance metadata */
  if(__jlog_open_metastore(ctx,1) != 0) {
    FASSERT(ctx, 0, "jlog_ctx_init calls jlog_open_metastore");
    SYS_FAIL(JLOG_ERR_CREATE_META);
  }
  if(__jlog_save_metastore(ctx, 0) != 0) {
    FASSERT(ctx, 0, "jlog_ctx_init calls jlog_save_metastore");
    SYS_FAIL(JLOG_ERR_CREATE_META);
  }
  //  FASSERT(ctx, 0, "Start of fassert log");
 finish:
  FASSERT(ctx, ctx->last_error == JLOG_ERR_SUCCESS, "jlog_ctx_init failed");
  if(ctx->last_error == JLOG_ERR_SUCCESS) return 0;
  return -1;
}

int jlog_ctx_close(jlog_ctx *ctx) {
  jlog_ctx_flush_pre_commit_buffer(ctx);
  __jlog_close_writer(ctx);
  __jlog_close_pre_commit(ctx);
  __jlog_close_indexer(ctx);
  __jlog_close_reader(ctx);
  __jlog_close_metastore(ctx);
  __jlog_close_checkpoint(ctx);
  free(ctx->subscriber_name);
  free(ctx->path);
  free(ctx->compressed_data_buffer);
  free(ctx->mess_data);
  free(ctx);
  return 0;
}

static int __jlog_metastore_atomic_increment(jlog_ctx *ctx) {
  char file[MAXPATHLEN] = {0};

#ifdef DEBUG
  fprintf(stderr, "atomic increment on %u\n", ctx->current_log);
#endif
  FASSERT(ctx, ctx != NULL, "__jlog_metastore_atomic_increment");
  if(ctx->data) SYS_FAIL(JLOG_ERR_NOT_SUPPORTED);
  if (!jlog_file_lock(ctx->metastore))
    SYS_FAIL(JLOG_ERR_LOCK);
  if(__jlog_restore_metastore(ctx, 1, 0)) {
    FASSERT(ctx, 0,
            "jlog_metastore_atomic_increment calls jlog_restore_metastore");
    SYS_FAIL(JLOG_ERR_META_OPEN);
  }
  if(ctx->meta->storage_log == ctx->current_log) {
    /* We're the first ones to it, so we get to increment it */
    ctx->current_log++;
    STRSETDATAFILE(ctx, file, ctx->current_log);
    ctx->data = jlog_file_open(file, O_CREAT, ctx->file_mode, ctx->multi_process);
    ctx->meta->storage_log = ctx->current_log;
    if(__jlog_save_metastore(ctx, 1)) {
      FASSERT(ctx, 0,
              "jlog_metastore_atomic_increment calls jlog_save_metastore");
      SYS_FAIL(JLOG_ERR_META_OPEN);
    }
  }
 finish:
  jlog_file_unlock(ctx->metastore);
  /* Now we update our curent_log to the current storage_log,
   * it may have advanced farther than we know.
   */
  ctx->current_log = ctx->meta->storage_log;
  if(ctx->last_error == JLOG_ERR_SUCCESS) return 0;
  return -1;
}

int jlog_ctx_write_message(jlog_ctx *ctx, jlog_message *mess, struct timeval *when) {
  struct timeval now;
  jlog_message_header_compressed hdr;
  off_t current_offset = 0;
  size_t hdr_size = sizeof(jlog_message_header);
  int i = 0;

  if (IS_COMPRESS_MAGIC(ctx)) {
    hdr_size = sizeof(jlog_message_header_compressed);
  } 

  ctx->last_error = JLOG_ERR_SUCCESS;
  if(ctx->context_mode != JLOG_APPEND) {
    ctx->last_error = JLOG_ERR_ILLEGAL_WRITE;
    ctx->last_errno = EPERM;
    return -1;
  }

  /* build the data we want to write outside of any lock */
  hdr.reserved = ctx->meta->hdr_magic;
  if (when) {
    hdr.tv_sec = when->tv_sec;
    hdr.tv_usec = when->tv_usec;
  } else {
    gettimeofday(&now, NULL);
    hdr.tv_sec = now.tv_sec;
    hdr.tv_usec = now.tv_usec;
  }

  /* we store the original message size in the header */
  hdr.mlen = mess->mess_len;

  struct iovec v[2];
  v[0].iov_base = (void *) &hdr;
  v[0].iov_len = hdr_size;

  /* create a stack space to compress into which is large enough for most messages to compress into */
  char compress_space[16384] = {0};
  v[1].iov_base = compress_space;
  size_t compressed_len = sizeof(compress_space);

  if (IS_COMPRESS_MAGIC(ctx)) {
    if (jlog_compress(mess->mess, mess->mess_len, (char **)&v[1].iov_base, &compressed_len) != 0) {
      FASSERT(ctx, 0, "jlog_compress failed in jlog_ctx_write_message");
      SYS_FAIL(JLOG_ERR_FILE_WRITE);
    }
    hdr.compressed_len = compressed_len;
    v[1].iov_len = hdr.compressed_len;
  } else {
    v[1].iov_base = mess->mess;
    v[1].iov_len = mess->mess_len;
  }

  size_t total_size = v[0].iov_len + v[1].iov_len;

  /* now grab the file lock and write to pre_commit or file depending */
  /** 
   * this needs to be synchronized as concurrent writers can 
   * overwrite the shared ctx->data pointer as they move through
   * individual file segments.
   * 
   * Thread A-> open, write to existing segment, 
   * Thread B-> check open (already open)
   * Thread A-> close and null out ctx->data pointer
   * Thread B-> wha?!?
   */
  pthread_mutex_lock(&ctx->write_lock);
 begin:
  __jlog_open_writer(ctx);
  if(!ctx->data) {
    ctx->last_error = JLOG_ERR_FILE_OPEN;
    ctx->last_errno = errno;
    pthread_mutex_unlock(&ctx->write_lock);
    return -1;
  }

  if (!jlog_file_lock(ctx->data)) {
    ctx->last_error = JLOG_ERR_LOCK;
    ctx->last_errno = errno;
    pthread_mutex_unlock(&ctx->write_lock);
    return -1;
  }

  if (ctx->pre_commit_buffer_len > 0 && 
      ctx->pre_commit_pos + total_size > ctx->pre_commit_end) {

    if ((current_offset = jlog_file_size(ctx->data)) == -1)
      SYS_FAIL(JLOG_ERR_FILE_SEEK);

    if(ctx->meta->unit_limit <= current_offset) {
      jlog_file_unlock(ctx->data);
      __jlog_close_writer(ctx);
      __jlog_metastore_atomic_increment(ctx);
      goto begin;
    }

    /* we have to flush our pre_commit_buffer out to the real log */
    if (!jlog_file_pwrite(ctx->data, ctx->pre_commit_buffer, 
                          ctx->pre_commit_pos - ctx->pre_commit_buffer, 
                          current_offset)) {
      FASSERT(ctx, 0, "jlog_file_pwrite failed in jlog_ctx_write_message");
      SYS_FAIL(JLOG_ERR_FILE_WRITE);
    }
    /* rewind the pre_commit_buffer to beginning */
    ctx->pre_commit_pos = ctx->pre_commit_buffer;
    /* ensure we save this in the mmapped data */
    *ctx->pre_commit_pointer = 0;
  }

  if (total_size <= (ctx->pre_commit_buffer_len - sizeof(*ctx->pre_commit_pointer))) {
    /**
     * Write the iovecs to the pre-commit buffer 
     * 
     * This is protected by the file lock on the main data file so needs no special treatment
     */
    for (i = 0; i < 2; i++) {
      memcpy(ctx->pre_commit_pos, v[i].iov_base, v[i].iov_len);
      ctx->pre_commit_pos += v[i].iov_len;
      *ctx->pre_commit_pointer += v[i].iov_len;
    }
  } else {
    /* incoming message won't fit in pre_commit buffer, it was flushed above so write to file directly. */
    if (!jlog_file_pwritev_verify_return_value(ctx->data, v, 2, current_offset, total_size)) {
      FASSERT(ctx, 0, "jlog_file_pwritev failed in jlog_ctx_write_message");
      SYS_FAIL(JLOG_ERR_FILE_WRITE);
    }
    current_offset += total_size;;
  }

  if (IS_COMPRESS_MAGIC(ctx) && v[1].iov_base != compress_space) {
    free(v[1].iov_base);
  }

  if(ctx->meta->unit_limit <= current_offset) {
    jlog_file_unlock(ctx->data);
    __jlog_close_writer(ctx);
    __jlog_metastore_atomic_increment(ctx);
    pthread_mutex_unlock(&ctx->write_lock);
    return 0;
  }
 finish:
  jlog_file_unlock(ctx->data);
  pthread_mutex_unlock(&ctx->write_lock);
  if(ctx->last_error == JLOG_ERR_SUCCESS) return 0;
  return -1;
}

int jlog_ctx_read_checkpoint(jlog_ctx *ctx, const jlog_id *chkpt) {
  ctx->last_error = JLOG_ERR_SUCCESS;
  
  if(ctx->context_mode != JLOG_READ) {
    ctx->last_error = JLOG_ERR_ILLEGAL_CHECKPOINT;
    ctx->last_errno = EPERM;
    return -1;
  }
  if(__jlog_set_checkpoint(ctx, ctx->subscriber_name, chkpt) != 0) {
    ctx->last_error = JLOG_ERR_CHECKPOINT;
    ctx->last_errno = 0;
    return -1;
  }
  return 0;
}

int jlog_ctx_remove_subscriber(jlog_ctx *ctx, const char *s) {
  char name[MAXPATHLEN];
  int rv;

  compute_checkpoint_filename(ctx, s, name);
  rv = unlink(name);

  if (rv == 0) {
    ctx->last_error = JLOG_ERR_SUCCESS;
    return 1;
  }
  if (errno == ENOENT) {
    ctx->last_error = JLOG_ERR_INVALID_SUBSCRIBER;
    return 0;
  }
  return -1;
}

int jlog_ctx_add_subscriber(jlog_ctx *ctx, const char *s, jlog_position whence) {
  jlog_id chkpt;
  jlog_ctx *tmpctx = NULL;
  jlog_file *jchkpt;
  ctx->last_error = JLOG_ERR_SUCCESS;

  jchkpt = __jlog_open_named_checkpoint(ctx, s, O_CREAT|O_EXCL);
  if(!jchkpt) {
    ctx->last_errno = errno;
    if(errno == EEXIST)
      ctx->last_error = JLOG_ERR_SUBSCRIBER_EXISTS;
    else
      ctx->last_error = JLOG_ERR_OPEN;
    return -1;
  }
  jlog_file_close(jchkpt);
  
  if(whence == JLOG_BEGIN) {
    memset(&chkpt, 0, sizeof(chkpt));
    jlog_ctx_first_log_id(ctx, &chkpt);
    if(__jlog_set_checkpoint(ctx, s, &chkpt) != 0) {
      ctx->last_error = JLOG_ERR_CHECKPOINT;
      ctx->last_errno = 0;
      return -1;
    }
    return 0;
  }
  if(whence == JLOG_END) {
    jlog_id start, finish;
    memset(&chkpt, 0, sizeof(chkpt));
    FASSERT(ctx, ctx != NULL, "__jlog_ctx_add_subscriber");
    if(__jlog_open_metastore(ctx,0) != 0) {
      FASSERT(ctx, 0, "jlog_ctx_add_subscriber calls jlog_open_metastore");
      SYS_FAIL(JLOG_ERR_META_OPEN);
    }
    if(__jlog_restore_metastore(ctx, 0, 1)) {
      FASSERT(ctx, 0, "jlog_ctx_add_subscriber calls jlog_restore_metastore");
      SYS_FAIL(JLOG_ERR_META_OPEN);
    }
    chkpt.log = ctx->meta->storage_log;
    if(__jlog_set_checkpoint(ctx, s, &chkpt) != 0)
      SYS_FAIL(JLOG_ERR_CHECKPOINT);
    tmpctx = jlog_new(ctx->path);
    if(jlog_ctx_open_reader(tmpctx, s) < 0) goto finish;
    if(jlog_ctx_read_interval(tmpctx, &start, &finish) < 0) goto finish;
    jlog_ctx_close(tmpctx);
    tmpctx = NULL;
    if(__jlog_set_checkpoint(ctx, s, &finish) != 0)
      SYS_FAIL(JLOG_ERR_CHECKPOINT);
    return 0;
  }
  ctx->last_error = JLOG_ERR_NOT_SUPPORTED;
 finish:
  if(tmpctx) jlog_ctx_close(tmpctx);
  return -1;
}

int jlog_ctx_add_subscriber_copy_checkpoint(jlog_ctx *old_ctx, const char *new,
                                const char *old) {
  jlog_id chkpt;
  jlog_ctx *new_ctx = NULL;

  /* If there's no old checkpoint available, just return */
  if (jlog_get_checkpoint(old_ctx, old, &chkpt)) {
    return -1;
  }

  /* If we can't open the jlog_ctx, just return */
  new_ctx = jlog_new(old_ctx->path);
  if (!new_ctx) {
    return -1;
  }
  if (jlog_ctx_add_subscriber(new_ctx, new, JLOG_BEGIN)) {
    /* If it already exists, we want to overwrite it */
    if (errno != EEXIST) {
      jlog_ctx_close(new_ctx);
      return -1;
    }
  }

  /* Open a reader for the new subscriber */
  if(jlog_ctx_open_reader(new_ctx, new) < 0) {
    jlog_ctx_close(new_ctx);
    return -1;
  }

  /* Set the checkpoint of the new subscriber to 
   * the old subscriber's checkpoint */
  if (jlog_ctx_read_checkpoint(new_ctx, &chkpt)) {
    return -1;
  }

  jlog_ctx_close(new_ctx);
  return 0;
}

int jlog_ctx_set_subscriber_checkpoint(jlog_ctx *ctx, const char *s,
                                const jlog_id *checkpoint) 
{

  if (jlog_ctx_add_subscriber(ctx, s, JLOG_BEGIN)) {
    if (errno != EEXIST) {
      return -1;
    }
  }

  return __jlog_set_checkpoint(ctx, s, checkpoint);
}


int jlog_ctx_write(jlog_ctx *ctx, const void *data, size_t len) {
  jlog_message m;
  m.mess = (void *)data;
  m.mess_len = len;
  return jlog_ctx_write_message(ctx, &m, NULL);
}

static int __jlog_find_first_log_after(jlog_ctx *ctx, jlog_id *chkpt,
                                jlog_id *start, jlog_id *finish) {
  jlog_id last;
  int closed;

  memcpy(start, chkpt, sizeof(*chkpt));
 attempt:
  if(__jlog_resync_index(ctx, start->log, &last, &closed) != 0) {
    if(ctx->last_error == JLOG_ERR_FILE_OPEN &&
        ctx->last_errno == ENOENT) {
      char file[MAXPATHLEN];
      int ferr;
      struct stat sb = {0};

      memset(file, 0, sizeof(file));
      STRSETDATAFILE(ctx, file, start->log + 1);
      while((ferr = stat(file, &sb)) == -1 && errno == EINTR);
      /* That file doesn't exist... bad, but we can fake a recovery by
         advancing the next file that does exist */
      ctx->last_error = JLOG_ERR_SUCCESS;
      if(start->log >= ctx->meta->storage_log || (ferr != 0 && errno != ENOENT)) {
        /* We don't advance past where people are writing */
        memcpy(finish, start, sizeof(*start));
        return 0;
      }
      start->marker = 0;
      start->log++;  /* BE SMARTER! */
      goto attempt;
    }
    return -1; /* Just persist resync's error state */
  }

  /* If someone checkpoints off the end, be nice */
  if(last.log == start->log && last.marker < start->marker)
    memcpy(start, &last, sizeof(*start));

  if(!memcmp(start, &last, sizeof(last)) && closed) {
    if(start->log >= ctx->meta->storage_log) {
      /* We don't advance past where people are writing */
      memcpy(finish, start, sizeof(*start));
      return 0;
    }
    start->marker = 0;
    start->log++;
    goto attempt;
  }
  memcpy(finish, &last, sizeof(last));
  return 0;
}
int jlog_ctx_read_message(jlog_ctx *ctx, const jlog_id *id, jlog_message *m) {
  off_t index_len;
  u_int64_t data_off;
  int with_lock = 0;
  size_t hdr_size = 0;
  uint32_t *message_disk_len = &m->aligned_header.mlen;
  /* We don't want the style to change mid-read, so use whatever
   * the style is now */
  jlog_read_method_type read_method = ctx->read_method;

  if (IS_COMPRESS_MAGIC(ctx)) {
    hdr_size = sizeof(jlog_message_header_compressed);
    message_disk_len = &m->aligned_header.compressed_len;
  } else {
    hdr_size = sizeof(jlog_message_header);
  }

 once_more_with_lock:

  ctx->last_error = JLOG_ERR_SUCCESS;
  if (ctx->context_mode != JLOG_READ)
    SYS_FAIL(JLOG_ERR_ILLEGAL_WRITE);
  if (id->marker < 1) {
    SYS_FAIL(JLOG_ERR_ILLEGAL_LOGID);
  }

  __jlog_open_reader(ctx, id->log);
  if(!ctx->data)
    SYS_FAIL(JLOG_ERR_FILE_OPEN);
  __jlog_open_indexer(ctx, id->log);
  if(!ctx->index)
    SYS_FAIL(JLOG_ERR_IDX_OPEN);

  if(with_lock) {
    if (!jlog_file_lock(ctx->index)) {
      with_lock = 0;
      SYS_FAIL(JLOG_ERR_LOCK);
    }
  }

  if ((index_len = jlog_file_size(ctx->index)) == -1)
    SYS_FAIL(JLOG_ERR_IDX_SEEK);
  if (index_len % sizeof(u_int64_t))
    SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
  if (id->marker * sizeof(u_int64_t) > index_len) {
    SYS_FAIL(JLOG_ERR_ILLEGAL_LOGID);
  }

  if (!jlog_file_pread(ctx->index, &data_off, sizeof(u_int64_t),
                       (id->marker - 1) * sizeof(u_int64_t)))
  {
    SYS_FAIL(JLOG_ERR_IDX_READ);
  }
  if (data_off == 0 && id->marker != 1) {
    if (id->marker * sizeof(u_int64_t) == index_len) {
      /* close tag; not a real offset */
      ctx->last_error = JLOG_ERR_CLOSE_LOGID;
      ctx->last_errno = 0;
      if(with_lock) jlog_file_unlock(ctx->index);
      return -1;
    } else {
      /* an offset of 0 in the middle of an index means corruption */
      SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
    }
  }

  if(__jlog_setup_reader(ctx, id->log, 0) != 0)
    SYS_FAIL(ctx->last_error);

  switch(read_method) {
    case JLOG_READ_METHOD_MMAP:
      if(data_off > ctx->mmap_len - hdr_size) {
#ifdef DEBUG
        fprintf(stderr, "read idx off end: %llu\n", data_off);
#endif
        SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
      }

      memcpy(&m->aligned_header, ((u_int8_t *)ctx->mmap_base) + data_off,
             hdr_size);

      if(data_off + hdr_size + *message_disk_len > ctx->mmap_len) {
#ifdef DEBUG
        fprintf(stderr, "read idx off end: %llu %llu\n", data_off, ctx->mmap_len);
#endif
        SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
      }
      m->header = &m->aligned_header;
      if (IS_COMPRESS_MAGIC(ctx)) {
        if (ctx->mess_data_size < m->aligned_header.mlen) {
          ctx->mess_data = realloc(ctx->mess_data, m->aligned_header.mlen * 2);
          ctx->mess_data_size = m->aligned_header.mlen * 2;
        }
        jlog_decompress((((char *)ctx->mmap_base) + data_off + hdr_size),
                        m->header->compressed_len, ctx->mess_data, ctx->mess_data_size);
        m->mess_len = m->header->mlen;
        m->mess = ctx->mess_data;
      } else {
        m->mess_len = m->header->mlen;
        m->mess = (((u_int8_t *)ctx->mmap_base) + data_off + hdr_size);
      }
      break;
    case JLOG_READ_METHOD_PREAD:
      if(data_off > ctx->data_file_size - hdr_size) {
#ifdef DEBUG
        fprintf(stderr, "read idx off end: %llu\n", data_off);
#endif
        SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
      }
      if (!jlog_file_pread(ctx->data, &m->aligned_header, hdr_size, data_off))
      {
        SYS_FAIL(JLOG_ERR_IDX_READ);
      }
      if(data_off + hdr_size + *message_disk_len > ctx->data_file_size) {
#ifdef DEBUG
        fprintf(stderr, "read idx off end: %llu %llu\n", data_off, ctx->data_file_size);
#endif
        SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
      }
      m->header = &m->aligned_header;
      if (ctx->mess_data_size < m->aligned_header.mlen) {
        ctx->mess_data_size = m->aligned_header.mlen * 2;
        ctx->mess_data = realloc(ctx->mess_data, ctx->mess_data_size);
      }
      if (IS_COMPRESS_MAGIC(ctx)) {
        if (ctx->compressed_data_buffer_len < m->aligned_header.compressed_len) {
          ctx->compressed_data_buffer_len = m->aligned_header.compressed_len * 2;
          ctx->compressed_data_buffer = realloc(ctx->compressed_data_buffer, ctx->compressed_data_buffer_len);
        }
        if (!jlog_file_pread(ctx->data, ctx->compressed_data_buffer, m->aligned_header.compressed_len, data_off + hdr_size)) {
          SYS_FAIL(JLOG_ERR_IDX_READ);
        }
        jlog_decompress((char *)ctx->compressed_data_buffer,
                        m->header->compressed_len, ctx->mess_data, ctx->mess_data_size);
      } else {
        if (!jlog_file_pread(ctx->data, ctx->mess_data, m->aligned_header.mlen, data_off + hdr_size)) {
          SYS_FAIL(JLOG_ERR_IDX_READ);
        }
      }
      m->mess_len = m->header->mlen;
      m->mess = ctx->mess_data;
      break;
    default:
      break;
  }

 finish:
  if(with_lock) jlog_file_unlock(ctx->index);
  if(ctx->last_error == JLOG_ERR_SUCCESS) return 0;
  if(!with_lock) {
    if (ctx->last_error == JLOG_ERR_IDX_CORRUPT) {
      if (jlog_file_lock(ctx->index)) {
        jlog_file_truncate(ctx->index, 0);
        jlog_file_unlock(ctx->index);
      }
    }
    ___jlog_resync_index(ctx, id->log, NULL, NULL);
    with_lock = 1;
#ifdef DEBUG
    fprintf(stderr, "read retrying with lock\n");
#endif
    goto once_more_with_lock;
  }
  return -1;
}

static int __jlog_ctx_bulk_read_messages_compressed(jlog_ctx *ctx, const jlog_id *id, const int count,
                                                    jlog_message *m, u_int64_t data_off,
                                                    jlog_read_method_type read_method) {
  assert(IS_COMPRESS_MAGIC(ctx));
  assert(ctx->reader_is_initialized);

  int i = 0;
  uint64_t uncompressed_size = 0, compressed_size = 0;
  const size_t hdr_size = sizeof(jlog_message_header_compressed);
  jlog_message *msg = NULL;
  u_int64_t data_off_iter = data_off;

  for (i=0; i < count; i++) {
    msg = &m[i];
    switch(read_method) {
      case JLOG_READ_METHOD_MMAP:
        if(data_off_iter > ctx->mmap_len - hdr_size) {
#ifdef DEBUG
          fprintf(stderr, "read idx off end: %llu\n", data_off_iter);
#endif
          SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
        }
        memcpy(&msg->aligned_header, ((u_int8_t *)ctx->mmap_base) + data_off_iter, hdr_size);
        break;
      case JLOG_READ_METHOD_PREAD:
        if(data_off_iter > ctx->data_file_size - hdr_size) {
#ifdef DEBUG
          fprintf(stderr, "read idx off end: %llu\n", data_off_iter);
#endif
          SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
        }
        if (!jlog_file_pread(ctx->data, &msg->aligned_header, hdr_size, data_off_iter)) {
          SYS_FAIL(JLOG_ERR_IDX_READ);
        }
        break;
      default:
        SYS_FAIL(JLOG_ERR_NOT_SUPPORTED);
        break;
    }
    msg->header = &msg->aligned_header;
    compressed_size += msg->header->compressed_len;
    uncompressed_size += msg->header->mlen;
    data_off_iter += (hdr_size + msg->header->compressed_len);
  }
  if(data_off + (hdr_size * count) + compressed_size > ctx->data_file_size) {
#ifdef DEBUG
    fprintf(stderr, "read idx off end: %llu %llu\n", data_off, ctx->data_file_size);
#endif
    SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
  }
  data_off_iter = data_off;
  if (ctx->mess_data_size < uncompressed_size) {
    ctx->mess_data = realloc(ctx->mess_data, uncompressed_size + 1);
    ctx->mess_data_size = uncompressed_size;
  }
  char *uncompressed_data_ptr = ctx->mess_data;
  for (i=0; i < count; i++) {
    msg = &m[i];
    switch(read_method) {
      case JLOG_READ_METHOD_MMAP:
        jlog_decompress((((char *)ctx->mmap_base) + data_off_iter + hdr_size),
                        msg->header->compressed_len, uncompressed_data_ptr, msg->header->mlen);
        break;
      case JLOG_READ_METHOD_PREAD:
        if (ctx->compressed_data_buffer_len < msg->aligned_header.compressed_len) {
          ctx->compressed_data_buffer_len = msg->aligned_header.compressed_len * 2;
          ctx->compressed_data_buffer = realloc(ctx->compressed_data_buffer, ctx->compressed_data_buffer_len);
        }
        if (!jlog_file_pread(ctx->data, ctx->compressed_data_buffer, msg->header->compressed_len, data_off_iter + hdr_size)) {
          SYS_FAIL(JLOG_ERR_IDX_READ);
        }
        jlog_decompress((char *)ctx->compressed_data_buffer,
                        msg->header->compressed_len, uncompressed_data_ptr, msg->header->mlen);
        break;
      default:
        SYS_FAIL(JLOG_ERR_NOT_SUPPORTED);
        break;
    }
    msg->mess_len = msg->header->mlen;
    msg->mess = uncompressed_data_ptr;
    data_off_iter += (hdr_size + msg->header->compressed_len);
    uncompressed_data_ptr += msg->header->mlen;
  }
 finish:
  if(ctx->last_error == JLOG_ERR_SUCCESS) {
    return count;
  }
  return -1;
}

static int __jlog_ctx_bulk_pread_messages_uncompressed(jlog_ctx *ctx, const jlog_id *id, const int count,
                                                       jlog_message *m, u_int64_t data_off) {
  assert(!IS_COMPRESS_MAGIC(ctx));
  assert(ctx->reader_is_initialized);

  int i = 0;
  uint64_t total_size = 0;
  const size_t hdr_size = sizeof(jlog_message_header);
  jlog_message *msg = NULL;
  u_int64_t data_off_iter = data_off;

  for (i=0; i < count; i++) {
    msg = &m[i];
    if(data_off_iter > ctx->data_file_size - hdr_size) {
#ifdef DEBUG
      fprintf(stderr, "read idx off end: %llu\n", data_off_iter);
#endif
      SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
    }
    if (!jlog_file_pread(ctx->data, &msg->aligned_header, hdr_size, data_off_iter)) {
      SYS_FAIL(JLOG_ERR_IDX_READ);
    }
    msg->header = &msg->aligned_header;
    total_size += msg->header->mlen;
    data_off_iter += (hdr_size + msg->header->mlen);
  }
  if(data_off + (hdr_size * count) + total_size > ctx->data_file_size) {
#ifdef DEBUG
    fprintf(stderr, "read idx off end: %llu %llu\n", data_off, ctx->data_file_size);
#endif
    SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
  }
  data_off_iter = data_off;
  if (ctx->mess_data_size < total_size) {
    ctx->mess_data_size = total_size + 1;
    ctx->mess_data = realloc(ctx->mess_data, ctx->mess_data_size);
  }
  char *data_ptr = ctx->mess_data;
  for (i=0; i < count; i++) {
    msg = &m[i];
    if (!jlog_file_pread(ctx->data, data_ptr, msg->header->mlen, data_off_iter + hdr_size)) {
      SYS_FAIL(JLOG_ERR_IDX_READ);
    }
    msg->mess_len = msg->header->mlen;
    msg->mess = data_ptr;
    data_off_iter += (hdr_size + msg->header->mlen);
    data_ptr += msg->header->mlen;
  }

 finish:
  if(ctx->last_error == JLOG_ERR_SUCCESS) {
    return count;
  }
  return -1;
}

int jlog_ctx_bulk_read_messages(jlog_ctx *ctx, const jlog_id *id, const int count, jlog_message *m) {
  off_t index_len;
  u_int64_t data_off;
  int with_lock = 0;
  size_t hdr_size = 0;
  uint32_t *message_disk_len;
  int i;
  /* We don't want the style to change mid-read, so use whatever
   * the style is now */
  jlog_read_method_type read_method = ctx->read_method;

  if (count <= 0) {
    return 0;
  }

 once_more_with_lock:

  data_off = 0;

  ctx->last_error = JLOG_ERR_SUCCESS;
  if (ctx->context_mode != JLOG_READ)
    SYS_FAIL(JLOG_ERR_ILLEGAL_WRITE);
  if (id->marker < 1) {
    SYS_FAIL(JLOG_ERR_ILLEGAL_LOGID);
  }

  __jlog_open_reader(ctx, id->log);
  if(!ctx->data)
    SYS_FAIL(JLOG_ERR_FILE_OPEN);
  __jlog_open_indexer(ctx, id->log);
  if(!ctx->index)
    SYS_FAIL(JLOG_ERR_IDX_OPEN);

  if(with_lock) {
    if (!jlog_file_lock(ctx->index)) {
      with_lock = 0;
      SYS_FAIL(JLOG_ERR_LOCK);
    }
  }

  if ((index_len = jlog_file_size(ctx->index)) == -1)
    SYS_FAIL(JLOG_ERR_IDX_SEEK);
  if (index_len % sizeof(u_int64_t))
    SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
  if (id->marker * sizeof(u_int64_t) > index_len) {
    SYS_FAIL(JLOG_ERR_ILLEGAL_LOGID);
  }

  if (!jlog_file_pread(ctx->index, &data_off, sizeof(u_int64_t),
                       (id->marker - 1) * sizeof(u_int64_t)))
  {
    SYS_FAIL(JLOG_ERR_IDX_READ);
  }

  if (data_off == 0 && id->marker != 1) {
    if (id->marker * sizeof(u_int64_t) == index_len) {
      /* close tag; not a real offset */
      ctx->last_error = JLOG_ERR_CLOSE_LOGID;
      ctx->last_errno = 0;
      if(with_lock) jlog_file_unlock(ctx->index);
      return -1;
    } else {
      /* an offset of 0 in the middle of an index means curruption */
      SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
    }
  }

  if(__jlog_setup_reader(ctx, id->log, 0) != 0)
    SYS_FAIL(ctx->last_error);

  if (IS_COMPRESS_MAGIC(ctx)) {
    if (__jlog_ctx_bulk_read_messages_compressed(ctx, id, count, m, data_off, read_method) < 0) {
      SYS_FAIL(ctx->last_error);
    }
    goto finish;
  }

  switch(read_method) {
    case JLOG_READ_METHOD_PREAD:
      if (__jlog_ctx_bulk_pread_messages_uncompressed(ctx, id, count, m, data_off) < 0) {
        SYS_FAIL(ctx->last_error);
      }
      break;
    case JLOG_READ_METHOD_MMAP:
      for (i=0; i < count; i++) {
        jlog_message *msg = &m[i];
        message_disk_len = &msg->aligned_header.mlen;
        hdr_size = sizeof(jlog_message_header);

        if(data_off > ctx->mmap_len - hdr_size) {
#ifdef DEBUG
          fprintf(stderr, "read idx off end: %llu\n", data_off);
#endif
          SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
        }

        memcpy(&msg->aligned_header, ((u_int8_t *)ctx->mmap_base) + data_off,
               hdr_size);

        if(data_off + hdr_size + *message_disk_len > ctx->mmap_len) {
#ifdef DEBUG
          fprintf(stderr, "read idx off end: %llu %llu\n", data_off, ctx->mmap_len);
#endif
          SYS_FAIL(JLOG_ERR_IDX_CORRUPT);
        }

        msg->header = &msg->aligned_header;
        msg->mess_len = msg->header->mlen;
        msg->mess = (((u_int8_t *)ctx->mmap_base) + data_off + hdr_size);
        data_off += (msg->mess_len + hdr_size);
      }
      break;
    default:
      SYS_FAIL(JLOG_ERR_NOT_SUPPORTED);
      break;
  }
 finish:
  if(with_lock) jlog_file_unlock(ctx->index);
  if(ctx->last_error == JLOG_ERR_SUCCESS) return 0;
  if(!with_lock) {
    if (ctx->last_error == JLOG_ERR_IDX_CORRUPT) {
      if (jlog_file_lock(ctx->index)) {
        jlog_file_truncate(ctx->index, 0);
        jlog_file_unlock(ctx->index);
      }
    }
    ___jlog_resync_index(ctx, id->log, NULL, NULL);
    with_lock = 1;
#ifdef DEBUG
    fprintf(stderr, "read retrying with lock\n");
#endif
    goto once_more_with_lock;
  }
  return -1;
}
int jlog_ctx_read_interval(jlog_ctx *ctx, jlog_id *start, jlog_id *finish) {
  jlog_id chkpt;
  int count = 0;

  ctx->last_error = JLOG_ERR_SUCCESS;
  if(ctx->context_mode != JLOG_READ) {
    ctx->last_error = JLOG_ERR_ILLEGAL_WRITE;
    ctx->last_errno = EPERM;
    return -1;
  }

  __jlog_restore_metastore(ctx, 0, 1);
  if(jlog_get_checkpoint(ctx, ctx->subscriber_name, &chkpt))
    SYS_FAIL(JLOG_ERR_INVALID_SUBSCRIBER);
  if(__jlog_find_first_log_after(ctx, &chkpt, start, finish) != 0)
    goto finish; /* Leave whatever error was set in find_first_log_after */
  if(start->log != chkpt.log) start->marker = 0;
  else start->marker = chkpt.marker;
  if(start->log != chkpt.log) {
    /* We've advanced our checkpoint, let's not do this work again */
    if(__jlog_set_checkpoint(ctx, ctx->subscriber_name, start) != 0)
      SYS_FAIL(JLOG_ERR_CHECKPOINT);
  }
  /* Here 'start' is actually the checkpoint, so we must advance it one.
     However, that may not be possible, if there are no messages, so first
     make sure finish is bigger */
  count = finish->marker - start->marker;
  if(finish->marker > start->marker) start->marker++;

  /* If the count is less than zero, the checkpoint is off the end
   * of the file. When this happens, we need to set it to the end of
   * the file */
  if (count < 0) {
    fprintf(stderr, "need to repair checkpoint for %s - start (%08x:%08x) > finish (%08x:%08x)\n", ctx->path, 
        start->log, start->marker, finish->log, finish->marker);
    if(__jlog_set_checkpoint(ctx, ctx->subscriber_name, finish) != 0) {
      fprintf(stderr, "failed repairing checkpoint for %s\n", ctx->path);
      SYS_FAIL(JLOG_ERR_CHECKPOINT);
    }
    if(jlog_get_checkpoint(ctx, ctx->subscriber_name, &chkpt)) {
      /* Should never happen */
      SYS_FAIL(JLOG_ERR_INVALID_SUBSCRIBER);
    }
    fprintf(stderr, "repaired checkpoint for %s: %08x:%08x\n", ctx->path, chkpt.log, chkpt.marker);
    ctx->last_error = JLOG_ERR_SUCCESS;
    count = 0;
  }

  /* We need to munmap it, so that we can remap it with more data if needed */
  __jlog_teardown_reader(ctx);
 finish:
  if(ctx->last_error == JLOG_ERR_SUCCESS) return count;
  return -1;
}

int jlog_ctx_first_log_id(jlog_ctx *ctx, jlog_id *id) {
  DIR *d;
  struct dirent *de;
  ctx->last_error = JLOG_ERR_SUCCESS;
  u_int32_t log;
  int found = 0;

  id->log = 0xffffffff;
  id->marker = 0;
  d = opendir(ctx->path);
  if (!d) return -1;

  while ((de = readdir(d))) {
    int i;
    char *cp = de->d_name;
    if(strlen(cp) != 8) continue;
    log = 0;
    for(i=0;i<8;i++) {
      log <<= 4;
      if(cp[i] >= '0' && cp[i] <= '9') log |= (cp[i] - '0');
      else if(cp[i] >= 'a' && cp[i] <= 'f') log |= (cp[i] - 'a' + 0xa);
      else if(cp[i] >= 'A' && cp[i] <= 'F') log |= (cp[i] - 'A' + 0xa);
      else break;
    }
    if(i != 8) continue;
    found = 1;
    if(log < id->log) id->log = log;
  }
  if(!found) id->log = 0;
  closedir(d);
  return 0;
}

int jlog_ctx_last_storage_log(jlog_ctx *ctx, uint32_t *logid) {
  ctx->last_error = JLOG_ERR_SUCCESS;
  if(ctx->context_mode != JLOG_READ) {
    ctx->last_error = JLOG_ERR_ILLEGAL_WRITE;
    ctx->last_errno = EPERM;
    return -1;
  }
  if (__jlog_restore_metastore(ctx, 0, 1) != 0) return -1;
  *logid = ctx->meta->storage_log;
  return 0;
}

int jlog_ctx_last_log_id(jlog_ctx *ctx, jlog_id *id) {
  ctx->last_error = JLOG_ERR_SUCCESS;
  if(ctx->context_mode != JLOG_READ) {
    ctx->last_error = JLOG_ERR_ILLEGAL_WRITE;
    ctx->last_errno = EPERM;
    return -1;
  }
  if (__jlog_restore_metastore(ctx, 0, 1) != 0) return -1;
  ___jlog_resync_index(ctx, ctx->meta->storage_log, id, NULL);
  if(ctx->last_error == JLOG_ERR_SUCCESS) return 0;
  return -1;
}

int jlog_ctx_advance_id(jlog_ctx *ctx, jlog_id *cur, 
                        jlog_id *start, jlog_id *finish)
{
  int rv;
  if(memcmp(cur, finish, sizeof(jlog_id))) {
    start->marker++;
  } else {
    if((rv = __jlog_find_first_log_after(ctx, cur, start, finish)) != 0) {
      return rv;
    }
    if(cur->log != start->log) {
      start->marker = 1;
    }
    else start->marker = cur->marker;
  }
  return 0;
}

static int is_datafile(const char *f, u_int32_t *logid) {
  int i;
  u_int32_t l = 0;
  for(i=0; i<8; i++) {
    if((f[i] >= '0' && f[i] <= '9') ||
       (f[i] >= 'a' && f[i] <= 'f')) {
      l <<= 4;
      l |= (f[i] < 'a') ? (f[i] - '0') : (f[i] - 'a' + 10);
    }
    else
      return 0;
  }
  if(f[i] != '\0') return 0;
  if(logid) *logid = l;
  return 1;
}

int jlog_clean(const char *file) {
  int rv = -1;
  u_int32_t earliest = 0;
  jlog_ctx *log;
  DIR *dir;
  struct dirent *de;

  log = jlog_new(file);
  jlog_ctx_open_writer(log);
  dir = opendir(file);
  if(!dir) goto out;

  earliest = 0;
  if(jlog_pending_readers(log, 0, &earliest) < 0) goto out;

  rv = 0;
  while((de = readdir(dir)) != NULL) {
    u_int32_t logid;
    if(is_datafile(de->d_name, &logid) && logid < earliest) {
      char fullfile[MAXPATHLEN];
      char fullidx[MAXPATHLEN];

      memset(fullfile, 0, sizeof(fullfile));
      memset(fullidx, 0, sizeof(fullidx));
      snprintf(fullfile, sizeof(fullfile), "%s/%s", file, de->d_name);
      snprintf(fullidx, sizeof(fullidx), "%s/%s" INDEX_EXT, file, de->d_name);
      (void)unlink(fullfile);
      (void)unlink(fullidx); /* this may not exist; don't care */
      rv++;
    }
  }
  closedir(dir);
 out:
  jlog_ctx_close(log);
  return rv;
}

/* ------------------ jlog_ctx_repair() and friends ----------- */

/*
  This code attempts to repair problems with the metastore file and
  also a checkpoint file, within a jlog directory. The top level
  function takes an integer parameter and returns an integer result.
  If the argument is zero, then non-aggressive repairs
  are attempted. If the argument is non-zero, and if the
  non-aggressive repairs were not successful, then an aggressive
  repair approach is attempted. This consists of; (a) deleting
  all files in the log directory; (b) deleting the log directory
  itself.

  The reader will note that some of this functionality is addressed
  by other code within this file. An early decision was made not
  to reuse any of this code, but rather to attempt a solution from
  first principles. This is not due to a bad case of NIH, instead
  it is due to a desire to implement all and only the behaviors
  stated, without any (apparent) possibility of side effects.

  The reader will also notice that this code uses memory allocation
  for filenames and directory paths, rather than static variables of
  size MAXPATHLEN. This is also intentional. Having large local
  variables (like 4k in this case) can lead to unfortunate behavior
  on some systems. The compiler should do the right thing, but that
  does not mean that it will do the right thing.
*/

// find the earliest and latest hex files in the directory

static int findel(DIR *dir, unsigned int *earp, unsigned int *latp) {
  unsigned int maxx = 0;
  unsigned int minn = 0;
  unsigned int hexx = 0;
  struct dirent *ent;
  int havemaxx = 0;
  int haveminn = 0;
  int nent = 0;

  if ( dir == NULL )
    return 0;
  (void)rewinddir(dir);
  while ( (ent = readdir(dir)) != NULL ) {
    if ( ent->d_name[0] != '\0' ) {
      nent++;
      if ( strlen(ent->d_name) == 8 &&
           sscanf(ent->d_name, "%x", &hexx) == 1 ) {
        if ( havemaxx == 0 ) {
          havemaxx = 1;
          maxx = hexx;
        } else {
          if ( hexx > maxx )
            maxx = hexx;
        }
        if ( haveminn == 0 ) {
          haveminn = 1;
          minn = hexx;
        } else {
          if ( hexx < minn )
            minn = hexx;
        }
      }
    }
  }
  if ( (havemaxx == 1) && (latp != NULL) )
    *latp = maxx;
  if ( (haveminn == 1) && (earp != NULL) )
    *earp = minn;
  // a valid directory has at least . and .. entries
  return (nent >= 2);
}

static int __jlog_get_storage_bounds(jlog_ctx *ctx, unsigned int *earliest, unsigned *latest) {
  DIR *dir = NULL;
  dir = opendir(ctx->path);
  FASSERT(ctx, dir != NULL, "cannot open jlog directory");
  if ( dir == NULL ) {
    ctx->last_error = JLOG_ERR_NOTDIR;
    return 0;
  }
  int b0 = findel(dir, earliest, latest);
  (void)closedir(dir);
  return b0;
}

static int validate_metastore(const struct _jlog_meta_info *info, struct _jlog_meta_info *out) {
  int valid = 1;
  if(info->hdr_magic == DEFAULT_HDR_MAGIC || IS_COMPRESS_MAGIC_HDR(info->hdr_magic)) {
    if(out) out->hdr_magic = info->hdr_magic;
  }
  else {
    valid = 0;
  }
  if(info->unit_limit > 0) {
    if(out) out->unit_limit = info->unit_limit;
  }
  else {
    valid = 0;
  }
  if(info->safety == JLOG_UNSAFE ||
     info->safety == JLOG_ALMOST_SAFE ||
     info->safety == JLOG_SAFE) {
    if(out) out->safety = info->safety;
  }
  else {
    valid = 0;
  }
  return valid;
}

static int metastore_ok_p(jlog_ctx *ctx, char *ag, unsigned int lat, struct _jlog_meta_info *out) {
  struct _jlog_meta_info current;
  if(out) {
    /* setup the real defaults */
    out->storage_log = lat;
    out->unit_limit = 4*1024*1024;
    out->safety = 1;
    out->hdr_magic = DEFAULT_HDR_MAGIC;
  }
  int fd = open(ag, O_RDONLY);
  if ( fd < 0 ) return 0;
  if ( lseek(fd, 0, SEEK_END) != sizeof(current) ) {
    (void)close(fd);
    return 0;
  }
  (void)lseek(fd, 0, SEEK_SET);
  int rd = read(fd, &current, sizeof(current));
  (void)close(fd);
  fd = -1;
  if ( rd != sizeof(current) )
    return 0;

  /* validate */
  int valid = validate_metastore(&current, out);
  if(current.storage_log != lat) {
    // we don't need to set out->storage_log, as it was set at the outset of this function
    valid = 0;
  }
  return valid;
}

static int repair_metastore(jlog_ctx *ctx, const char *pth, unsigned int lat) {
  if ( pth == NULL ) pth = ctx->path;
  if ( pth == NULL || pth[0] == '\0' ) {
    FASSERT(ctx, 0, "invalid metastore path");
    return 0;
  }
  size_t leen = strlen(pth);
  if ( (leen == 0) || (leen > (MAXPATHLEN-12)) ) {
    FASSERT(ctx, 0, "invalid metastore path length");
    return 0;
  }
  size_t leen2 = leen + strlen("metastore") + 4; 
  char *ag = (char *)calloc(leen2, sizeof(char));
  if ( ag == NULL )             /* out of memory, so bail */
    return 0;
  (void)snprintf(ag, leen2-1, "%s%cmetastore", pth, IFS_CH);
  struct _jlog_meta_info out;
  int b = metastore_ok_p(ctx, ag, lat, &out);
  FASSERT(ctx, b, "metastore integrity check failed");
  if(b != 0) return 1;
  int fd = open(ag, O_RDWR|O_CREAT, DEFAULT_FILE_MODE);
  free((void *)ag);
  ag = NULL;
  FASSERT(ctx, fd >= 0, "cannot create new metastore file");
  if ( fd < 0 )
    return 0;
  if(ftruncate(fd, sizeof(out)) != 0) {
    FASSERT(ctx, 0, "ftruncate failed (non-fatal)");
  }
  int wr = write(fd, &out, sizeof(out));
  (void)close(fd);
  FASSERT(ctx, wr == sizeof(out), "cannot write new metastore file");
  return (wr == sizeof(out));
}

static int new_checkpoint(jlog_ctx *ctx, char *ag, int fd, jlog_id point) {
  int newfd = 0;
  int sta = 0;
  if ( ag == NULL || ag[0] == '\0' ) {
    FASSERT(ctx, 0, "invalid checkpoint path");
    return 0;
  }
  if ( fd < 0 ) {
    (void)unlink(ag);
    fd = creat(ag, DEFAULT_FILE_MODE);
    FASSERT(ctx, fd >= 0, "cannot create checkpoint file");
    if ( fd < 0 )
      return 0;
    else
      newfd = 1;
  }
  int x = ftruncate(fd, 0);
  FASSERT(ctx, x >= 0, "ftruncate failed to zero out checkpoint file");
  if ( x >= 0 ) {
    off_t xcvR = lseek(fd, 0, SEEK_SET);
    FASSERT(ctx, xcvR == 0, "cannot seek to beginning of checkpoint file");
    if ( xcvR == 0 ) {
      unsigned int goal[2];
      goal[0] = point.log;
      goal[1] = point.marker;
      int wr = write(fd, goal, sizeof(goal));
      FASSERT(ctx, wr == sizeof(goal), "cannot write checkpoint file");
      sta = (wr == sizeof(goal));
    }
  }
  if ( newfd == 1 )
    (void)close(fd);
  return sta;
}

static int repair_checkpointfile(jlog_ctx *ctx, const char *pth, unsigned int ear, unsigned int lat) {
  DIR *dir = opendir(pth);
  FASSERT(ctx, dir != NULL, "invalid directory");
  if ( dir == NULL )
    return 0;
  struct dirent *ent = NULL;
  char *ag = NULL;
  int   fd = -1;

  const size_t twoI = 2*sizeof(unsigned int);
  int rv = 0;
  while ( (ent = readdir(dir)) != NULL ) {
    int sta = 0;
    /* cp.7e -> cp.~.... */
    if ( strncmp(ent->d_name, "cp.", 3) != 0) continue;
    if ( strncmp(ent->d_name, "cp.7e", 5) == 0 ) continue;
    size_t leen = strlen(pth) + strlen(ent->d_name) + 5;
    FASSERT(ctx, leen < MAXPATHLEN, "invalid checkpoint path length");
    if ( leen >= MAXPATHLEN ) continue;
    ag = (char *)calloc(leen+1, sizeof(char));
    if ( ag == NULL ) continue;
    (void)snprintf(ag, leen-1, "%s%c%s", pth, IFS_CH, ent->d_name);
    int closed;
    jlog_id last;
    fd = open(ag, O_RDWR);
    sta = 0;
    FASSERT(ctx, fd >= 0, "cannot open checkpoint file %s", ent->d_name);
    if ( fd >= 0 ) {
      off_t oof = lseek(fd, 0, SEEK_END);
      (void)lseek(fd, 0, SEEK_SET);
      FASSERT(ctx, oof == (off_t)twoI, "checkpoint %s file size incorrect: %zu != %zu", ent->d_name, oof, twoI);
      if ( oof == (off_t)twoI ) {
        unsigned int have[2];
        int rd = read(fd, have, sizeof(have));
        FASSERT(ctx, rd == sizeof(have), "cannot read checkpoint file %s", ent->d_name);
        if ( rd == sizeof(have) ) {
          jlog_id expect, current;
          expect.log = current.log = have[0];
          expect.marker = current.marker = have[1];
          if(expect.log < ear) {
            FASSERT(ctx, 0, "checkpoint %s log too small %08x -> %08x", ent->d_name, expect.log, ear);
            expect.log = ear;
          }
          if(expect.log > lat) {
            FASSERT(ctx, 0, "checkpoint %s log too big %08x -> %08x", ent->d_name, expect.log, ear);
            expect.log = lat;
          }
          if(__jlog_resync_index(ctx, expect.log, &last, &closed)) {
            FASSERT(ctx, 0, "could not resync index for %08x", have[0]);
            last.log = expect.log;
            last.marker = 0;
          }
          if ( (last.log != current.log) || (last.marker < current.marker) ) {
            FASSERT(ctx, 0, "fixing checkpoint %s data %08x:%08x != %08x:%08x", ent->d_name,
                    current.log, current.marker, last.log, last.marker);
          } else {
            sta = 1;
            rv++;
          }
        }
      }
    }
    if ( sta == 0 ) {
      sta = new_checkpoint(ctx, ag, fd, last);
      FASSERT(ctx, sta, "cannot create new checkpoint file");
    }
    if ( fd >= 0 ) {
      (void)close(fd);
      fd = -1;
    }
    if ( ag != NULL ) {
      (void)free((void *)ag);
      ag = NULL;
    }
  }
  closedir(dir);
  return rv;
}

/*
  When doing a directory traveral using readdir(), it is not safe to
  perform a rename() or unlink() during the traversal. So we have to
  save these filenames for processing after the traversal is done.
*/

static int analyze_datafile(jlog_ctx *ctx, u_int32_t logid) {
  char idxfile[MAXPATHLEN];
  int rv = 0;

  if (jlog_inspect_datafile(ctx, logid, 0) > 0) {
    fprintf(stderr, "REPAIRING %s/%08x\n", ctx->path, logid);
    rv = jlog_repair_datafile(ctx, logid);
    STRSETDATAFILE(ctx, idxfile, logid);
    strcat(idxfile, INDEX_EXT);
    unlink(idxfile);
  }
  return rv;
}

static int repair_data_files(jlog_ctx *log) {
  if(log->context_mode == JLOG_READ) {
    FASSERT(log, 0, "repair_data_files: illegal call in JLOG_READ mode");
    log->last_error = JLOG_ERR_ILLEGAL_WRITE;
    log->last_errno = EPERM;
    return -1;
  }
  DIR *dir;
  struct dirent *de;
  dir = opendir(log->path);
  if(!dir) {
    log->last_error = JLOG_ERR_NOTDIR;
    log->last_errno = errno;
    return -1;
  }
  int rv = 0;
  while((de = readdir(dir)) != NULL) {
    u_int32_t logid;
    if(is_datafile(de->d_name, &logid)) {
      char fullfile[MAXPATHLEN];
      char fullidx[MAXPATHLEN];
      struct stat st;
      int readers;
      snprintf(fullfile, sizeof(fullfile), "%s/%s", log->path, de->d_name);
      snprintf(fullidx, sizeof(fullidx), "%s/%s" INDEX_EXT, log->path, de->d_name);
      if(stat(fullfile, &st) == 0) {
        readers = __jlog_pending_readers(log, logid);
        if(analyze_datafile(log, logid) < 0) {
          rv = -1;
        }
        if(readers == 0) {
          unlink(fullfile);
          unlink(fullidx);
        }
      }
    }
  }
  closedir(dir);
  return rv;
}

/* exported */
int jlog_ctx_repair(jlog_ctx *ctx, int aggressive) {
  // step 1: extract the directory path
  const char *pth;

  if ( ctx != NULL )
    pth = ctx->path;
  else
    pth = NULL; // fassertxgetpath();
  if ( pth == NULL || pth[0] == '\0' ) {
    FASSERT(ctx, 0, "repair command cannot find jlog path");
    ctx->last_error = JLOG_ERR_NOTDIR;
    return 0;               /* hopeless without a dir name */
  }
  // step 2: find the earliest and the latest files with hex names
  unsigned int ear = 0;
  unsigned int lat = 0;
  int b0 = __jlog_get_storage_bounds(ctx, &ear, &lat);
  FASSERT(ctx, b0, "cannot find hex files in jlog directory");
  if ( b0 == 1 ) {
    // step 3: attempt to repair the metastore. It might not need any
    // repair, in which case nothing will happen
    int b1 = repair_metastore(ctx, pth, lat);
    // step 4: attempt to repair the checkpoint file. It might not need
    // any repair, in which case nothing will happen
    int b2 = repair_checkpointfile(ctx, pth, ear, lat);

    // if aggressive repair is not authorized, fail
    if ( aggressive != 0 ) {
      if(repair_data_files(ctx) < 0) {
        return 1;
      }
    }

    if (b1 != 1) {
      ctx->last_error = JLOG_ERR_CREATE_META;
      return 0;
    }

    if (b2 != 1) {
      ctx->last_error = JLOG_ERR_CHECKPOINT;
      return 1;
    }
  }
  ctx->last_error = JLOG_ERR_SUCCESS;
  return 0;
}

/* -------------end of jlog_ctx_repair() and friends ----------- */

/* vim:se ts=2 sw=2 et: */
