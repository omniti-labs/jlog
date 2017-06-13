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

#include <stdio.h>
#include <getopt.h>
#include "jlog.h"
#include "jlog_compress.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#else
extern int close(int);
extern int write(int, void *, unsigned);
#endif

#ifdef HAVE_FCNTL_H
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#else
extern int open(const char *, int);
extern int creat(const char *, int);
#endif

#if HAVE_ERRNO_H
#include <errno.h>
#endif

#ifndef MIN
#define  MIN(x, y)               ((x) < (y) ? (x) : (y))
#endif

#ifndef DEFAULT_FILE_MODE
#define DEFAULT_FILE_MODE 0640
#endif

#if defined(linux) || defined(__linux) || defined(__linux__)
#include <time.h>
typedef long long unsigned int hrtime_t;
inline hrtime_t my_gethrtime() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return ((ts.tv_sec * 1000000000) + ts.tv_nsec);
}
#elif defined(__MACH__)
#include <mach/mach.h>
#include <mach/mach_time.h>

typedef uint64_t hrtime_t;
static int initialized = 0;
static mach_timebase_info_data_t    sTimebaseInfo;
inline hrtime_t my_gethrtime() {
  uint64_t t;
  if(!initialized) {
    if(sTimebaseInfo.denom == 0)
      (void) mach_timebase_info(&sTimebaseInfo);
  }
  t = mach_absolute_time();
  return t * sTimebaseInfo.numer / sTimebaseInfo.denom;
}
#elif defined(BSD)
#include <time.h>
#define NANOSEC	1000000000

typedef uint64_t hrtime_t;
inline hrtime_t my_gethrtime() {
  struct timespec ts;
  clock_gettime(CLOCK_UPTIME,&ts);
  return (((u_int64_t) ts.tv_sec) * NANOSEC + ts.tv_nsec);
}
#else // illumos/Solaris
inline hrtime_t my_gethrtime() {
  return gethrtime();
}
#endif

#define SUBSCRIBER "voyeur"
#define CHECKPOINT_SUBSCRIBER "voyeur-check"
#define LOGNAME    "/tmp/jtest.foo"
jlog_ctx *ctx;
static size_t default_pre_commit_size = 1024*128;

void usage() {
  fprintf(stderr,
          "options:\n"
          "\tinit [-p <path>] [-s <subscriber>] [-j <journalsize>]\n"
          "\tinit_compressed [-p <path>] [-s <subscriber>] [-j <journalsize>]\n"
          "\tread [-p <path>] [-n <count>] [-s <subscriber>]\n"
          "\tbulk_read [-p <path>] [-n <count>] [-s <subscriber>]\n"
          "\twrite [-p <path>] [-l <len>] [-n <count>]\n"
          "\trepair [-p <path>]\n"
          "\ttwo_checkpoints [-p <path>] [-n <count>] [-s <subscriber>]\n"
          "\tresize_pre_commit [-p <path>] [-l <new_size>]\n");
}

static void
print_rate(hrtime_t s, hrtime_t f, uint64_t cnt) {
  double d;
  if(cnt) {
    d = (double)cnt * 1000000000;
    d /= (double)(f-s);
    printf("output %0.2f msg/sec\n", d);
  }
}


void jcreate(const char *path, const char *subscriber, int compressed, int jsize) {
  ctx = jlog_new(path);
  jlog_ctx_set_use_compression(ctx, compressed);
  jlog_ctx_alter_journal_size(ctx, jsize);
  if(jlog_ctx_init(ctx) != 0) {
    fprintf(stderr, "jlog_ctx_init failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
  } else {
    jlog_ctx_add_subscriber(ctx, subscriber, JLOG_BEGIN);
  }
  jlog_ctx_close(ctx);
}

void jresize_pre_commit(const char *path, size_t pre_commit_size) {
  ctx = jlog_new(path);
  jlog_ctx_set_pre_commit_buffer_size(ctx, pre_commit_size);
  jlog_ctx_open_writer(ctx);

  jlog_ctx_close(ctx);
}


/*
  In an effort to test all functionality of the repair function, we add
  the follow to the jlog directory:
     file 00000001, arbitrary contents
     file 00000003, arbitrary contents
     file 0000010a, arbitrary contents
     file cp.7473,  arbitrary contents
  
  We also corrupt the contents of the file metastore

  The non-aggressive repair should clean this up nicely
*/

static const char *names[] = { "00000001", "00000003", "0000010a",
                               "cp.7473" } ;

// this must be at least 7*4 characters, 4=number of names above

                              /*1      2      3      4      5      */
                              /*12345671234567123456712345671234567*/
static const char streeng[] =  "Gloria!FahdahtIthinktheyvegotyournu";

static void addonefile(const char *logname, const char *nam, int idx) {
  size_t leen = strlen(logname) + strlen(nam) + 4;
  char *ag = (char *)calloc(leen, sizeof(char));
  if ( ag == NULL )
    return;
  (void)snprintf(ag, leen-1, "%s%c%s", logname, IFS_CH, nam);
  int fd = creat(ag, DEFAULT_FILE_MODE);
  if ( fd >= 0 ) {
    if(write(fd, &streeng[7*idx], 7) != 7) exit(1);
    (void)close(fd);
  }
  free((void *)ag);
}

static void corruptmetastore(const char *logname) {
  size_t leen = strlen(logname) + strlen("metastore") + 4;
  char *ag = (char *)calloc(leen, sizeof(char));
  if ( ag == NULL )
    return;
  (void)snprintf(ag, leen-1, "%s%cmetastore", logname, IFS_CH);
  int fd = open(ag, 02);
  if ( fd < 0 )
    return;
  if(write(fd, &streeng[0], 7) != 7) exit(1);
  (void)close(fd);
}

static void addsomefiles(const char *logname) {
  int i;
  for(i=0;i<(sizeof(names)/sizeof(char *));i++)
    addonefile(logname, names[i], i);
  corruptmetastore(logname);
}

void jrepair(const char *path) {
  ctx = jlog_new(path);
  if(jlog_ctx_init(ctx) != 0) {
    fprintf(stderr, "jlog_ctx_init failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
  } else {
    addsomefiles(path);
    int b = jlog_ctx_repair(ctx, 0);
    if ( b != 1 ) {
      (void)fprintf(stderr, "jlog_ctx_repair(0) failed: %d %s\n",
                    jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
      b = jlog_ctx_repair(ctx, 1);
      if ( b != 1 ) {
        (void)fprintf(stderr, "jlog_ctx_repair(1) failed: %d %s\n",
                      jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
      } else {
        (void)printf("Aggressive file repair succeeded\n");
      }
    } else {
      (void)printf("Non-aggressive file repair succeeded\n");
    }
  }
  jlog_ctx_close(ctx);
}

void jopenw(char *foo, int count, const char *path) {
  hrtime_t s, f;
  int i;

  s = f = my_gethrtime();

  ctx = jlog_new(path);

  jlog_ctx_set_multi_process(ctx, 0);
  if(jlog_ctx_open_writer(ctx) != 0) {
    fprintf(stderr, "jlog_ctx_open_writer failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    exit(-1);
  } 
  int cnt = 0;
  for(i=0; i<count; i++) {
    if(jlog_ctx_write(ctx, foo, strlen(foo)) != 0)
      fprintf(stderr, "jlog_ctx_write_message failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    
    cnt++;
    if (i % 1000 == 0) {
      f = my_gethrtime();
    }

    if(f-s > 1000000000) {
      print_rate(s, f, cnt);
      cnt = 0;
      s = f;
    }
  }
  
  jlog_ctx_close(ctx);
}

void jopenr(const char *s, int expect, const char *path) {
  char begins[20], ends[20];
  jlog_id begin, end;
  int count;
  jlog_message message;

  ctx = jlog_new(path);
  if(jlog_ctx_open_reader(ctx, s) != 0) {
    fprintf(stderr, "jlog_ctx_open_reader failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    exit(-1);
  }
  while(expect > 0) {
    if((count = jlog_ctx_read_interval(ctx, &begin, &end)) == -1) {
      fprintf(stderr, "jlog_ctx_read_interval failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
      exit(-1);
    }
    jlog_snprint_logid(begins, sizeof(begins), &begin);
    jlog_snprint_logid(ends, sizeof(ends), &end);
    /* printf("reader [%s]  (%s, %s] count: %d\n", s, begins, ends, count); */
    if(count > 0) {
      int i;
      count = MIN(count, expect);
      for(i=0; i<count; i++, JLOG_ID_ADVANCE(&begin)) {
        end = begin;
        if(jlog_ctx_read_message(ctx, &begin, &message) != 0) {
          fprintf(stderr, "read failed: %d\n", jlog_ctx_err(ctx));
        } else {
          expect--;
          jlog_snprint_logid(begins, sizeof(begins), &begin);
          fprintf(stderr, "[%7d] read: [%s]\n\t'%.*s'\n", expect, begins,
                  message.mess_len, (char *)message.mess);
        }
      }
      if(jlog_ctx_read_checkpoint(ctx, &end) != 0) {
        fprintf(stderr, "checkpoint failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
      } else {
        fprintf(stderr, "\tcheckpointed...\n");
      }
    }
  }
  jlog_ctx_close(ctx);
}

void jopenr_bulk_read(const char *s, int expect, const char *path) {
  char begins[20], ends[20];
  jlog_id begin, end;
  int count;
  jlog_message *messages;

  ctx = jlog_new(path);
  if(jlog_ctx_open_reader(ctx, s) != 0) {
    fprintf(stderr, "jlog_ctx_open_reader failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    exit(-1);
  }
  while(expect > 0) {
    int retry = 1;
  retry:
    if((count = jlog_ctx_read_interval(ctx, &begin, &end)) == -1) {
      fprintf(stderr, "jlog_ctx_read_interval failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
      exit(-1);
    }
    jlog_snprint_logid(begins, sizeof(begins), &begin);
    jlog_snprint_logid(ends, sizeof(ends), &end);
    if(count > 0) {
      int i;
      count = MIN(count, expect);
      messages = calloc(count, sizeof(jlog_message));
      if(jlog_ctx_bulk_read_messages(ctx, &begin, count, messages) != 0) {
        fprintf(stderr, "read failed: %d/%s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
        if(retry) {
          retry = 0;
          goto retry;
        }
        exit(-1);
      } else {
        for(i=0; i<count; i++, JLOG_ID_ADVANCE(&begin)) {
          expect--;
          jlog_message *message = &messages[i];
          jlog_snprint_logid(begins, sizeof(begins), &begin);
          fprintf(stderr, "[%7d] bulk_read: [%s] - %d\n\t'%.*s'\n", expect, begins,
                  message->mess_len, message->mess_len, (char *)message->mess);
          end = begin;
        }
      }
      if(jlog_ctx_read_checkpoint(ctx, &end) != 0) {
        fprintf(stderr, "checkpoint failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
      } else {
        fprintf(stderr, "\tcheckpointed...\n");
      }
    }
  }
  jlog_ctx_close(ctx);
}

void jopenr_two_checks(const char *sub, const char *check_sub, int expect, const char *path) {
  char begins[20], ends[20];
  jlog_id begin, end, checkpoint;
  int count, pass = 0, orig_expect = expect;
  jlog_message message;

  ctx = jlog_new(path);
  if(jlog_ctx_open_reader(ctx, sub) != 0) {
    fprintf(stderr, "jlog_ctx_open_reader failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    exit(-1);
  }

  /* add our special trailing check point subscriber */
  if (jlog_ctx_add_subscriber(ctx, check_sub, JLOG_BEGIN) != 0 && errno != EEXIST) {
    fprintf(stderr, "jlog_ctx_add_subscriber failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    exit(-1);
  }

  /* now move the checkpoint subscriber to where the real reader is */
  if (jlog_get_checkpoint(ctx, sub, &checkpoint) != 0) {
    fprintf(stderr, "jlog_get_checkpoint failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    exit(-1);
  }
  
  if (jlog_ctx_set_subscriber_checkpoint(ctx, check_sub, &checkpoint) != 0) {
    fprintf(stderr, "jlog_ctx_set_subscriber_checkpoint failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    exit(-1);
  }

 AGAIN:
  pass++;
  /* now we can read and eventually rewind to wherever checkpoint is */
  while(expect > 0) {
    if((count = jlog_ctx_read_interval(ctx, &begin, &end)) == -1) {
      fprintf(stderr, "jlog_ctx_read_interval failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
      exit(-1);
    }
    jlog_snprint_logid(begins, sizeof(begins), &begin);
    jlog_snprint_logid(ends, sizeof(ends), &end);
    /* printf("reader [%s]  (%s, %s] count: %d\n", s, begins, ends, count); */
    if(count > 0) {
      int i;
      count = MIN(count, expect);
      for(i=0; i<count; i++, JLOG_ID_ADVANCE(&begin)) {
        end = begin;
        if(jlog_ctx_read_message(ctx, &begin, &message) != 0) {
          fprintf(stderr, "read failed: %d\n", jlog_ctx_err(ctx));
        } else {
          expect--;
          jlog_snprint_logid(begins, sizeof(begins), &begin);
          fprintf(stderr, "[%7d] read: [%s]\n\t'%.*s'\n", expect, begins,
                  message.mess_len, (char *)message.mess);
        }
      }
      if(jlog_ctx_read_checkpoint(ctx, &end) != 0) {
        fprintf(stderr, "checkpoint failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
      } else {
        fprintf(stderr, "\tcheckpointed...\n");
      }
    }
  }

  /* move checkpoint to our original position */
  if (jlog_get_checkpoint(ctx, check_sub, &checkpoint) != 0) {
    fprintf(stderr, "jlog_get_checkpoint failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    exit(-1);
  }
      
  if (jlog_ctx_read_checkpoint(ctx, &checkpoint) != 0) {
    fprintf(stderr, "checkpoint failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
  } else {
    fprintf(stderr, "\trewound checkpoint...\n");
    expect = orig_expect;
  }

  if (pass < 2) {
    goto AGAIN;
  }

  fprintf(stderr, "\tpass 2 complete");
  jlog_ctx_close(ctx);
}


int main(int argc, char **argv) {
  int i, len = -1, count = -1;
  int jsize = 1024000;
  const char *path = LOGNAME;
  const char *subscriber = SUBSCRIBER;
  const char *command;
  if(argc < 2) {
    usage();
    exit(-1);
  }
  command = argv[1];
  while(-1 != (i = getopt(argc-1, argv+1, "p:n:l:s:j:"))) {
    switch(i) {
    case 'p': path = optarg; break;
    case 's': subscriber = optarg; break;
    case 'l': len = atoi(optarg); break;
    case 'n': count = atoi(optarg); break;
    case 'j': jsize = atoi(optarg); break;
    default: usage(); exit(-1);
    }
  }
#if _WIN32
  mem_init();
#endif
  if(!strcmp(command, "init") || !strcmp(command, "init_compressed")) {
    int compress = strcmp(command, "init_compressed") == 0;
    jcreate(path, subscriber, compress, jsize);
    exit(0);
  } else if(!strcmp(command, "write")) {
    char *message;
    if(len < 0) len = 100;
    if(count < 0) count = 1;
    message = malloc(len+1);
    memset(message, 'X', len-1);
    message[len-1] = '\n';
    message[len] = '\0';
    jopenw(message, count, path);
    exit(0);
  } else if(!strcmp(command, "read")) {
    if(count < 0) count = 1;
    jopenr(subscriber, count, path);
    exit(0);
  } else if(!strcmp(command, "bulk_read")) {
    if(count < 0) count = 1;
    jopenr_bulk_read(subscriber, count, path);
    exit(0);
  } else if(!strcmp(command, "repair")) {
    jrepair(path);
    exit(0);
  } else if (!strcmp(command, "two_checkpoints")) {
    if(count < 0) count = 1;
    jopenr_two_checks(subscriber, CHECKPOINT_SUBSCRIBER, count, path);
    exit(0);
  } else if (!strcmp(command, "resize_pre_commit")) {
    i++;
    size_t new_size = default_pre_commit_size;
    if(len >= 0) new_size = len;
    jresize_pre_commit(path, new_size);
    exit(0);
  }

  else {
    fprintf(stderr, "command '%s' not understood\n", command);
    usage();
  }
  return 0;
}
