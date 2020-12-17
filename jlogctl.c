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

#include "jlog_config.h"
#include "jlog_private.h"
#include "getopt_long.h"
#include <stdio.h>
#include <stdarg.h>
#if HAVE_UNISTD_H
#include <unistd.h>
#endif
#if HAVE_ERRNO_H
#include <errno.h>
#endif
#if HAVE_DIRENT_H
#include <dirent.h>
#endif

static int verbose = 0;
static int show_progress = 0;
static int show_subscribers = 0;
static int show_files = 0;
static int show_index_info = 0;
static int analyze_datafiles = 0;
static int repair_datafiles = 0;
static int do_jlog_repair = 0;
static int cleanup = 0;
static int quiet = 0;
static char *add_subscriber = NULL;
static char *remove_subscriber = NULL;
static char *update_subscriber = NULL;
static jlog_id new_checkpoint;
static int set_checkpoint_flag = 0;

static void usage(const char *progin) {
  const char *prog = strrchr(progin, '/');
  prog = prog ? prog+1 : progin;
  printf("Usage:\n");
  printf("\n=== Subscriber Management ===\n\n");
  printf("%s subscriber [-j <jlogpath>] [-v]\n", prog);
  printf("\t-a <sub> [-C <8x:8x>]\t\tAdd a subscriber (at checkpoint)\n");
  printf("\t-u <sub> -C <8x:8x>\t\tUpdate a subscriber to checkpoint\n");
  printf("\t-e <sub>\t\t\tErase a subscriber\n");
  printf("\t-l\t\t\t\tList subscribers (default behavior)\n");
  printf("\t-p <sub>\t\t\tShow perspective of subscriber\n");
  printf("\n=== Data Management ===\n\n");
  printf("%s clean [-j <jlogpath>] [-v]\tRemove unsubscribed data\n", prog);
  printf("\n");
  printf("%s data [-j <jlogpath>] [-v]\n", prog);
  printf("\t-i\t\t\t\tShow index information\n");
  printf("\t-d\t\t\t\tAnalyze datafiles\n");
  printf("\t-r\t\t\t\tAnalyze and repair datafiles\n");
  printf("\n=== Administrative ===\n\n");
  printf("%s create -j <jlogpath> [-v] [-s <segsize>] [-p <precommit>] [-c <on|off]\n", prog);
  printf("\n");
  printf("%s alter [-j <jlogpath>] [-v] [-s <segsize>] [-p <precommit>] [-c <on|off]\n", prog);
  printf("\n");
  printf("%s meta [-j <jlogpath>] [-c|-f|-l|-m|-p|-s]\n", prog);
  printf("\t-c\tshow compression setting\n");
  printf("\t-f\tshow safety setting\n");
  printf("\t-l\tshow current storage log\n");
  printf("\t-m\tshow header magic\n");
  printf("\t-p\tshow precommit size\n");
  printf("\t-s\tshow segment size\n");
  printf("\t\tommitting flag shows all with field names\n");
  printf("\n");
  printf("%s repair [-j <jlogpath>] [-v]\tRepair metadata\n", prog);
  printf("\t-f\t\t\t\talso analyze and repair datafiles\n");
  printf("\n");
  printf("%s help\t\t\t\tThis help\n",prog);
}
static void oldusage(const char *prog) {
  usage(prog);
  printf("\n\n");
  printf("Old Usage:\n%s <options> logpath1 [logpath2 [...]]\n",
         prog);
  printf("\t-a <sub>\t- Add <sub> as a log subscriber\n");
  printf("\t-e <sub>\t- Erase <sub> as a log subscriber\n");
  printf("\t-u <sub>\t- Update <sub> as a log subscriber\n");
  printf("\t-C <8x:8x>\t- Checkpoint for -a or -u\n");
  printf("\t-p <sub>\t- Show the perspective of the subscriber <sub>\n");
  printf("\t-R\t\t- Perform native log repair\n");
  printf("\t-l\t\t- List all log segments with sizes and readers\n");
  printf("\t-i\t\t- List index information\n");
  printf("\t-c\t\t- Clean all log segments with no pending readers\n");
  printf("\t-s\t\t- Show all subscribers\n");
  printf("\t-d\t\t- Analyze datafiles\n");
  printf("\t-r\t\t- Analyze datafiles and repair if needed\n");
  printf("\t-v\t\t- Verbose output\n");
  printf("\n  * WARNING: the -r and -R options cannot be used on jlogs that are "
         "open by another process\n");
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

static int ownership_check(const char *path) {
  DIR *dir;
  struct dirent *de;
  struct stat expect, st;

  if(stat(path, &expect) < 0) {
    fprintf(stderr, "could not stat '%s': %s\n", path, strerror(errno));
    return -1;
  }
  dir = opendir(path);
  if(!dir) {
    fprintf(stderr, "error opening '%s': %s\n", path, strerror(errno));
    return -1;
  }
  int rv = 0;
  while((de = readdir(dir)) != NULL) {
    /* No need to test our parent directory */
    if(!strcmp(de->d_name, "..")) {
      continue;
    }
    char fullfile[MAXPATHLEN];
    snprintf(fullfile, sizeof(fullfile), "%s/%s", path, de->d_name);
    if(stat(fullfile, &st) != 0) {
      fprintf(stderr, "failed to stat %s: %s\n", fullfile, strerror(errno));
      rv = -1;
    }
    else {
      if(expect.st_uid != st.st_uid ||
         expect.st_gid != st.st_gid) {
        fprintf(stderr, "%s owner/group [%d/%d] doesn't match jlog [%d/%d]\n",
                de->d_name, st.st_uid, st.st_gid, expect.st_uid, expect.st_gid);
        rv =-1;
      }
    }
  }
  if(rv != 0) {
    char fullpath[MAXPATHLEN];
    const char *tgtpath;
    tgtpath = realpath(path, fullpath);
    if(!tgtpath) tgtpath = path;
    fprintf(stderr, "\nA permissions mismatch can cause these to fail later\n");
    fprintf(stderr, "*IF* the jlog ownership is correct you can run (as root):\n");
    fprintf(stderr, "\n  chown -R %d:%d %s\n", expect.st_uid, expect.st_gid, tgtpath);
  }
  return rv;
}
static void analyze_datafile(jlog_ctx *ctx, u_int32_t logid) {
  char idxfile[MAXPATHLEN];

  if (jlog_inspect_datafile(ctx, logid, verbose) > 0) {
    fprintf(stderr, "One or more errors were found.\n");
    if(repair_datafiles) {
      jlog_repair_datafile(ctx, logid);
      fprintf(stderr,
              "Log file reconstructed, deleting the corresponding idx file.\n");
      STRSETDATAFILE(ctx, idxfile, logid);
      strcat(idxfile, INDEX_EXT);
      unlink(idxfile);
    }
  }
}
static void my_fprintf(void *ctx, const char *format, ...) {
  va_list arg;
  size_t len = strlen(format);
  char *possible_copy = (char *)format;
  if(len > 0 && format[len-1] != '\n') {
    possible_copy = malloc(len+2);
    memcpy(possible_copy, format, len);
    possible_copy[len] = '\n';
    possible_copy[len+1] = '\0';
  }
  va_start(arg, format);
  vfprintf((FILE *)ctx, possible_copy, arg);
  va_end(arg);
  if(possible_copy != format) free(possible_copy);
}
static void process_jlog(const char *file, const char *sub) {
  jlog_ctx *log;

  /* First pass to just make sure it isn't completely busted */
  log = jlog_new(file);
  if(verbose) jlog_set_error_func(log, (jlog_error_func)my_fprintf, stderr);
  if(do_jlog_repair) {
    jlog_ctx_repair(log, 0);
  }
  if(jlog_ctx_open_writer(log)) {
    fprintf(stderr, "Failed to open jlog at '%s'\n", file);
    if(jlog_ctx_err(log) == JLOG_ERR_META_OPEN) {
      fprintf(stderr, "perhaps the metastore is damaged or this directory isn't a jlog?\n");
    }
    exit(0);
  }
  jlog_ctx_close(log);

  /* For real now */
  log = jlog_new(file);
  if(verbose) jlog_set_error_func(log, (jlog_error_func)my_fprintf, stderr);

  if(add_subscriber) {
    if(jlog_ctx_add_subscriber(log, add_subscriber, JLOG_BEGIN)) {
      fprintf(stderr, "Could not add subscriber '%s': %s\n", add_subscriber,
              jlog_ctx_err_string(log));
    } else {
      if(set_checkpoint_flag) {
        if(jlog_ctx_read_checkpoint(log, &new_checkpoint)) {
          fprintf(stderr, "error setting checkpoint %s to %08x:%08x\n",
                  sub, new_checkpoint.log, new_checkpoint.marker);
        }
      }
      jlog_id start = { 0, 0};
      if(jlog_get_checkpoint(log, add_subscriber, &start)) {
        fprintf(stderr, "Error reading checkpoint\n");
      }
      if(!quiet) printf("Added subscriber '%s' -> %08x:%08x\n", add_subscriber, start.log, start.marker);
      return;
    }
  }
  if(update_subscriber) {
    if(jlog_ctx_open_reader(log, update_subscriber)) {
      fprintf(stderr, "error opening '%s': %s\n", file, jlog_ctx_err_string(log));
      return;
    }
    jlog_id start = { 0, 0}, changed = {0, 0};
    if(jlog_get_checkpoint(log, NULL, &start)) {
      fprintf(stderr, "Error reading checkpoint\n");
    }
    if(set_checkpoint_flag) {
      if(jlog_ctx_read_checkpoint(log, &new_checkpoint)) {
        fprintf(stderr, "error setting checkpoint %s to %08x:%08x\n",
                update_subscriber, new_checkpoint.log, new_checkpoint.marker);
        return;
      }
    }
    if(jlog_get_checkpoint(log, NULL, &changed)) {
      fprintf(stderr, "Error reading checkpoint\n");
    }
    if(!quiet) printf("Updated subscriber '%s' %08x:%08x -> %08x:%08x\n", update_subscriber,
                      start.log, start.marker, changed.log, changed.marker);
    return;
  }
  if(remove_subscriber) {
    if(jlog_ctx_remove_subscriber(log, remove_subscriber) <= 0) {
      fprintf(stderr, "Could not erase subscriber '%s': %s\n",
              remove_subscriber, jlog_ctx_err_string(log));
    } else {
      if(!quiet) printf("Erased subscriber '%s'\n", remove_subscriber);
    }
  }
  if(!sub) {
    if(jlog_ctx_open_writer(log)) {
      fprintf(stderr, "error opening '%s': %s\n", file, jlog_ctx_err_string(log));
      return;
    }
  } else {
    if(jlog_ctx_open_reader(log, sub)) {
      fprintf(stderr, "error opening '%s': %s\n", file, jlog_ctx_err_string(log));
      return;
    }
  }
  if(show_progress) {
    jlog_id id, id2, id3;
    char buff[20], buff2[20], buff3[20];
    jlog_get_checkpoint(log, sub, &id);
    if(jlog_ctx_last_log_id(log, &id3)) {
      fprintf(stderr, "jlog_error: %s\n", jlog_ctx_err_string(log));
      fprintf(stderr, "error callign jlog_ctx_last_log_id\n");
    }
    jlog_snprint_logid(buff, sizeof(buff), &id);
    jlog_snprint_logid(buff3, sizeof(buff3), &id3);
    if(!quiet) printf("--------------------\n");
    if(!quiet) printf("  Perspective of the '%s' subscriber\n", sub);
    if(!quiet) printf("    current checkpoint: %s\n", buff);
    if(!quiet) printf("Last write: %s\n", buff3);
    if(jlog_ctx_read_interval(log, &id, &id2) < 0) {
      fprintf(stderr, "jlog_error: %s\n", jlog_ctx_err_string(log));
    }
    jlog_snprint_logid(buff, sizeof(buff), &id);
    jlog_snprint_logid(buff2, sizeof(buff2), &id2);
    if(!quiet) printf("\t     next interval: [%s, %s]\n", buff, buff2);
    if(!quiet) printf("--------------------\n\n");
  }
  if(show_subscribers) {
    char **list;
    int i;
    jlog_ctx_list_subscribers(log, &list);
    for(i=0; list[i]; i++) {
      jlog_id id;
      char buff[20];
      jlog_get_checkpoint(log, list[i], &id);
      jlog_snprint_logid(buff, sizeof(buff), &id);
      if(!quiet) printf("\t%32s @ %s\n", list[i], buff);
    }
    jlog_ctx_list_subscribers_dispose(log, list);
  }
  if(show_files) {
    DIR *dir;
    struct dirent *de;
    dir = opendir(file);
    if(!dir) {
      fprintf(stderr, "error opening '%s'\n", file);
      return;
    }
    while((de = readdir(dir)) != NULL) {
      u_int32_t logid;
      if(is_datafile(de->d_name, &logid)) {
        char fullfile[MAXPATHLEN];
        char fullidx[MAXPATHLEN];
        struct stat st;
        int readers;
        snprintf(fullfile, sizeof(fullfile), "%s/%s", file, de->d_name);
        snprintf(fullidx, sizeof(fullidx), "%s/%s" INDEX_EXT, file, de->d_name);
        if(stat(fullfile, &st)) {
          if(!quiet) printf("\t%8s [error statting file: %s\n", de->d_name, strerror(errno));
        } else {
          readers = __jlog_pending_readers(log, logid);
          if(!quiet) printf("\t%8s [%9llu bytes] %d pending readers\n",
                            de->d_name, (unsigned long long)st.st_size, readers);
          if(show_index_info && !quiet) {
            struct stat sb;
            if (stat(fullidx, &sb)) {
              printf("\t\t idx: none\n");
            } else {
              u_int32_t marker;
              int closed;
              if (jlog_idx_details(log, logid, &marker, &closed)) {
                printf("\t\t idx: error\n");
              } else {
                printf("\t\t idx: %u messages (%08x), %s\n",
                       marker, marker, closed?"closed":"open");
              }
            }
          }
          if (analyze_datafiles) analyze_datafile(log, logid);
          if((readers == 0) && cleanup) {
            unlink(fullfile);
            unlink(fullidx);
          }
        }
      }
    }
    closedir(dir);
  }
  jlog_ctx_close(log);
}

static int
set_checkpoint(const char *h) {
  char *endptr;
  if(strlen(h) != 17 || h[8] != ':') return -1;
  new_checkpoint.log = strtoul(h, &endptr, 16);
  if(*endptr != ':') return -1;
  new_checkpoint.marker = strtoul(endptr+1, &endptr, 16);
  if(*endptr != '\0') return -1;
  return 0;
}
int main_subscriber(const char *prog, int argc, char **argv) {
  const char *jlog = ".";
  int option_index = 0;
  int c;
  const char *subscriber = NULL;
  int optcnt = 0;
  while((c = getopt_long(argc,argv,"j:a:e:u:C:p:lv",NULL,&option_index)) != EOF) {
    switch(c) {
      case 'j':
        jlog = optarg;
        break;
      case 'C':
        if(set_checkpoint(optarg) != 0) {
          fprintf(stderr, "Invalid argument to -C\n");
          exit(-1);
        }
        set_checkpoint_flag = 1;
        break;
      case 'a':
       add_subscriber = optarg;
       optcnt++;
       break;
      case 'e':
       remove_subscriber = optarg;
       optcnt++;
       break;
      case 'u':
       update_subscriber = optarg;
       optcnt++;
       break;
      case 'p':
       show_progress = 1;
       optcnt++;
       subscriber = optarg;
       break;
      case 'l':
       show_subscribers = 1;
       optcnt++;
       break;
      case 'v':
       verbose++;
       break;
      default:
       usage(prog);
       exit(-1);
    }
  }
  if(optind != argc) {
    fprintf(stderr, "extraneous arguments: %s\n", argv[optind]);
    usage(prog);
    exit(-1);
  }
  if(optcnt > 1 || (set_checkpoint_flag && !(add_subscriber || update_subscriber))) {
    fprintf(stderr, "invalid option combination\n");
    usage(prog);
    exit(-1);
  }
  if(optcnt == 0) show_subscribers = 1;
  process_jlog(jlog, subscriber);
  ownership_check(jlog);
  return 0;
}
int main_data(const char *prog, int argc, char **argv) {
  const char *jlog = ".";
  int option_index = 0;
  int c;
  show_files = 1;
  while((c = getopt_long(argc,argv,"j:irdv",NULL,&option_index)) != EOF) {
    switch(c) {
      case 'j':
        jlog = optarg;
        break;
      case 'v':
       verbose++;
       break;
      case 'i':
       show_index_info = 1;
       break;
      case 'r':
       analyze_datafiles = 1;
       repair_datafiles = 1;
       break;
      case 'd':
       analyze_datafiles = 1;
       break;
      default:
       usage(prog);
       exit(-1);
    }
  }
  if(optind != argc) {
    fprintf(stderr, "extraneous arguments: %s\n", argv[optind]);
    usage(prog);
    exit(-1);
  }
  process_jlog(jlog, NULL);
  ownership_check(jlog);
  return 0;
}
int main_clean(const char *prog, int argc, char **argv) {
  const char *jlog = ".";
  int option_index = 0;
  int c;
  while((c = getopt_long(argc,argv,"j:v",NULL,&option_index)) != EOF) {
    switch(c) {
      case 'j':
        jlog = optarg;
        break;
      case 'v':
       verbose++;
       break;
      default:
       usage(prog);
       exit(-1);
    }
  }
  show_files = 1;
  quiet = 1;
  cleanup = 1;
  if(optind != argc) {
    fprintf(stderr, "extraneous arguments: %s\n", argv[optind]);
    usage(prog);
    exit(-1);
  }
  process_jlog(jlog, NULL);
  ownership_check(jlog);
  return 0;
}
int main_repair(const char *prog, int argc, char **argv) {
  const char *jlog = ".";
  int option_index = 0;
  int c;
  while((c = getopt_long(argc,argv,"j:vf",NULL,&option_index)) != EOF) {
    switch(c) {
      case 'j':
        jlog = optarg;
        break;
      case 'v':
       verbose++;
       break;
      case 'f':
        show_files = 1;
        analyze_datafiles = 1;
        repair_datafiles = 1;
       break;
      default:
       usage(prog);
       exit(-1);
    }
  }
  if(optind != argc) {
    fprintf(stderr, "extraneous arguments: %s\n", argv[optind]);
    usage(prog);
    exit(-1);
  }
  do_jlog_repair = 1;
  process_jlog(jlog, NULL);
  ownership_check(jlog);
  return 0;
}
int main_meta(const char *prog, int argc, char **argv) {
  int c;
  int option_index = 0;
  const char *jlog = ".";
  enum {
    SHOW_ALL = 0,
    SHOW_COMPRESSION,
    SHOW_SAFETY,
    SHOW_PRECOMMIT,
    SHOW_SEGMENTSIZE,
    SHOW_STORAGELOG,
    SHOW_MAGIC
  } mode = SHOW_ALL;
  while((c = getopt_long(argc,argv,"cfj:lmpsv",NULL,&option_index)) != EOF) {
    switch(c) {
      case 'j':
        jlog = optarg;
        break;
      case 'v':
       verbose++;
       break;
#define DOOPT(c, v) case c: if(mode != SHOW_ALL) { usage(prog); exit(-1); } mode = v; break
      DOOPT('c', SHOW_COMPRESSION);
      DOOPT('f', SHOW_SAFETY);
      DOOPT('l', SHOW_STORAGELOG);
      DOOPT('m', SHOW_MAGIC);
      DOOPT('p', SHOW_PRECOMMIT);
      DOOPT('s', SHOW_SEGMENTSIZE);
      default:
        usage(prog);
        exit(-1);
    }
  }

  jlog_ctx *log = jlog_new(jlog);
  if(jlog_ctx_open_writer(log) != 0) {
    fprintf(stderr, "Failed to alter jlog '%s': %s\n", jlog, jlog_ctx_err_string(log));
    return -1;
  }

  if(optind != argc) {
    fprintf(stderr, "extraneous arguments: %s\n", argv[optind]);
    usage(prog);
    exit(-1);
  }

  switch(mode) {
    case SHOW_ALL:
      printf("magic          %08x\n", log->meta->hdr_magic);
      printf("storagelog     %08x\n", log->meta->storage_log);
      printf("compression    %s\n",
        log->meta->hdr_magic == DEFAULT_HDR_MAGIC_COMPRESSION ?
          "on" :
          (log->meta->hdr_magic == DEFAULT_HDR_MAGIC ? "off" : "unknown"));
      printf("safety         ");
      if(log->meta->safety == JLOG_UNSAFE)
        printf("unsafe\n");
      else if(log->meta->safety == JLOG_ALMOST_SAFE)
        printf("metasafe\n");
      else if(log->meta->safety == JLOG_SAFE)
        printf("safe\n");
      else
        printf("0x%08x\n", log->meta->safety);
      printf("precommit      %zu\n",
             log->pre_commit_buffer_len ?
               log->pre_commit_buffer_len - sizeof(*log->pre_commit_pointer) : 0);
      printf("segmentsize    %u\n", log->meta->unit_limit);
      break;
    case SHOW_STORAGELOG: printf("%08x\n", log->meta->storage_log); break;
    case SHOW_MAGIC: printf("%08x\n", log->meta->hdr_magic); break;
    case SHOW_SAFETY:
      if(log->meta->safety == JLOG_UNSAFE)
        printf("unsafe\n");
      else if(log->meta->safety == JLOG_ALMOST_SAFE)
        printf("metasafe\n");
      else if(log->meta->safety == JLOG_SAFE)
        printf("safe\n");
      else
        printf("0x%08x\n", log->meta->safety);
      break;
    case SHOW_SEGMENTSIZE: printf("%u\n", log->meta->unit_limit); break;
    case SHOW_PRECOMMIT: printf("%zu\n", log->pre_commit_buffer_len ? log->pre_commit_buffer_len - sizeof(*log->pre_commit_pointer) : 0); break;
    case SHOW_COMPRESSION:
      if(log->meta->hdr_magic == DEFAULT_HDR_MAGIC_COMPRESSION)
        printf("on\n");
      else if(log->meta->hdr_magic == DEFAULT_HDR_MAGIC)
        printf("off\n");
      else
        printf("unknown\n");
      break;
  }
  return 0;
}
int main_alter(const char *prog, int create, int argc, char **argv) {
  const char *jlog = create ? NULL : ".";
  int option_index = 0;
  int c;
  int segment_size = -1;
  int precommit_size = -1;
  int use_compression = -1;
  int optcnt = create;
  while((c = getopt_long(argc,argv,"c:s:p:j:vm",NULL,&option_index)) != EOF) {
    switch(c) {
      case 'j':
        jlog = optarg;
        break;
      case 'v':
       verbose++;
       break;
      case 's':
       segment_size = atoi(optarg);
       optcnt++;
       break;
      case 'p':
       precommit_size = atoi(optarg);
       optcnt++;
       break;
      case 'c':
       if(!strcmp(optarg, "on")) {
         use_compression = 1; 
         optcnt++;
         break;
       }
       else if(!strcmp(optarg, "off")) {
         use_compression = 0;
         optcnt++;
         break;
       }
       // Fallthru
      default:
       usage(prog);
       exit(-1);
    }
  }
  if(optind != argc) {
    fprintf(stderr, "extraneous arguments: %s\n", argv[optind]);
    usage(prog);
    exit(-1);
  }
  if(optcnt < 1 || jlog == NULL) {
    fprintf(stderr, "bad options\n");
    usage(prog);
    exit(-1);
  }
  jlog_ctx *log = jlog_new(jlog);
  if(create) {
    if(jlog_ctx_init(log) != 0) {
      fprintf(stderr, "Failed to initialize jlog '%s': %s\n", jlog, jlog_ctx_err_string(log));
      return -1;
    }
    jlog_ctx_close(log);
    log = jlog_new(jlog);
  }
  if(jlog_ctx_open_writer(log) != 0) {
    fprintf(stderr, "Failed to alter jlog '%s': %s\n", jlog, jlog_ctx_err_string(log));
    return -1;
  }
  if(segment_size >= 0) {
    jlog_ctx_alter_journal_size(log, segment_size);
  }
  if(use_compression >= 0) {
    jlog_ctx_set_use_compression(log, use_compression);
  }
  if(precommit_size >= 0) {
    jlog_ctx_flush_pre_commit_buffer(log);
  }
  jlog_ctx_close(log);

  if(precommit_size >= 0) {
    log = jlog_new(jlog);
    jlog_ctx_set_pre_commit_buffer_size(log, precommit_size);
    if(jlog_ctx_open_writer(log) != 0) {
      fprintf(stderr, "Failed to alter jlog '%s': %s\n", jlog, jlog_ctx_err_string(log));
      jlog_ctx_close(log);
      return -1;
    }
    jlog_ctx_close(log);
  }
  return 0;
}
int main(int argc, char **argv) {
  int i, c;
  int option_index = 0;
  char *subscriber = NULL;
  if(argc == 1) {
    usage(argv[0]);
    return -1;
  }
  if(argc > 1 && !strcmp(argv[1], "-h")) {
    usage(argv[0]);
    return 0;
  }
  if(argc > 1 && *argv[1] != '-') {
    if(!strcmp(argv[1], "subscriber")) {
      return main_subscriber(argv[0], argc-1, argv+1);
    }
    else if(!strcmp(argv[1], "clean")) {
      return main_clean(argv[0], argc-1, argv+1);
    }
    else if(!strcmp(argv[1], "data")) {
      return main_data(argv[0], argc-1, argv+1);
    }
    else if(!strcmp(argv[1], "create")) {
      return main_alter(argv[0], 1, argc-1, argv+1);
    }
    else if(!strcmp(argv[1], "alter")) {
      return main_alter(argv[0], 0, argc-1, argv+1);
    }
    else if(!strcmp(argv[1], "meta")) {
      return main_meta(argv[0], argc-1, argv+1);
    }
    else if(!strcmp(argv[1], "repair")) {
      return main_repair(argv[0], argc-1, argv+1);
    }
    else if(!strcmp(argv[1], "help")) {
      usage(argv[0]);
      return 0;
    }
    usage(argv[0]);
    return -1;
  }
  while((c = getopt_long(argc, argv, "a:e:u:C:dsilrcp:vR",
                         NULL, &option_index)) != EOF) {
    switch(c) {
     case 'C':
       if(set_checkpoint(optarg) != 0) {
         fprintf(stderr, "Invalid argument to -C\n");
         exit(-1);
       }
       set_checkpoint_flag = 1;
       break;
     case 'v':
      verbose = 1;
      break;
     case 'i':
      show_files = 1;
      show_index_info = 1;
      break;
     case 'R':
      do_jlog_repair = 1;
      break;
     case 'r':
      show_files = 1;
      analyze_datafiles = 1;
      repair_datafiles = 1;
      break;
     case 'd':
      show_files = 1;
      analyze_datafiles = 1;
      break;
     case 'a':
      add_subscriber = optarg;
      break;
     case 'e':
      remove_subscriber = optarg;
      break;
     case 'u':
      update_subscriber = optarg;
      break;
     case 'p':
      show_progress = 1;
      subscriber = optarg;
      break;
     case 's':
      show_subscribers = 1;
      break;
     case 'c':
      show_files = 1;
      quiet = 1;
      cleanup = 1;
      break;
     case 'l':
      show_files = 1;
      break;
     default:
      oldusage(argv[0]);
      exit(-1);
    }
  }
  if(optind == argc) {
    oldusage(argv[0]);
    exit(-1);
  }
  for(i=optind; i<argc; i++) {
    if(!quiet) printf("%s\n", argv[i]);
    process_jlog(argv[i], subscriber);
    ownership_check(argv[i]);
  }
  return 0;
}
