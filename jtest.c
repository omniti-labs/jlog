#include "jlog.h"

#define SUBSCRIBER "voyeur"
#define LOGNAME    "/tmp/jtest.foo"
jlog_ctx *ctx;

void usage() {
  fprintf(stderr, "options:\n\tinit\n\tread <count>\n\twrite <len> <count>\n");
}
void jcreate() {
  ctx = jlog_new(LOGNAME);
  jlog_ctx_alter_journal_size(ctx, 1024);
  if(jlog_ctx_init(ctx) != 0) {
    fprintf(stderr, "jlog_ctx_init failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
  } else {
    jlog_ctx_add_subscriber(ctx, SUBSCRIBER, JLOG_BEGIN);
  }
  jlog_ctx_close(ctx);
}

void jopenw(char *foo, int count) {
  int i;
  ctx = jlog_new(LOGNAME);
  if(jlog_ctx_open_writer(ctx) != 0) {
    fprintf(stderr, "jlog_ctx_open_writer failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    exit(-1);
  }
  for(i=0; i<count; i++)
    if(jlog_ctx_write(ctx, foo, strlen(foo)) != 0)
      fprintf(stderr, "jlog_ctx_write_message failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));

  jlog_ctx_close(ctx);
}
void jopenr(char *s, int expect) {
  char begins[20], ends[20];
  jlog_id begin, end;
  int count;
  jlog_message message;

  ctx = jlog_new(LOGNAME);
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
int main(int argc, char **argv) {
  int i;
#if _WIN32
  mem_init();
#endif
  for(i=1; i<argc; i++) {
    if(!strcmp(argv[i], "init")) {
      jcreate();
      exit(0);
    } else if(!strcmp(argv[i], "write")) {
      int len;
      char *message;
      if(i+2 >= argc) { usage(); exit(-1); }
      i++;
      len = atoi(argv[i]);
      message = malloc(len+1);
      memset(message, 'X', len-1);
      message[len-1] = '\n';
      message[len] = '\0';
      i++;
      jopenw(message, atoi(argv[i]));
      exit(0);
    } else if(!strcmp(argv[i], "read")) {
      if(i+1 >= argc) { usage(); exit(-1); }
      i++;
      jopenr(SUBSCRIBER, atoi(argv[i]));
      exit(0);
    } else {
      fprintf(stderr, "command '%s' not understood\n", argv[i]);
      usage();
    }
  }
  return 0;
}
