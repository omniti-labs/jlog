#include <stdio.h>
#include <stdlib.h>
#include <signal.h>

#include "jlog.h"

#define SUBSCRIBER "jlog-tail"

jlog_ctx* ctx;

void shutdown_handler(int signum) {
  (void) signum;

  jlog_ctx_remove_subscriber(ctx, SUBSCRIBER);
  jlog_ctx_close(ctx);
  exit(0);
}

int main(int argc, char** argv) {
  const char* path;
  jlog_id begin, end;
  int count;

  if(argc != 2) {
    fprintf(stderr, "usage: %s /path/to/jlog\n", argv[0]);
    exit(1);
  }
  path = argv[1];

  signal(SIGINT, shutdown_handler);
  signal(SIGHUP, shutdown_handler);
  signal(SIGTERM, shutdown_handler);
  signal(SIGQUIT, shutdown_handler);
  
  ctx = jlog_new(path);
  jlog_ctx_add_subscriber(ctx, SUBSCRIBER, JLOG_END);
  
  if (jlog_ctx_open_reader(ctx, SUBSCRIBER) != 0) {
    fprintf(stderr, "jlog_ctx_open_reader failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    exit(1);
  }

  while(1) {
    count = jlog_ctx_read_interval(ctx, &begin, &end);
    if (count > 0) {
      int i;
      jlog_message m;
    
      for (i = 0; i < count; i++, JLOG_ID_ADVANCE(&begin)) {
        end = begin;
      
        if (jlog_ctx_read_message(ctx, &begin, &m) == 0) {
          printf("%.*s\n", m.mess_len, (char*)m.mess);
          fflush(stdout);
        } else {
          fprintf(stderr, "jlog_ctx_read_message failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
        }
      }

      // checkpoint (commit) our read:
      jlog_ctx_read_checkpoint(ctx, &end);
    }
  }
}
