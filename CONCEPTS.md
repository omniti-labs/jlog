# JLog Concepts

A JLog is durable message queue; records (messages) are written to a JLog and
will remain logged until subscribers have read past that message and issued a
checkpoint.

JLogs are implemented as a directory on disk that contains a handful of control
files and a number of segment files. The segment files contain the messages
that were written to the JLog; each segment file can contain multiple messages
and grow to be 4MB in size. If a new message written to the JLog would cause
this limit to be exceeded, a new segment file is created. Segment files are
deleted automatically when all subscribers have consumed the contents of that
segment file and issued a checkpoint.

## Subscribers

In order to manage data in the JLog, the JLog needs to track who is
subscribing. Each subscriber has a name; the name must be unique for each
consuming process, otherwise the behavior of JLog is undefined as two different
processes will conflict over their state.

It is recommended that the writer of a jlog have knowledge of the subscriber
list before it writes data to the jlog, so that segments are not pruned away
before a given subscriber starts to read--such a subscriber would effectively
be "late" to the party and miss out on the data.

## Using the C API

Here's a quick overview of how to use the C API for writing and reading:

### Writer

Here, the writer is appending data to the jlog, which we're storing at
/var/log/jlogexample. We have two subscribers, named "one" and "two":

    jlog_ctx *ctx;
    const char *path = "/var/log/jlogexample";
    int rv;
    
    // First, ensure that the jlog is created
    ctx = jlog_new(path);
    if (jlog_ctx_init(ctx) != 0) {
      if(jlog_ctx_err(ctx) != JLOG_ERR_CREATE_EXISTS) {
        fprintf(stderr, "jlog_ctx_init failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
        exit(1);
      }
      // Make sure it knows about our subscriber(s)
      jlog_ctx_add_subscriber(ctx, "one", JLOG_BEGIN);
      jlog_ctx_add_subscriber(ctx, "two", JLOG_BEGIN);
    }
    
    // Now re-open for writing
    jlog_ctx_close(ctx);
    ctx = jlog_new(path);
    if (jlog_ctx_open_writer(ctx) != 0) {
       fprintf(stderr, "jlog_ctx_open_writer failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
       exit(0);
    }
    
    // Send in some data
    rv = jlog_ctx_write(ctx, "hello\n", strlen("hello\n");
    if (rv != 0) {
      fprintf(stderr, "jlog_ctx_write_message failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
    }
    jlog_ctx_close(ctx);

### Reader

Using the reader for subscriber "one" looks like this:

    jlog_ctx *ctx;
    const char *path = "/var/log/jlogexample";
    int rv;
    jlog_id begin, end;
    int count;
    
    ctx = jlog_new(path);
    if (jlog_ctx_open_reader(ctx, "one") != 0) {
      fprintf(stderr, "jlog_ctx_open_reader failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
      exit(1);
    }
    
    count = jlog_ctx_read_interval(ctx, &begin, &end);
    if (count > 0) {
      int i;
      jlog_message m;
    
      for (i = 0; i < count; i++, JLOG_ID_ADVANCE(&begin)) {
         end = begin;
    
         if (jlog_ctx_read_message(ctx, &begin, &m) == 0) {
            printf("Got: %.*s\n", m.mess_len, (char*)m.mess);
         } else {
           fprintf(stderr, "jlog_ctx_read_message failed: %d %s\n", jlog_ctx_err(ctx), jlog_ctx_err_string(ctx));
         }
      }
    
      // checkpoint (commit) our read:
      jlog_ctx_read_checkpoint(ctx, &end);
    }
    jlog_ctx_close(ctx);
