# for jlog_ctx_write_message
cdef extern struct timeval:
  long tv_sec
  long tv_usec

cdef extern from "jlog.h":

  ctypedef struct jlog_ctx:
    pass

  ctypedef struct jlog_message_header:
    int reserved
    int tv_sec
    int tv_usec
    int mlen

  ctypedef struct jlog_id:
    int log
    int marker

  void JLOG_ID_ADVANCE(jlog_id *id)

  ctypedef struct jlog_message:
    jlog_message_header *header
    int mess_len
    void *mess
    jlog_message_header aligned_header

  ctypedef enum jlog_position:
    JLOG_BEGIN,
    JLOG_END

  ctypedef enum jlog_safety:
    JLOG_UNSAFE,
    JLOG_ALMOST_SAFE,
    JLOG_SAFE

  ctypedef enum jlog_err:
    JLOG_ERR_SUCCESS = 0,
    JLOG_ERR_ILLEGAL_INIT,
    JLOG_ERR_ILLEGAL_OPEN,
    JLOG_ERR_OPEN,
    JLOG_ERR_NOTDIR,
    JLOG_ERR_CREATE_PATHLEN,
    JLOG_ERR_CREATE_EXISTS,
    JLOG_ERR_CREATE_MKDIR,
    JLOG_ERR_CREATE_META,
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
    JLOG_ERR_ILLEGAL_WRITE,
    JLOG_ERR_ILLEGAL_CHECKPOINT,
    JLOG_ERR_INVALID_SUBSCRIBER,
    JLOG_ERR_ILLEGAL_LOGID,
    JLOG_ERR_SUBSCRIBER_EXISTS,
    JLOG_ERR_CHECKPOINT,
    JLOG_ERR_NOT_SUPPORTED,
    JLOG_ERR_CLOSE_LOGID

  ctypedef void (*jlog_error_func) (void *ctx, char *msg, ...)

  jlog_ctx *jlog_new(char *path)
  void jlog_set_error_func(jlog_ctx *ctx, jlog_error_func Func, void *ptr)
  long jlog_raw_size(jlog_ctx *ctx)
  int jlog_ctx_init(jlog_ctx *ctx)
  int jlog_get_checkpoint(jlog_ctx *ctx, char *s, jlog_id *id)
  int jlog_ctx_list_subscribers_dispose(jlog_ctx *ctx, char **subs)
  int jlog_ctx_list_subscribers(jlog_ctx *ctx, char ***subs)

  int jlog_ctx_err(jlog_ctx *ctx)
  char *jlog_ctx_err_string(jlog_ctx *ctx)
  int jlog_ctx_errno(jlog_ctx *ctx)
  int jlog_ctx_open_writer(jlog_ctx *ctx)
  int jlog_ctx_open_reader(jlog_ctx *ctx, char *subscriber)
  int jlog_ctx_close(jlog_ctx *ctx)

  int jlog_ctx_alter_mode(jlog_ctx *ctx, int mode)
  int jlog_ctx_alter_journal_size(jlog_ctx *ctx, long size)
  int jlog_ctx_alter_safety(jlog_ctx *ctx, jlog_safety safety)
  int jlog_ctx_add_subscriber(jlog_ctx *ctx, char *subscriber, jlog_position whence)
  int jlog_ctx_remove_subscriber(jlog_ctx *ctx, char *subscriber)

  int jlog_ctx_write(jlog_ctx *ctx, void *message, long mess_len)
  int jlog_ctx_write_message(jlog_ctx *ctx, jlog_message *msg, timeval *when)
  int jlog_ctx_read_interval(jlog_ctx *ctx, jlog_id *first_mess, jlog_id *last_mess)
  int jlog_ctx_read_message(jlog_ctx *ctx, jlog_id *, jlog_message *)
  int jlog_ctx_read_checkpoint(jlog_ctx *ctx, jlog_id *checkpoint)
  int jlog_snprint_logid(char *buff, int n, jlog_id *checkpoint)

  int jlog_pending_readers(jlog_ctx *ctx, int log, int *earliest_ptr)
  int __jlog_pending_readers(jlog_ctx *ctx, int log)
  int jlog_ctx_first_log_id(jlog_ctx *ctx, jlog_id *id)
  int jlog_ctx_last_log_id(jlog_ctx *ctx, jlog_id *id)
  int jlog_ctx_advance_id(jlog_ctx *ctx, jlog_id *cur, jlog_id *start, jlog_id *finish)
  int jlog_clean(char *path)
