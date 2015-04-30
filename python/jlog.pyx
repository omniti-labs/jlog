cimport cjlog
from cpython cimport PyString_FromStringAndSize
from cpython cimport PyString_FromString
from cpython cimport PyString_AsString

JLOG_BEGIN = cjlog.JLOG_BEGIN
JLOG_END = cjlog.JLOG_END

JLOG_UNSAFE = cjlog.JLOG_UNSAFE
JLOG_ALMOST_SAFE = cjlog.JLOG_ALMOST_SAFE
JLOG_SAFE = cjlog.JLOG_SAFE

JLOG_ERR_SUCCESS = cjlog.JLOG_ERR_SUCCESS
JLOG_ERR_ILLEGAL_INIT = cjlog.JLOG_ERR_ILLEGAL_INIT
JLOG_ERR_ILLEGAL_OPEN = cjlog.JLOG_ERR_ILLEGAL_OPEN
JLOG_ERR_OPEN = cjlog.JLOG_ERR_OPEN
JLOG_ERR_NOTDIR = cjlog.JLOG_ERR_NOTDIR
JLOG_ERR_CREATE_PATHLEN = cjlog.JLOG_ERR_CREATE_PATHLEN
JLOG_ERR_CREATE_EXISTS = cjlog.JLOG_ERR_CREATE_EXISTS
JLOG_ERR_CREATE_MKDIR = cjlog.JLOG_ERR_CREATE_MKDIR
JLOG_ERR_CREATE_META = cjlog.JLOG_ERR_CREATE_META
JLOG_ERR_LOCK = cjlog.JLOG_ERR_LOCK
JLOG_ERR_IDX_OPEN = cjlog.JLOG_ERR_IDX_OPEN
JLOG_ERR_IDX_SEEK = cjlog.JLOG_ERR_IDX_SEEK
JLOG_ERR_IDX_CORRUPT = cjlog.JLOG_ERR_IDX_CORRUPT
JLOG_ERR_IDX_WRITE = cjlog.JLOG_ERR_IDX_WRITE
JLOG_ERR_IDX_READ = cjlog.JLOG_ERR_IDX_READ
JLOG_ERR_FILE_OPEN = cjlog.JLOG_ERR_FILE_OPEN
JLOG_ERR_FILE_SEEK = cjlog.JLOG_ERR_FILE_SEEK
JLOG_ERR_FILE_CORRUPT = cjlog.JLOG_ERR_FILE_CORRUPT
JLOG_ERR_FILE_READ = cjlog.JLOG_ERR_FILE_READ
JLOG_ERR_FILE_WRITE = cjlog.JLOG_ERR_FILE_WRITE
JLOG_ERR_META_OPEN = cjlog.JLOG_ERR_META_OPEN
JLOG_ERR_ILLEGAL_WRITE = cjlog.JLOG_ERR_ILLEGAL_WRITE
JLOG_ERR_ILLEGAL_CHECKPOINT = cjlog.JLOG_ERR_ILLEGAL_CHECKPOINT
JLOG_ERR_INVALID_SUBSCRIBER = cjlog.JLOG_ERR_INVALID_SUBSCRIBER
JLOG_ERR_ILLEGAL_LOGID = cjlog.JLOG_ERR_ILLEGAL_LOGID
JLOG_ERR_SUBSCRIBER_EXISTS = cjlog.JLOG_ERR_SUBSCRIBER_EXISTS
JLOG_ERR_CHECKPOINT = cjlog.JLOG_ERR_CHECKPOINT
JLOG_ERR_NOT_SUPPORTED = cjlog.JLOG_ERR_NOT_SUPPORTED
JLOG_ERR_CLOSE_LOGID = cjlog.JLOG_ERR_CLOSE_LOGID

class Error(Exception):
  def __init__(self, message, reason = None):
    if not reason:
      self.message = message
    else:
      self.message = message + ": " + reason

  def __str__(self):
    return self.message


cdef class JLogReader:

  cdef object ctx_initialized
  cdef object path
  cdef object subscriber
  cdef int max_iter
  cdef cjlog.jlog_ctx *ctx
  cdef cjlog.jlog_id begin
  cdef cjlog.jlog_id end
  cdef int count


  # path - the directory path to the jlog directory
  # subscriber - the subscriber for the jlog
  # max_iter - limit the number of iterable reads. [default: unlimited]
  def __cinit__(self, path, subscriber, max_iter = -1):
    self.path = path
    self.subscriber = subscriber
    self.ctx = cjlog.jlog_new(path)
    self.max_iter = max_iter
    self.count = 0
    self.ctx_initialized = False

    if self.ctx is NULL:
      raise Error("jlog failed to initialize")
    else:
      self.ctx_initialized = True

    error = cjlog.jlog_ctx_open_reader(self.ctx, PyString_AsString(subscriber))
    if error:
      raise Error("jlog reader failed to open subscriber %s@%s" % \
          (subscriber, path), JLogReader.error_msg())


  def __dealloc__(self):
    if self.ctx_initialized:
      cjlog.jlog_ctx_close(self.ctx)
      self.ctx_initialized = False


  # python iterable interface.
  def __next__(self):
    cdef cjlog.jlog_message msg

    if not self.count:
      # check and see if there's anything to read since the last batch
      count = cjlog.jlog_ctx_read_interval(self.ctx, &self.begin, \
          &self.end)
      if self.max_iter > 0:
        # limit reads if max_iter smaller
        self.count = min(self.max_iter, count)
      else:
        self.count = count

    if self.count < 1:
      raise StopIteration()

    error = cjlog.jlog_ctx_read_message(self.ctx, &self.begin, &msg)
    if error:
      raise Error("jlog_ctx_read_message", JLogReader.error_msg())

    pystr = PyString_FromStringAndSize(<char *>msg.mess, msg.mess_len)

    self.count -= 1
    if not self.count:
      # since we may not be at the true end due to max_iter limit, save where
      # our last read was.
      cjlog.jlog_ctx_read_checkpoint(self.ctx, &self.begin)
    else:
      cjlog.JLOG_ID_ADVANCE(&self.begin)

    return pystr


  def __iter__(self):
    return self


  def __len__(self):
    # use our own temporary begin/end so as not to disturb the object state
    cdef cjlog.jlog_id begin
    cdef cjlog.jlog_id end
    count = cjlog.jlog_ctx_read_interval(self.ctx, &begin, &end)
    if self.max_iter > 0:
      # limit reported length to max_iter if smaller
      return min(self.max_iter, count)
    else:
      return count

  def error_msg(self):
    return PyString_FromString(cjlog.jlog_ctx_err_string(self.ctx))
