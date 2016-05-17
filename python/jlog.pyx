cimport cjlog
from cpython cimport PyString_FromStringAndSize
from cpython cimport PyString_FromString
from cpython cimport PyString_AsString
from cpython cimport PyString_AsStringAndSize

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
JLOG_ERR_CREATE_PRE_COMMIT = cjlog.JLOG_ERR_CREATE_PRE_COMMIT
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
JLOG_ERR_PRE_COMMIT_OPEN = cjlog.JLOG_ERR_PRE_COMMIT_OPEN
JLOG_ERR_ILLEGAL_WRITE = cjlog.JLOG_ERR_ILLEGAL_WRITE
JLOG_ERR_ILLEGAL_CHECKPOINT = cjlog.JLOG_ERR_ILLEGAL_CHECKPOINT
JLOG_ERR_INVALID_SUBSCRIBER = cjlog.JLOG_ERR_INVALID_SUBSCRIBER
JLOG_ERR_ILLEGAL_LOGID = cjlog.JLOG_ERR_ILLEGAL_LOGID
JLOG_ERR_SUBSCRIBER_EXISTS = cjlog.JLOG_ERR_SUBSCRIBER_EXISTS
JLOG_ERR_CHECKPOINT = cjlog.JLOG_ERR_CHECKPOINT
JLOG_ERR_NOT_SUPPORTED = cjlog.JLOG_ERR_NOT_SUPPORTED
JLOG_ERR_CLOSE_LOGID = cjlog.JLOG_ERR_CLOSE_LOGID

class JLogError(Exception):
  def __init__(self, message, reason = None):
    if not reason:
      self.message = message
    else:
      self.message = message + ": " + reason

  def __str__(self):
    return self.message


cpdef jlog_add_subscriber(path, subscriber, position = JLOG_BEGIN):
  cdef cjlog.jlog_ctx *ctx
  ctx = cjlog.jlog_new(path)
  if ctx is NULL:
    return False
  error = cjlog.jlog_ctx_add_subscriber(ctx, subscriber, position)
  if error:
    return False
  else:
    return True


cpdef jlog_remove_subscriber(path, subscriber):
  cdef cjlog.jlog_ctx *ctx
  ctx = cjlog.jlog_new(path)
  if ctx is NULL:
    return False
  error = cjlog.jlog_ctx_remove_subscriber(ctx, subscriber)
  if error:
    return False
  else:
    return True


# Keep methods and variables that work for both readers and writers here
cdef class BaseJLog:
  cdef public object ctx_initialized
  cdef public object path
  cdef cjlog.jlog_ctx *ctx

  def add_subscriber(self, subscriber, position = JLOG_BEGIN):
    if not self.ctx_initialized:
      return False
    error = cjlog.jlog_ctx_add_subscriber(self.ctx, subscriber, position)
    if error:
      return False
    else:
      return True

  def remove_subscriber(self, subscriber):
    if not self.ctx_initialized:
      return False
    error = cjlog.jlog_ctx_remove_subscriber(self.ctx, subscriber)
    if error:
      return False
    else:
      return True


cdef class JLogReader(BaseJLog):
  cdef public object subscriber
  cdef cjlog.jlog_id begin
  cdef cjlog.jlog_id end
  cdef int count


  # path - the directory path to the jlog directory
  # subscriber - the subscriber for the jlog
  def __cinit__(self, path, subscriber):
    self.path = path
    self.subscriber = subscriber
    self.ctx = cjlog.jlog_new(path)
    self.count = 0
    self.ctx_initialized = False

    if self.ctx is NULL:
      raise JLogError("jlog failed to initialize")
    else:
      self.ctx_initialized = True

    error = cjlog.jlog_ctx_open_reader(self.ctx, subscriber)
    if error:
      raise JLogError("jlog reader failed to open subscriber %s@%s" % \
          (subscriber, path), JLogReader.error_msg())


  def __dealloc__(self):
    if self.ctx_initialized:
      cjlog.jlog_ctx_close(self.ctx)
      self.ctx_initialized = False


  # python iterable interface.
  def __next__(self):
    cdef cjlog.jlog_message msg

    # Stop iteration if we reached -1 (end of data) on the last
    # iteration. Set count to 0 so iteration can try again
    if self.count is -1:
      self.count = 0
      raise StopIteration()

    if not self.count:
      # check and see if there's anything to read since the last batch
      self.count = cjlog.jlog_ctx_read_interval(self.ctx, &self.begin, \
          &self.end)

    # nothing to read
    if self.count < 1:
      raise StopIteration()

    error = cjlog.jlog_ctx_read_message(self.ctx, &self.begin, &msg)
    if error:
      raise JLogError("jlog_ctx_read_message", JLogReader.error_msg())

    # checkpoint the read since we don't know when the caller will stop or
    # we could have an exception
    # XXX - make checkpoint happen on exceptions, dealloc. Calling it every
    #       read is slow
    cjlog.jlog_ctx_read_checkpoint(self.ctx, &self.begin)

    pystr = PyString_FromStringAndSize(<char *>msg.mess, msg.mess_len)

    self.count -= 1
    if not self.count:
      self.count = -1
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
    return count


  # truncate the subscriber to size entries or 0 if size is not specified. If
  # there are no other pending readers the files will be cleaned up.
  def truncate(self, size = None):
    if size is not None and size > 0:
      count = cjlog.jlog_ctx_read_interval(self.ctx, &self.begin, &self.end)
      if count > size:
        self.begin.log = self.end.log
        self.begin.marker = self.end.marker - size
      else:
        return False
    else:
      self.begin = self.end
      cjlog.JLOG_ID_ADVANCE(&self.begin)

    # checkpointing the file will cause all log segments without readers
    # to be cleaned up
    cjlog.jlog_ctx_read_checkpoint(self.ctx, &self.begin)
    return True


  def error_msg(self):
    return PyString_FromString(cjlog.jlog_ctx_err_string(self.ctx))


cdef class JLogWriter(BaseJLog):

  # path - the directory path to the jlog directory
  # subscriber - the subscriber for the jlog
  def __cinit__(self, path):
    self.path = path
    self.ctx = cjlog.jlog_new(path)
    self.ctx_initialized = False

    if self.ctx is NULL:
      JLogError("jlog writer failed to initialize")

    error = cjlog.jlog_ctx_init(self.ctx)
    if error and cjlog.jlog_ctx_err(self.ctx) is cjlog.JLOG_ERR_CREATE_EXISTS:
      raise JLogError("jlog writer failed to open %s" % path,
        self.error_msg())

    # jlog requires a clean handle to open even after an init
    error = cjlog.jlog_ctx_close(self.ctx)
    if error:
      JLogError("jlog writer failed reinit after creation")
    self.ctx = cjlog.jlog_new(path)
    self.ctx_initialized = True

    error = cjlog.jlog_ctx_open_writer(self.ctx)
    if error:
      raise JLogError("jlog writer failed to open %s" % path, self.error_msg())


  def write(self, msg_py):
    cdef char *msg
    cdef Py_ssize_t mlen
    PyString_AsStringAndSize(msg_py, &msg, &mlen)
    error = cjlog.jlog_ctx_write(self.ctx, msg, mlen)
    if error:
      raise JLogError("jlog write error", self.error_msg())


  def __dealloc__(self):
    if self.ctx_initialized:
      cjlog.jlog_ctx_close(self.ctx)
      self.ctx_initialized = False


  def error_msg(self):
    return PyString_FromString(cjlog.jlog_ctx_err_string(self.ctx))
