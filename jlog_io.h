#ifndef _JLOG_IO_H_
#define _JLOG_IO_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _jlog_file jlog_file;

/**
 * opens a jlog_file
 *
 * since a jlog_file is a shared handle potentially used by many threads,
 * the underlying open mode is always O_RDWR; only the O_CREAT and O_EXCL
 * flags are honored
 * @return pointer to jlog_file on success, NULL on failure
 * @internal
 */
jlog_file *jlog_file_open(const char *path, int flags, int mode);

/**
 * closes a jlog_file
 * @return 1 on success, 0 on failure
 * @internal
 */
int jlog_file_close(jlog_file *f);

/**
 * exclusively locks a jlog_file against other processes and threads
 * @return 1 on success, 0 on failure
 * @internal
 */
int jlog_file_lock(jlog_file *f);

/**
 * unlocks a jlog_file
 * @return 1 on success, 0 on failure
 * @internal
 */
int jlog_file_unlock(jlog_file *f);

/**
 * preads from a jlog_file, retries EINTR
 * @return 1 if the read was fully satisfied, 0 otherwise
 * @internal
 */
int jlog_file_pread(jlog_file *f, void *buf, size_t nbyte, off_t offset);

/**
 * pwrites to a jlog_file, retries EINTR
 * @return 1 if the write was fully satisfied, 0 otherwise
 * @internal
 */
int jlog_file_pwrite(jlog_file *f, const void *buf, size_t nbyte, off_t offset);

/**
 * calls fdatasync (if avalable) or fsync on the underlying filehandle
 * @return 1 on success, 0 on failure
 * @internal
 */
int jlog_file_sync(jlog_file *f);

/**
 * maps the entirety of a jlog_file into memory for reading
 * @param[out] map is set to the base of the mapped region
 * @param[out] len is set to the length of the mapped region
 * @return 1 on success, 0 on failure
 * @internal
 */
int jlog_file_map_read(jlog_file *f, void **base, size_t *len);

/**
 * gives the size of a jlog_file
 * @return size of file on success, -1 on failure
 * @internal
 */
off_t jlog_file_size(jlog_file *f);

/**
 * truncates a jlog_file, retries EINTR
 * @return 1 on success, 0 on failure
 * @internal
 */
int jlog_file_truncate(jlog_file *f, off_t len);

#ifdef __cplusplus
}  /* Close scope of 'extern "C"' declaration which encloses file. */
#endif

#endif /* _JLOG_IO_H_ */
/* vim:se ts=2 sw=2 et: */
