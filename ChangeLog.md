# ChangeLog

## 2

### 2.5.0

* Rework jlogctl to be subcommand-based while preserving old flags.
* Fix repair to correctly leave checkpoints intact.
* Add full repair (and metastore reconstruction) to jlotctl.

### 2.4.0

* Improve speed of assessing the last log id.

### 2.3.2

* Address message writes larger than a precommit.
* Fix memory leak when dealing with compressed records.

### 2.3.1.1

* Avoid writes to metastore in read mode.

### 2.3.1

* Handle interrupted syscalls when opening files.

### 2.3.0

* Add `jlog_ctx_bulk_read_messages` API.

### 2.2.2.1

* Avoid compiler warnings.
* Fix support on FreeBSD.

### 2.2.2

* (no changes)

### 2.2.1.3

* Fix read side indexer to not skip late writer writes into current log file

### 2.2.1.2

* Fix deadlock in case where there is pre-commit resize and there is buffer to flush

### 2.2.1.1

* Force `-D_REENTRANT`
* Allow changing of precommit size on open

### 2.2.1

* Support precommit buffer for higher performance
* Use pwritev for lockless writes

### 2.2.0

* Add `jlog_ctx_set_subscriber_checkpoint` API

### 2.1.3.2

* JNI load fixes.

### 2.1.3.1

* Better JNI file links.

### 2.1.3

* Code cleanup
* Java true-up and install improvements.

### 2.1.2 (no release)

### 2.1.1

* Correctly notice error in file opening.

### 2.1.0

* Add `jlog_ctx_repair` API
* Java true-up

### 2.0.2

* Automatically repair in read internal when checkpoint is too big.
* Several memory leaks fixed on FreeBSD.
* Update Python wrapper.
* Add `jlog_ctx_add_subscriber_copy_checkpoint` API.

### 2.0.1

* Validate header magic in `jlog_repair_datafile`

### 2.0.0

* Make a better effort of advancing past missing files.
* Add `jlog_clean` API.
* Make `jlog_pending_readers` more robust.
* Support header magic other than 0.
* Improve error messages.
* Use mmap for metastore.
* Darwin support.
* Add sleep backoff and simplify subscriber removal.
* Add `jlogtail`.
* Fix complications when using from C++.

## 1

### 1.2.2

* PHP support.
* Java JNI support.

### 1.2.1

* Address locking issue on Illumos and Solaris.
* Fix handling of interrupted system calls.

### 1.2

* Fix uninitialized variable and interrupted `fstat()` operation.

### 1.1

* Optimize adding subscribers with `JLOG_BEGIN`.
* Code cleanup.

### 1.0

* Initial release.
