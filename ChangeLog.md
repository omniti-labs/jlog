# ChangeLog

## 2

### 2.6.0 (2024-05-28)

 * Add support for switch from `mmap` to `pread` for message reads due to
   bad interaction with `mmap` on certain filesystems.
 * Correct issue where bulk message reads did not work with compression
   enabled.
 * Fix issue where short writes could happen due to failing to check the
   return value of `pwritev`.

### 2.5.4 (2022-09-20)

 * No longer perform an unneeded ownership check on the parent directory. This
   silences an erroneous/misleading jlogctl error message.

### 2.5.3 (2020-07-20)

* Correct issue where writing to pre-commit buffer could cause jlog to
  advance with a zero-sized log segment.
* Make jlogctl alter -p <X> work.
* Make jlogctl repair maintain all pre-existing valid meta fields.
* `jlog_ctx_read_interval` now will automatically advance past more than one
  missing data files.

### 2.5.2 (2020-06-11)

* Add an ownership check after jlogctl runs to alert operators to potentially
  unusable file ownership problems.

### 2.5.1 (2020-06-08)

* Add a 'meta' subcommand to assist metastore inspection.
* make `jlog_ctx_repair` repair data files when run with `aggressive` != 0
* Offer `jlog_err_string`

### 2.5.0 (2020-04-22)

* Rework jlogctl to be subcommand-based while preserving old flags.
* Fix repair to correctly leave checkpoints intact.
* Add full repair (and metastore reconstruction) to jlogctl.

### 2.4.0 (2019-07-11)

* Improve speed of assessing the last log id.

### 2.3.2 (2018-08-07)

* Address message writes larger than a precommit.
* Fix memory leak when dealing with compressed records.

### 2.3.1.1 (2018-08-06)

* Avoid writes to metastore in read mode.

### 2.3.1 (2018-07-25)

* Handle interrupted syscalls when opening files.

### 2.3.0 (2017-05-19)

* Add `jlog_ctx_bulk_read_messages` API.

### 2.2.2.1 (2017-02-09)

* Avoid compiler warnings.
* Fix support on FreeBSD.

### 2.2.2 (2016-12-14)

* (no changes)

### 2.2.1.3 (2016-12-14)

* Fix read side indexer to not skip late writer writes into current log file

### 2.2.1.2 (2016-08-26)

* Fix deadlock in case where there is pre-commit resize and there is buffer to flush

### 2.2.1.1 (2016-08-22)

* Force `-D_REENTRANT`
* Allow changing of precommit size on open

### 2.2.1 (2016-05-19)

* Support precommit buffer for higher performance
* Use pwritev for lockless writes

### 2.2.0 (2016-05-05)

* Add `jlog_ctx_set_subscriber_checkpoint` API

### 2.1.3.2 (2016-03-22)

* JNI load fixes.

### 2.1.3.1 (2016-03-22)

* Better JNI file links.

### 2.1.3 (2016-03-21)

* Code cleanup
* Java true-up and install improvements.

### 2.1.2 (no release)

### 2.1.1 (2016-03-04)

* Correctly notice error in file opening.

### 2.1.0 (2016-03-04)

* Add `jlog_ctx_repair` API
* Java true-up

### 2.0.2 (2015-10-13)

* Automatically repair in read internal when checkpoint is too big.
* Several memory leaks fixed on FreeBSD.
* Update Python wrapper.
* Add `jlog_ctx_add_subscriber_copy_checkpoint` API.

### 2.0.1 (2015-03-23)

* Validate header magic in `jlog_repair_datafile`

### 2.0.0 (2015-02-19)

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

### 1.2.2 (2013-01-03)

* PHP support.
* Java JNI support.

### 1.2.1 (2012-11-15)

* Address locking issue on Illumos and Solaris.
* Fix handling of interrupted system calls.

### 1.2 (2011-08-31)

* Fix uninitialized variable and interrupted `fstat()` operation.

### 1.1 (2011-02-22)

* Optimize adding subscribers with `JLOG_BEGIN`.
* Code cleanup.

### 1.0 (2009-05-13)

* Initial release.
