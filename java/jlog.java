/*
 * Copyright (c) 2012, Circonus, Inc.
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
 *    * Neither the name Circonus, Inc. nor the names
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
package com.omniti.labs;

import java.util.Date;

public class jlog {
  public enum jlog_safety { JLOG_UNSAFE, JLOG_ALMOST_SAFE, JLOG_SAFE };
  public enum jlog_position { JLOG_BEGIN, JLOG_END };
  public enum jlog_err { JLOG_ERR_SUCCESS, JLOG_ERR_ILLEGAL_INIT,
         JLOG_ERR_ILLEGAL_OPEN, JLOG_ERR_OPEN, JLOG_ERR_NOTDIR,
         JLOG_ERR_CREATE_PATHLEN, JLOG_ERR_CREATE_EXISTS,
         JLOG_ERR_CREATE_MKDIR, JLOG_ERR_CREATE_META, JLOG_ERR_LOCK,
         JLOG_ERR_IDX_OPEN, JLOG_ERR_IDX_SEEK, JLOG_ERR_IDX_CORRUPT,
         JLOG_ERR_IDX_WRITE, JLOG_ERR_IDX_READ, JLOG_ERR_FILE_OPEN,
         JLOG_ERR_FILE_SEEK, JLOG_ERR_FILE_CORRUPT, JLOG_ERR_FILE_READ,
         JLOG_ERR_FILE_WRITE, JLOG_ERR_META_OPEN, JLOG_ERR_ILLEGAL_WRITE,
         JLOG_ERR_ILLEGAL_CHECKPOINT, JLOG_ERR_INVALID_SUBSCRIBER,
         JLOG_ERR_ILLEGAL_LOGID, JLOG_ERR_SUBSCRIBER_EXISTS,
         JLOG_ERR_CHECKPOINT, JLOG_ERR_NOT_SUPPORTED };

  public class jlogException extends Exception {
    public jlogException(String a) { super(a); }
  }
  public class jlogAlreadyOpenedException extends jlogException {
    public jlogAlreadyOpenedException(String a) { super(a); }
  }
  public class jlogNotWriterException extends jlogException {
    public jlogNotWriterException(String a) { super(a); }
  }
  public class jlogNotReaderException extends jlogException {
    public jlogNotReaderException(String a) { super(a); }
  }
  public class jlogSubscriberExistsException extends jlogException {
    public jlogSubscriberExistsException(String a) { super(a); }
  }
  public class jlogInvalidSubscriberException extends jlogException {
    public jlogInvalidSubscriberException(String a) { super(a); }
  }
  public class jlogIOException extends jlogException {
    public jlogIOException(String a) { super(a); }
  }

  public class Id {
    private long log;
    private long marker;
    public long getLog() { return log; }
    public long getMarker() { return marker; }
    public void increment() { marker++; }
    public boolean equals(Id o) {
      return (log == o.getLog() && marker == o.getMarker());
    }
    public int hashCode() { return (int)log ^ (int)marker; }
    public String toString() {
      return log + "/" + marker;
    }
  };
  public class Interval {
    private Id start;
    private Id finish;
    private int count;
    public Id getStart() { return start; }
    public Id getFinish() { return finish; }
    public int count() { return count; }
    public boolean equals(Interval o) {
      return (start.equals(o.getStart()) && finish.equals(o.getFinish()));
    }
    public int hashCode() { return start.hashCode() ^ finish.hashCode(); }
    public String toString() {
      return start + ":" + finish;
    }
  };
  public class Message {
    long when;
    byte[] data;
    Message(long ms, byte[] w) {
      when = ms; data = w;
    }
    public long getTime() { return when; }
    public Date getDate() { return new Date(when); }
    public byte[] getData() { return data; }
  };

  private long ctx;
  private String subscriber;
  private native void jlog_ctx_new(String path);
  private native void jlog_ctx_close();
  private native void jlog_ctx_init() throws jlogIOException;
  public native long raw_size();
  public native Id get_checkpoint(String subscriber)
    throws jlogIOException;
  public native String[] list_subscribers();
  public native jlog_err get_error();
  public native String get_error_string();
  public native int get_errno();
  public native void open_writer()
    throws jlogAlreadyOpenedException, jlogIOException;
  public native void open_reader(String sub)
    throws jlogAlreadyOpenedException, jlogIOException, jlogInvalidSubscriberException;
  public native void close();
  public native int repair(int aggressive);
  private native void alter_mode(int mode);
  private native void alter_journal_size(long size);
  private native void alter_safety(jlog_safety safety);
  public native void add_subscriber(String sub, jlog_position whence)
    throws jlogSubscriberExistsException, jlogIOException;
  public native void remove_subscriber(String sub)
    throws jlogInvalidSubscriberException, jlogIOException;
  public native void write(byte[] mess, long usec_epoch)
    throws jlogNotWriterException, jlogIOException;
  public native Interval read_interval()
    throws jlogNotReaderException, jlogIOException;
  public native Message read(Id what)
    throws jlogNotReaderException, jlogIOException;
  public native void read_checkpoint(Id what)
    throws jlogNotReaderException, jlogIOException;
  public native Id advance(Id what, Id start, Id finish)
    throws jlogNotReaderException, jlogIOException;
  public native Id first_log_id(Id id);
  public native Id last_log_id(Id id);


  public jlog(String path) {
    jlog_ctx_new(path);
  }
  protected void finalize() {
    close();
  }
  public void init() throws jlogIOException {
    jlog_ctx_init();
  }
  public void init(int mode, long size, jlog_safety safety) throws jlogIOException {
    alter_mode(mode);
    alter_journal_size(size);
    alter_safety(safety);
    jlog_ctx_init();
  }
  public Id get_checkpoint()
    throws jlogNotReaderException, jlogIOException {
    if(subscriber == null) throw new jlogNotReaderException("JLOG_ERR_ILLEGAL_OPEN");
    return get_checkpoint(subscriber);
  }
  public void write(byte[] mess, Date when) throws jlogNotWriterException, jlogIOException {
    write(mess, when.getTime());
  }
  public void write(String mess, Date when) throws jlogNotWriterException, jlogIOException {
    write(mess.getBytes(), when.getTime());
  }
  public void write(byte[] mess) throws jlogNotWriterException, jlogIOException {
    write(mess, (new Date()).getTime());
  }
  public void write(String mess) throws jlogNotWriterException, jlogIOException {
    write(mess.getBytes(), (new Date()).getTime());
  }
  public Id advance(Id what, Interval interval)
    throws jlogNotReaderException, jlogIOException {
    return advance(what, interval.getStart(), interval.getFinish());
  }

  static {
    try {
      System.loadLibrary("jlog");
   } catch (Exception e) {
     System.err.println("Cannot load jlog library: " + e);
     System.exit(-1);
   }
    try {
      System.loadLibrary("jnijlog");
   } catch (Exception e) {
     System.err.println("Cannot load jlog JNI library: " + e);
     System.exit(-1);
   }
  }

}
