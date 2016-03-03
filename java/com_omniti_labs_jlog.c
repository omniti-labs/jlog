#include "com_omniti_labs_jlog.h"
#include "jlog.h"

#define THROW(jenv,tclass,msg) do { \
    jclass throwable_class = (*(jenv))->FindClass((jenv), (tclass)); \
    jmethodID constructor = (*(jenv))->GetMethodID((jenv), throwable_class, \
      "<init>", "(Lcom/omniti/labs/jlog;Ljava/lang/String;)V"); \
    jstring jmsg = (*(jenv))->NewStringUTF((jenv), msg); \
    jobject throwable = (*(jenv))->NewObject((jenv), throwable_class, \
      constructor, self, jmsg); \
    (*(jenv))->Throw((jenv), throwable); \
} while(0)

#define FETCH_CTX(jenv, self, ctx) do { \
  jclass jlogclass = (*(jenv))->GetObjectClass((jenv),(self)); \
  jfieldID ctxfieldid = (*(jenv))->GetFieldID((jenv),jlogclass,"ctx","J"); \
  jlong jctx = (*(jenv))->GetLongField((jenv),(self),ctxfieldid); \
  (ctx) = (jlog_ctx *)jctx; \
} while(0)

#define SET_CTX(jenv, self, ctx) do { \
  jclass jlogclass = (*(jenv))->GetObjectClass((jenv),(self)); \
  jfieldID ctxfieldid = (*(jenv))->GetFieldID((jenv),jlogclass,"ctx","J"); \
  jlong jctx = (long)(ctx); \
  (*(jenv))->SetLongField((jenv),(self),ctxfieldid,jctx); \
} while(0)

#define SET_SUBSCRIBER(jenv, self, sub) do { \
  jclass jlogclass = (*(jenv))->GetObjectClass((jenv),(self)); \
  jfieldID subfieldid = (*(jenv))->GetFieldID((jenv),jlogclass,"subscriber","Ljava/lang/String;"); \
  (*(jenv))->SetObjectField((jenv),(self),subfieldid,sub); \
} while(0)

static char *
jstring_to_cstring(JNIEnv *jenv, jstring jstr) {
  jboolean isCopy;
  jsize len;
  const char *jcstr;
  char *rv;
  if(jstr == NULL) return NULL;
  len = (*jenv)->GetStringUTFLength(jenv, jstr);
  rv = malloc(len+1);
  if(rv == NULL) return NULL;
  jcstr = (*jenv)->GetStringUTFChars(jenv, jstr, &isCopy);
  if(jcstr == NULL) return NULL;
  memcpy(rv, jcstr, len);
  rv[len] = '\0';
  (*jenv)->ReleaseStringUTFChars(jenv, jstr, jcstr);
  return rv;
}

static void jlogId_to_jlogid(JNIEnv *jenv, jobject jid, jlog_id *id) {
  jclass jlogid_class;
  jfieldID fid;
  jlogid_class = (*jenv)->GetObjectClass(jenv, jid);
  fid = (*jenv)->GetFieldID(jenv, jlogid_class, "log", "J");
  id->log = (*jenv)->GetLongField(jenv, jid, fid);
  fid = (*jenv)->GetFieldID(jenv, jlogid_class, "marker", "J");
  id->marker = (*jenv)->GetLongField(jenv, jid, fid);
}

static jobject make_jlog_Id(JNIEnv *jenv, jlog_id id) {
  jclass jlogid_class;
  jfieldID fid;
  jobject jid;
  jlogid_class = (*jenv)->FindClass(jenv, "com/omniti/labs/jlog$Id");
  jid = (*jenv)->AllocObject(jenv, jlogid_class);
  fid = (*jenv)->GetFieldID(jenv, jlogid_class, "log", "J");
  (*jenv)->SetLongField(jenv, jid, fid, id.log);
  fid = (*jenv)->GetFieldID(jenv, jlogid_class, "marker", "J");
  (*jenv)->SetLongField(jenv, jid, fid, id.marker);
  return jid;
}

static jobject make_jlog_Interval(JNIEnv *jenv, jlog_id id1, jlog_id id2, int c) {
  jclass jloginterval_class;
  jfieldID fid;
  jobject jid;
  jloginterval_class = (*jenv)->FindClass(jenv, "com/omniti/labs/jlog$Interval");
  jid = (*jenv)->AllocObject(jenv, jloginterval_class);
  fid = (*jenv)->GetFieldID(jenv, jloginterval_class, "start", "Lcom/omniti/labs/jlog$Id;");
  (*jenv)->SetObjectField(jenv, jid, fid, make_jlog_Id(jenv, id1));
  fid = (*jenv)->GetFieldID(jenv, jloginterval_class, "finish", "Lcom/omniti/labs/jlog$Id;");
  (*jenv)->SetObjectField(jenv, jid, fid, make_jlog_Id(jenv, id2));
  fid = (*jenv)->GetFieldID(jenv, jloginterval_class, "count", "I");
  (*jenv)->SetIntField(jenv, jid, fid, c);
  return jid;
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    jlog_ctx_new
 * Signature: ()V
 */
void Java_com_omniti_labs_jlog_jlog_1ctx_1new
  (JNIEnv *jenv, jobject self, jstring path) {
  jlog_ctx *ctx;
  char *cpath;

  cpath = jstring_to_cstring(jenv, path);
  if(!cpath) {
    (*jenv)->ThrowNew(jenv, (*jenv)->FindClass(jenv, "java/lang/NullPointerException"), "");
    return;
  }
  ctx = jlog_new(cpath);
  SET_CTX(jenv,self,ctx);
  free(cpath);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    jlog_ctx_close
 * Signature: ()V
 */
void Java_com_omniti_labs_jlog_jlog_1ctx_1close
  (JNIEnv *jenv, jobject self) {
  jlog_ctx *ctx;
  FETCH_CTX(jenv,self,ctx);
  if(ctx == NULL) return;
  jlog_ctx_close(ctx);
  ctx = NULL;
  SET_CTX(jenv,self,ctx);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    jlog_ctx_init
 * Signature: ()V
 */
void Java_com_omniti_labs_jlog_jlog_1ctx_1init
  (JNIEnv *jenv, jobject self) {
  jlog_ctx *ctx;
  FETCH_CTX(jenv,self,ctx);
  if(jlog_ctx_init(ctx) != 0) {
    THROW(jenv,"com/omniti/labs/jlog$jlogIOException",jlog_ctx_err_string(ctx));
  }
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    raw_size
 * Signature: ()J
 */
jlong Java_com_omniti_labs_jlog_raw_1size
  (JNIEnv *jenv, jobject self) {
  jlog_ctx *ctx;
  FETCH_CTX(jenv,self,ctx);
  return jlog_raw_size(ctx);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    get_checkpoint
 */
jobject Java_com_omniti_labs_jlog_get_1checkpoint
  (JNIEnv *jenv, jobject self, jstring sub) {
  jlog_ctx *ctx;
  jlog_id id;
  char *subscriber = jstring_to_cstring(jenv, sub);
  FETCH_CTX(jenv,self,ctx);
  if(jlog_get_checkpoint(ctx, subscriber, &id) != 0) {
    free(subscriber);
    THROW(jenv,"com/omniti/labs/jlog$jlogIOException",jlog_ctx_err_string(ctx));
    return NULL;
  }
  free(subscriber);
  return make_jlog_Id(jenv, id);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    list_subscribers
 * Signature: ()[Ljava/lang/String;
 */
jobjectArray Java_com_omniti_labs_jlog_list_1subscribers
  (JNIEnv *jenv, jobject self) {
  jobjectArray rv;
  jclass jstringclass;
  char **subs;
  int cnt, i;
  jlog_ctx *ctx;
  FETCH_CTX(jenv,self,ctx);

  cnt = jlog_ctx_list_subscribers(ctx, &subs);
  if(cnt < 0) {
    THROW(jenv, "com/omniti/labs/jlog$jlogIOException", "directory inaccessible");
    return NULL;
  }
  jstringclass = (*jenv)->FindClass(jenv, "Ljava/lang/String;");
  rv = (*jenv)->NewObjectArray(jenv, cnt, jstringclass, NULL);
  for(i=0; i<cnt; i++)
    (*jenv)->SetObjectArrayElement(jenv, rv, i, (*jenv)->NewStringUTF(jenv, subs[i]));
  jlog_ctx_list_subscribers_dispose(ctx, subs);
  return rv;
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    get_error
 * Signature: ()Lcom/omniti/labs/jlog/jlog_err;
 */
#define mapenum(ename) do { \
    jfieldID fid = (*jenv)->GetStaticFieldID(jenv, errenum, #ename, "Lcom/omniti/labs/jlog$jlog_err;"); \
    obj = (*jenv)->GetStaticObjectField(jenv, errenum, fid); \
} while(0)

jobject Java_com_omniti_labs_jlog_get_1error
  (JNIEnv *jenv, jobject self) {
  jlog_ctx *ctx;
  jlog_err err;
  jclass errenum;
  jobject obj = NULL;
  FETCH_CTX(jenv,self,ctx);
  errenum = (*jenv)->FindClass(jenv, "com/omniti/labs/jlog$jlog_err");
  err = jlog_ctx_err(ctx);
  switch(err) {
    case JLOG_ERR_SUCCESS: mapenum(JLOG_ERR_SUCCESS); break;
    case JLOG_ERR_ILLEGAL_INIT: mapenum(JLOG_ERR_ILLEGAL_INIT); break;
    case JLOG_ERR_ILLEGAL_OPEN: mapenum(JLOG_ERR_ILLEGAL_OPEN); break;
    case JLOG_ERR_OPEN: mapenum(JLOG_ERR_OPEN); break;
    case JLOG_ERR_NOTDIR: mapenum(JLOG_ERR_NOTDIR); break;
    case JLOG_ERR_CREATE_PATHLEN: mapenum(JLOG_ERR_CREATE_PATHLEN); break;
    case JLOG_ERR_CREATE_EXISTS: mapenum(JLOG_ERR_CREATE_EXISTS); break;
    case JLOG_ERR_CREATE_MKDIR: mapenum(JLOG_ERR_CREATE_MKDIR); break;
    case JLOG_ERR_CREATE_META: mapenum(JLOG_ERR_CREATE_META); break;
    case JLOG_ERR_LOCK: mapenum(JLOG_ERR_LOCK); break;
    case JLOG_ERR_IDX_OPEN: mapenum(JLOG_ERR_IDX_OPEN); break;
    case JLOG_ERR_IDX_SEEK: mapenum(JLOG_ERR_IDX_SEEK); break;
    case JLOG_ERR_IDX_CORRUPT: mapenum(JLOG_ERR_IDX_CORRUPT); break;
    case JLOG_ERR_IDX_WRITE: mapenum(JLOG_ERR_IDX_WRITE); break;
    case JLOG_ERR_IDX_READ: mapenum(JLOG_ERR_IDX_READ); break;
    case JLOG_ERR_FILE_OPEN: mapenum(JLOG_ERR_FILE_OPEN); break;
    case JLOG_ERR_FILE_SEEK: mapenum(JLOG_ERR_FILE_SEEK); break;
    case JLOG_ERR_FILE_CORRUPT: mapenum(JLOG_ERR_FILE_CORRUPT); break;
    case JLOG_ERR_FILE_READ: mapenum(JLOG_ERR_FILE_READ); break;
    case JLOG_ERR_FILE_WRITE: mapenum(JLOG_ERR_FILE_WRITE); break;
    case JLOG_ERR_META_OPEN: mapenum(JLOG_ERR_META_OPEN); break;
    case JLOG_ERR_ILLEGAL_WRITE: mapenum(JLOG_ERR_ILLEGAL_WRITE); break;
    case JLOG_ERR_ILLEGAL_CHECKPOINT: mapenum(JLOG_ERR_ILLEGAL_CHECKPOINT); break;
    case JLOG_ERR_INVALID_SUBSCRIBER: mapenum(JLOG_ERR_INVALID_SUBSCRIBER); break;
    case JLOG_ERR_ILLEGAL_LOGID: mapenum(JLOG_ERR_ILLEGAL_LOGID); break;
    case JLOG_ERR_CLOSE_LOGID: mapenum(JLOG_ERR_CLOSE_LOGID); break;
    case JLOG_ERR_SUBSCRIBER_EXISTS: mapenum(JLOG_ERR_SUBSCRIBER_EXISTS); break;
    case JLOG_ERR_CHECKPOINT: mapenum(JLOG_ERR_CHECKPOINT); break;
    case JLOG_ERR_NOT_SUPPORTED: mapenum(JLOG_ERR_NOT_SUPPORTED); break;
  }
  return obj;
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    get_error_string
 * Signature: ()Ljava/lang/String;
 */
jstring Java_com_omniti_labs_jlog_get_1error_1string
  (JNIEnv *jenv, jobject self) {
  jlog_ctx *ctx;
  FETCH_CTX(jenv,self,ctx);
  return (*jenv)->NewStringUTF(jenv, jlog_ctx_err_string(ctx));
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    get_errno
 * Signature: ()I
 */
jint Java_com_omniti_labs_jlog_get_1errno
  (JNIEnv *jenv, jobject self) {
  jlog_ctx *ctx;
  FETCH_CTX(jenv,self,ctx);
  return jlog_ctx_errno(ctx);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    open_writer
 * Signature: ()V
 */
void Java_com_omniti_labs_jlog_open_1writer
  (JNIEnv *jenv, jobject self) {
  jlog_ctx *ctx;
  FETCH_CTX(jenv,self,ctx);
  if(jlog_ctx_open_writer(ctx) != 0) {
    if(jlog_ctx_err(ctx) == JLOG_ERR_ILLEGAL_OPEN)
      THROW(jenv,"com/omniti/labs/jlog$jlogAlreadyOpenedException",jlog_ctx_err_string(ctx));
    else
      THROW(jenv,"com/omniti/labs/jlog$jlogIOException",jlog_ctx_err_string(ctx));
    return ;
  }
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    open_reader
 * Signature: (Ljava/lang/String;)V
 */
void Java_com_omniti_labs_jlog_open_1reader
  (JNIEnv *jenv, jobject self, jstring sub) {
  int rv;
  jlog_ctx *ctx;
  char *subscriber;
  subscriber = jstring_to_cstring(jenv,sub);
  FETCH_CTX(jenv,self,ctx);
  rv = jlog_ctx_open_reader(ctx, subscriber);
  free(subscriber);
  if(rv != 0) {
    if(jlog_ctx_err(ctx) == JLOG_ERR_ILLEGAL_OPEN)
      THROW(jenv,"com/omniti/labs/jlog$jlogAlreadyOpenedException",jlog_ctx_err_string(ctx));
    else if(jlog_ctx_err(ctx) == JLOG_ERR_INVALID_SUBSCRIBER)
      THROW(jenv,"com/omniti/labs/jlog$jlogInvalidSubscriberException",jlog_ctx_err_string(ctx));
    else
      THROW(jenv,"com/omniti/labs/jlog$jlogIOException",jlog_ctx_err_string(ctx));
    return;
  }
  SET_SUBSCRIBER(jenv,self,sub);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    close
 * Signature: ()V
 */
void Java_com_omniti_labs_jlog_close
  (JNIEnv *jenv, jobject self) {
  jlog_ctx *ctx;
  FETCH_CTX(jenv,self,ctx);
  jlog_ctx_close(ctx);
  ctx = NULL;
  SET_CTX(jenv,self,ctx);
  SET_SUBSCRIBER(jenv,self,NULL);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    repair
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_com_omniti_labs_jlog_repair
  (JNIEnv *jenv, jobject self, jint aggro) {
  jlog_ctx *ctx;
  FETCH_CTX(jenv,self,ctx);
  jint b = (jint)jlog_ctx_repair(ctx, (int)aggro);
  return b;
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    alter_mode
 * Signature: (I)V
 */
void Java_com_omniti_labs_jlog_alter_1mode
  (JNIEnv *jenv, jobject self, jint mode) {
  jlog_ctx *ctx;
  FETCH_CTX(jenv,self,ctx);
  jlog_ctx_alter_mode(ctx, mode);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    alter_journal_size
 * Signature: (J)V
 */
void Java_com_omniti_labs_jlog_alter_1journal_1size
  (JNIEnv *jenv, jobject self, jlong size) {
  jlog_ctx *ctx;
  FETCH_CTX(jenv,self,ctx);
  jlog_ctx_alter_journal_size(ctx, size);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    alter_safety
 * Signature: (Lcom/omniti/labs/jlog/jlog_safety;)V
 */
void Java_com_omniti_labs_jlog_alter_1safety
  (JNIEnv *jenv, jobject self, jobject jsafety) {
  jlog_ctx *ctx;
  jclass enumClass = (*jenv)->FindClass(jenv, "com/omniti/labs/jlog$jlog_safety");
  jmethodID getOrdinalMethod = (*jenv)->GetMethodID(jenv, enumClass, "ordinal", "()I");
  jint value = (*jenv)->CallIntMethod(jenv, jsafety, getOrdinalMethod);
  FETCH_CTX(jenv,self,ctx);
  jlog_ctx_alter_safety(ctx,value);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    add_subscriber
 * Signature: (Ljava/lang/String;Lcom/omniti/labs/jlog/jlog_position;)V
 */
void Java_com_omniti_labs_jlog_add_1subscriber
  (JNIEnv *jenv, jobject self, jstring sub, jobject jwhence) {
  int rv;
  jlog_ctx *ctx;
  jclass enumClass = (*jenv)->FindClass(jenv, "com/omniti/labs/jlog$jlog_position");
  jmethodID getOrdinalMethod = (*jenv)->GetMethodID(jenv, enumClass, "ordinal", "()I");
  jint value = (*jenv)->CallIntMethod(jenv, jwhence, getOrdinalMethod);
  char *subscriber = jstring_to_cstring(jenv, sub);
  FETCH_CTX(jenv,self,ctx);
  rv = jlog_ctx_add_subscriber(ctx, subscriber, value);
  free(subscriber);
  if(rv < 0) {
    if(jlog_ctx_err(ctx) == JLOG_ERR_SUBSCRIBER_EXISTS)
      THROW(jenv, "com/omniti/labs/jlog$jlogSubscriberExistsException", "subscriber exists");
    else
      THROW(jenv,"com/omniti/labs/jlog$jlogIOException",jlog_ctx_err_string(ctx));
    return;
  }
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    remove_subscriber
 * Signature: (Ljava/lang/String;)V
 */
void Java_com_omniti_labs_jlog_remove_1subscriber
  (JNIEnv *jenv, jobject self, jstring sub) {
  int rv;
  jlog_ctx *ctx;
  char *subscriber = jstring_to_cstring(jenv, sub);
  FETCH_CTX(jenv,self,ctx);
  rv = jlog_ctx_remove_subscriber(ctx, subscriber);
  free(subscriber);
  if(rv != 0) {
    if(jlog_ctx_err(ctx) == JLOG_ERR_INVALID_SUBSCRIBER)
      THROW(jenv, "com/omniti/labs/jlog$jlogInvalidSubscriberException", "invalid subscriber");
    else
      THROW(jenv,"com/omniti/labs/jlog$jlogIOException",jlog_ctx_err_string(ctx));
    return;
  }
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    write
 * Signature: ([BJ)V
 */
void Java_com_omniti_labs_jlog_write
  (JNIEnv *jenv, jobject self, jbyteArray data, jlong whencems) {
  jlog_ctx *ctx;
  int rv;
  jbyte *rawdata;
  jsize len;
  jboolean isCopy;
  jlog_message mess;
  struct timeval whence;
  FETCH_CTX(jenv,self,ctx);

  rawdata = (*jenv)->GetByteArrayElements(jenv, data, &isCopy);
  len = (*jenv)->GetArrayLength(jenv, (jarray)data);
  mess.mess = rawdata;
  mess.mess_len = len;
  whence.tv_sec = whencems / 1000;
  whence.tv_usec = (whencems % 1000) * 1000;
  rv = jlog_ctx_write_message(ctx, &mess, &whence);
  (*jenv)->ReleaseByteArrayElements(jenv, data, rawdata, JNI_ABORT);
  if(rv != 0) {
    if(jlog_ctx_err(ctx) == JLOG_ERR_ILLEGAL_WRITE)
      THROW(jenv,"com/omniti/labs/jlog$jlogNotWriterException",jlog_ctx_err_string(ctx));
    else
      THROW(jenv,"com/omniti/labs/jlog$jlogIOException",jlog_ctx_err_string(ctx));
    return;
  }
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    read_interval
 * Signature: ()Lcom/omniti/labs/jlog/Interval;
 */
jobject Java_com_omniti_labs_jlog_read_1interval
  (JNIEnv *jenv, jobject self) {
  int rv;
  jlog_ctx *ctx;
  jlog_id start, finish;
  FETCH_CTX(jenv,self,ctx);
  if((rv = jlog_ctx_read_interval(ctx, &start, &finish)) < 0) {
    switch(jlog_ctx_err(ctx)) {
     case JLOG_ERR_SUCCESS:
      return NULL;
     case JLOG_ERR_ILLEGAL_WRITE:
      THROW(jenv,"com/omniti/labs/jlog$jlogNotReaderException",jlog_ctx_err_string(ctx));
      return NULL;
     default:
      THROW(jenv,"com/omniti/labs/jlog$jlogIOException",jlog_ctx_err_string(ctx));
      return NULL;
    }
  }
  return make_jlog_Interval(jenv, start, finish, rv);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    read
 * Signature: (Lcom/omniti/labs/jlog/Id;)Lcom/omniti/labs/jlog/Message;
 */
jobject Java_com_omniti_labs_jlog_read
  (JNIEnv *jenv, jobject self, jobject jid) {
  jlog_id id;
  jlog_message m;
  jobject jmess;
  jclass jmess_class;
  jlong jwhencems;
  jbyteArray jdata;
  jmethodID jmess_constructor;
  jlog_ctx *ctx;

  FETCH_CTX(jenv,self,ctx);
  jlogId_to_jlogid(jenv, jid, &id);
  if(jlog_ctx_read_message(ctx, &id, &m) != 0) {
    if(jlog_ctx_err(ctx) == JLOG_ERR_ILLEGAL_WRITE)
      THROW(jenv,"com/omniti/labs/jlog$jlogNotReaderException",jlog_ctx_err_string(ctx));
    else
      THROW(jenv,"com/omniti/labs/jlog$jlogIOException",jlog_ctx_err_string(ctx));
    return NULL;
  }
  jwhencems = m.header->tv_sec;
  jwhencems *= 1000;
  jwhencems += ((m.header->tv_usec % 1000000) / 1000);
  jdata = (*jenv)->NewByteArray(jenv, m.mess_len);
  if(jdata == NULL) {
    (*jenv)->ThrowNew(jenv, (*jenv)->FindClass(jenv, "java/lang/OutOfMemoryError"), "jlog message would exhaust memory");
    return NULL;
  }
  (*jenv)->SetByteArrayRegion(jenv, jdata, 0, m.mess_len, (jbyte *)m.mess);
  jmess_class = (*jenv)->FindClass(jenv, "com/omniti/labs/jlog$Message");
  jmess_constructor = (*jenv)->GetMethodID(jenv, jmess_class, "<init>", "(Lcom/omniti/labs/jlog;J[B)V");
  jmess = (*jenv)->NewObject(jenv, jmess_class, jmess_constructor, self, jwhencems, jdata);
  return jmess;
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    read_checkpoint
 * Signature: (Lcom/omniti/labs/jlog/Id;)V
 */
void Java_com_omniti_labs_jlog_read_1checkpoint
  (JNIEnv *jenv, jobject self, jobject jid) {
  jlog_ctx *ctx;
  jlog_id id;

  FETCH_CTX(jenv,self,ctx);
  jlogId_to_jlogid(jenv, jid, &id);
  if(jlog_ctx_read_checkpoint(ctx, &id) != 0) {
    if(jlog_ctx_err(ctx) == JLOG_ERR_ILLEGAL_CHECKPOINT)
      THROW(jenv,"com/omniti/labs/jlog$jlogNotReaderException",jlog_ctx_err_string(ctx));
    else
      THROW(jenv,"com/omniti/labs/jlog$jlogIOException",jlog_ctx_err_string(ctx));
    return;
  }
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    advance
 * Signature: (Lcom/omniti/labs/jlog/Id;Lcom/omniti/labs/jlog/Id;Lcom/omniti/labs/jlog/Id;)Lcom/omniti/labs/jlog/Id;
 */
jobject Java_com_omniti_labs_jlog_advance
  (JNIEnv *jenv, jobject self, jobject jid, jobject jstart, jobject jfinish) {
  jlog_ctx *ctx;
  jlog_id id, start, finish;
  jlogId_to_jlogid(jenv, jid, &id);
  jlogId_to_jlogid(jenv, jstart, &start);
  jlogId_to_jlogid(jenv, jfinish, &finish);
  FETCH_CTX(jenv,self,ctx);
  if(jlog_ctx_advance_id(ctx, &id, &start, &finish) != 0) {
    THROW(jenv,"com/omniti/labs/jlog$jlogIOException",jlog_ctx_err_string(ctx));
    return NULL;
  }
  return make_jlog_Id(jenv, start);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    first_log_id
 * Signature: (Lcom/omniti/labs/jlog/Id;)Lcom/omniti/labs/jlog/Id;
 */
jobject Java_com_omniti_labs_jlog_first_1log_1id
  (JNIEnv *jenv, jobject self, jobject jid) {
  jlog_ctx *ctx;
  jlog_id id;
  FETCH_CTX(jenv,self,ctx);
  if(jlog_ctx_first_log_id(ctx, &id) != 0) {
    THROW(jenv,"com/omniti/labs/jlog$jlogIOException","directory inaccessible");
    return NULL;
  }
  return make_jlog_Id(jenv, id);
}

/*
 * Class:     com_omniti_labs_jlog
 * Method:    last_log_id
 * Signature: (Lcom/omniti/labs/jlog/Id;)Lcom/omniti/labs/jlog/Id;
 */
jobject Java_com_omniti_labs_jlog_last_1log_1id
  (JNIEnv *jenv, jobject self, jobject jid) {
  jlog_ctx *ctx;
  jlog_id id;
  FETCH_CTX(jenv,self,ctx);
  if(jlog_ctx_last_log_id(ctx, &id) != 0) {
    THROW(jenv,"com/omniti/labs/jlog$jlogIOException","directory inaccessible");
    return NULL;
  }
  return make_jlog_Id(jenv, id);
}
